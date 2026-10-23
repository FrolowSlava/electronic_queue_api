package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"electronic-queue-api/internal/queue"
)

const maxBodyBytes = 4 << 10

func (a *api) createTicket(w http.ResponseWriter, r *http.Request) {
	if !requireEmptyBody(w, r) {
		return
	}
	ticket, err := a.store.CreateTicket(r.Context())
	if err != nil {
		a.handleError(w, r, err)
		return
	}
	w.Header().Set("Location", "/tickets/"+strconv.FormatInt(ticket.ID, 10))
	writeJSON(w, http.StatusCreated, ticket)
}

func (a *api) getTicket(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	ticket, err := a.store.GetTicket(r.Context(), id)
	if err != nil {
		a.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, ticket)
}

func (a *api) listTickets(w http.ResponseWriter, r *http.Request) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		invalidRequest(w, "Некорректные параметры запроса")
		return
	}
	for _, name := range []string{"status", "limit", "offset"} {
		if len(values[name]) > 1 {
			invalidRequest(w, "Параметр "+name+" должен быть указан один раз")
			return
		}
	}
	status := values.Get("status")
	if values.Has("status") && !queue.ValidStatus(status) {
		invalidRequest(w, "Неизвестное состояние талона")
		return
	}
	limit, ok := paginationValue(values, "limit", 50)
	if !ok || limit < 1 || limit > 200 {
		invalidRequest(w, "limit должен быть целым числом от 1 до 200")
		return
	}
	offset, ok := paginationValue(values, "offset", 0)
	if !ok || offset < 0 {
		invalidRequest(w, "offset должен быть неотрицательным целым числом")
		return
	}
	tickets, err := a.store.ListTickets(r.Context(), status, limit, offset)
	if err != nil {
		a.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Items  []queue.Ticket `json:"items"`
		Limit  int            `json:"limit"`
		Offset int            `json:"offset"`
	}{Items: tickets, Limit: limit, Offset: offset})
}

func (a *api) listOperators(w http.ResponseWriter, r *http.Request) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		invalidRequest(w, "Некорректные параметры запроса")
		return
	}
	var active *bool
	if values.Has("active") {
		if len(values["active"]) != 1 {
			invalidRequest(w, "Параметр active должен быть указан один раз")
			return
		}
		value := values.Get("active")
		if value != "true" && value != "false" {
			invalidRequest(w, "active должен иметь значение true или false")
			return
		}
		filter := value == "true"
		active = &filter
	}
	operators, err := a.store.ListOperatorsFiltered(r.Context(), active)
	if err != nil {
		a.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Items []queue.Operator `json:"items"`
	}{Items: operators})
}

func (a *api) createOperator(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Name string `json:"name"`
	}
	if !readJSON(w, r, &request, "Ожидается JSON-объект с единственным строковым полем name") {
		return
	}
	name := strings.TrimSpace(request.Name)
	if name == "" {
		invalidRequest(w, "name должен содержать непустое имя оператора")
		return
	}
	operator, err := a.store.CreateOperator(r.Context(), name)
	if err != nil {
		a.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, operator)
}

func (a *api) setOperatorActive(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var request struct {
		Active *bool `json:"active"`
	}
	if !readJSON(w, r, &request, "Ожидается JSON-объект с единственным логическим полем active") {
		return
	}
	if request.Active == nil {
		invalidRequest(w, "Обязательное поле active должно иметь значение true или false")
		return
	}
	operator, err := a.store.SetOperatorActive(r.Context(), id, *request.Active)
	if err != nil {
		a.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, operator)
}

func (a *api) next(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok || !requireEmptyBody(w, r) {
		return
	}
	ticket, err := a.store.Next(r.Context(), id)
	if errors.Is(err, queue.ErrEmptyQueue) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		a.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, ticket)
}

func (a *api) complete(w http.ResponseWriter, r *http.Request) {
	a.finish(w, r, false)
}

func (a *api) skip(w http.ResponseWriter, r *http.Request) {
	a.finish(w, r, true)
}

func (a *api) finish(w http.ResponseWriter, r *http.Request, skip bool) {
	operatorID, ok := pathID(w, r)
	if !ok {
		return
	}
	ticketID, ok := readTicketID(w, r)
	if !ok {
		return
	}
	var ticket queue.Ticket
	var err error
	if skip {
		ticket, err = a.store.Skip(r.Context(), operatorID, ticketID)
	} else {
		ticket, err = a.store.Complete(r.Context(), operatorID, ticketID)
	}
	if err != nil {
		a.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, ticket)
}

func (a *api) health(w http.ResponseWriter, r *http.Request) {
	if err := a.pool.Ping(r.Context()); err != nil {
		a.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, ok := parseID(r.PathValue("id"))
	if !ok {
		invalidRequest(w, "Идентификатор должен быть положительным целым числом без ведущих нулей")
	}
	return id, ok
}

func parseID(value string) (int64, bool) {
	id, err := strconv.ParseInt(value, 10, 64)
	return id, err == nil && id > 0 && strconv.FormatInt(id, 10) == value
}

func paginationValue(values url.Values, name string, fallback int) (int, bool) {
	if !values.Has(name) {
		return fallback, true
	}
	value := values.Get(name)
	if value == "" || strings.IndexFunc(value, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return 0, false
	}
	parsed, err := strconv.Atoi(value)
	return parsed, err == nil
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "Тело запроса превышает 4096 байт", nil)
		} else {
			invalidRequest(w, "Не удалось прочитать тело запроса")
		}
		return nil, false
	}
	return body, true
}

func requireEmptyBody(w http.ResponseWriter, r *http.Request) bool {
	body, ok := readBody(w, r)
	if !ok {
		return false
	}
	if len(body) != 0 {
		invalidRequest(w, "Для этой операции тело запроса не требуется")
		return false
	}
	return true
}

func readJSON(w http.ResponseWriter, r *http.Request, target any, message string) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Ожидается Content-Type: application/json", nil)
		return false
	}
	body, ok := readBody(w, r)
	if !ok {
		return false
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		invalidRequest(w, message)
		return false
	}
	// Второй JSON-объект и лишний текст после первого не должны игнорироваться.
	if err := decoder.Decode(new(any)); err != io.EOF {
		invalidRequest(w, "После JSON-объекта обнаружены лишние данные")
		return false
	}
	return true
}

func readTicketID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	var request struct {
		TicketID string `json:"ticketId"`
	}
	if !readJSON(w, r, &request, "Ожидается JSON-объект с единственным строковым полем ticketId") {
		return 0, false
	}
	id, valid := parseID(request.TicketID)
	if !valid {
		invalidRequest(w, "ticketId должен быть строкой с положительным целым числом без ведущих нулей")
		return 0, false
	}
	return id, true
}

func invalidRequest(w http.ResponseWriter, message string) {
	writeError(w, http.StatusBadRequest, "invalid_request", message, nil)
}
