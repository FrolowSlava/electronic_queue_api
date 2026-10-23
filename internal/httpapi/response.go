package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"

	"electronic-queue-api/internal/queue"
	"github.com/jackc/pgx/v5/pgconn"
)

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string, details map[string]string) {
	type apiError struct {
		Code    string            `json:"code"`
		Message string            `json:"message"`
		Details map[string]string `json:"details,omitempty"`
	}
	writeJSON(w, status, struct {
		Error apiError `json:"error"`
	}{Error: apiError{Code: code, Message: message, Details: details}})
}

func (a *api) handleError(w http.ResponseWriter, r *http.Request, err error) {
	var domainErr *queue.Error
	if errors.As(err, &domainErr) {
		status := http.StatusConflict
		if domainErr.Code == "invalid_request" {
			status = http.StatusBadRequest
		} else if domainErr.Code == "ticket_not_found" || domainErr.Code == "operator_not_found" {
			status = http.StatusNotFound
		}
		var details map[string]string
		if domainErr.CurrentTicketID > 0 {
			details = map[string]string{"currentTicketId": strconv.FormatInt(domainErr.CurrentTicketID, 10)}
		}
		writeError(w, status, domainErr.Code, domainErr.Message, details)
		return
	}

	a.logger.ErrorContext(r.Context(), "Ошибка операции", "method", r.Method, "path", r.URL.Path, "error", err)
	var pgErr *pgconn.PgError
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.As(err, &pgErr) && (pgErr.Code == "57014" || pgErr.Code == "55P03") {
		writeError(w, http.StatusServiceUnavailable, "operation_timeout", "Операция прервана или превышено время ожидания", nil)
		return
	}
	var connectErr *pgconn.ConnectError
	var netErr net.Error
	if errors.As(err, &connectErr) || errors.As(err, &netErr) ||
		pgErr != nil && (strings.HasPrefix(pgErr.Code, "08") || pgErr.Code == "57P01" || pgErr.Code == "57P02" || pgErr.Code == "57P03") {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "База данных временно недоступна", nil)
		return
	}
	writeError(w, http.StatusInternalServerError, "internal_error", "Внутренняя ошибка сервера", nil)
}
