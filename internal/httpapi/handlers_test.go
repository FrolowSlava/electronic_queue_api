package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"electronic-queue-api/internal/httpapi"
	"electronic-queue-api/internal/queue"
	"electronic-queue-api/internal/testutil"
)

type response struct {
	status int
	header http.Header
	body   []byte
}

func testServer(t *testing.T, timeout time.Duration) (*testutil.Database, *httptest.Server) {
	t.Helper()
	db := testutil.New(t, 3)
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("не удалось определить путь к Swagger")
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := httptest.NewServer(httpapi.New(queue.New(db.Pool), db.Pool, logger, timeout,
		filepath.Join(filepath.Dir(source), "..", "..")))
	t.Cleanup(server.Close)
	return db, server
}

func send(client *http.Client, method, url, body, contentType string) (response, error) {
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		return response{}, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := client.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, header: resp.Header, body: data}, err
}

func request(t *testing.T, server *httptest.Server, method, path, body, contentType string, wantStatus int) response {
	t.Helper()
	got, err := send(server.Client(), method, server.URL+path, body, contentType)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	if got.status != wantStatus {
		t.Fatalf("%s %s: статус %d, ожидался %d; тело: %s", method, path, got.status, wantStatus, got.body)
	}
	return got
}

func decode[T any](t *testing.T, resp response) T {
	t.Helper()
	if !strings.HasPrefix(resp.header.Get("Content-Type"), "application/json") {
		t.Fatalf("неверный Content-Type: %q", resp.header.Get("Content-Type"))
	}
	var value T
	if err := json.Unmarshal(resp.body, &value); err != nil {
		t.Fatalf("неверный JSON %q: %v", resp.body, err)
	}
	return value
}

func errorCode(t *testing.T, resp response, want string) map[string]any {
	t.Helper()
	value := decode[struct {
		Error struct {
			Code    string         `json:"code"`
			Message string         `json:"message"`
			Details map[string]any `json:"details"`
		} `json:"error"`
	}](t, resp)
	if value.Error.Code != want || value.Error.Message == "" {
		t.Fatalf("ожидалась непустая ошибка %q, получено: %s", want, resp.body)
	}
	return value.Error.Details
}

func ticket(t *testing.T, resp response, status string) map[string]any {
	t.Helper()
	value := decode[map[string]any](t, resp)
	if len(value) != 7 || value["status"] != status {
		t.Fatalf("неверное представление талона: %s", resp.body)
	}
	if _, ok := value["id"].(string); !ok {
		t.Fatalf("id должен быть строкой: %s", resp.body)
	}
	for _, name := range []string{"createdAt", "startedAt", "finishedAt"} {
		if value[name] == nil && name != "createdAt" {
			continue
		}
		text, ok := value[name].(string)
		if !ok || !strings.HasSuffix(text, "Z") {
			t.Fatalf("%s должно быть временем UTC: %s", name, resp.body)
		}
		if _, err := time.Parse(time.RFC3339Nano, text); err != nil {
			t.Fatalf("%s: неверное время: %v", name, err)
		}
	}
	return value
}

func TestHTTPLifecycle(t *testing.T) {
	_, server := testServer(t, 5*time.Second)
	created := request(t, server, "POST", "/tickets", "", "", http.StatusCreated)
	first := ticket(t, created, queue.StatusWaiting)
	if first["id"] != "1" || first["number"] != "A001" || created.header.Get("Location") != "/tickets/1" {
		t.Fatalf("неверные номер или Location: %s; %v", created.body, created.header)
	}
	for _, name := range []string{"operatorId", "startedAt", "finishedAt"} {
		if first[name] != nil {
			t.Fatalf("у waiting поле %s должно быть null: %s", name, created.body)
		}
	}
	ticket(t, request(t, server, "POST", "/tickets", "", "", 201), queue.StatusWaiting)
	got := ticket(t, request(t, server, "GET", "/tickets/1", "", "", 200), queue.StatusWaiting)
	if got["createdAt"] != first["createdAt"] {
		t.Fatal("GET изменил время создания")
	}
	operators := decode[struct {
		Items []map[string]any `json:"items"`
	}](t, request(t, server, "GET", "/operators", "", "", 200))
	if len(operators.Items) != 3 || operators.Items[0]["id"] != "1" || operators.Items[0]["currentTicketId"] != nil {
		t.Fatalf("неверные операторы: %+v", operators)
	}
	serving := ticket(t, request(t, server, "POST", "/operators/1/next", "", "", 200), queue.StatusServing)
	if serving["id"] != "1" || serving["operatorId"] != "1" || serving["startedAt"] == nil || serving["finishedAt"] != nil {
		t.Fatalf("неверное назначение: %+v", serving)
	}
	operators = decode[struct {
		Items []map[string]any `json:"items"`
	}](t, request(t, server, "GET", "/operators", "", "", 200))
	if operators.Items[0]["currentTicketId"] != "1" {
		t.Fatalf("не показан активный талон: %+v", operators)
	}
	details := errorCode(t, request(t, server, "POST", "/operators/1/next", "", "", 409), "operator_busy")
	if details["currentTicketId"] != "1" {
		t.Fatalf("неверные сведения о занятости: %+v", details)
	}
	errorCode(t, request(t, server, "POST", "/operators/2/complete", `{"ticketId":"1"}`, "application/json", 409), "ticket_not_owned")
	errorCode(t, request(t, server, "POST", "/operators/2/skip", `{"ticketId":"1"}`, "application/json", 409), "ticket_not_owned")
	errorCode(t, request(t, server, "POST", "/operators/1/complete", `{"ticketId":"2"}`, "application/json", 409), "invalid_ticket_state")
	completed := ticket(t, request(t, server, "POST", "/operators/1/complete", `{"ticketId":"1"}`, "application/json; charset=utf-8", 200), queue.StatusCompleted)
	if completed["operatorId"] != "1" || completed["startedAt"] != serving["startedAt"] || completed["finishedAt"] == nil {
		t.Fatalf("потеряна история завершения: %+v", completed)
	}
	ticket(t, request(t, server, "POST", "/operators/1/next", "", "", 200), queue.StatusServing)
	// Старый ticketId защищает следующий талон от запоздавшей команды.
	errorCode(t, request(t, server, "POST", "/operators/1/complete", `{"ticketId":"1"}`, "application/json", 409), "invalid_ticket_state")
	current := ticket(t, request(t, server, "GET", "/tickets/2", "", "", 200), queue.StatusServing)
	if current["finishedAt"] != nil {
		t.Fatal("повторное завершение затронуло следующий талон")
	}
	skipped := ticket(t, request(t, server, "POST", "/operators/1/skip", `{"ticketId":"2"}`, "application/json", 200), queue.StatusSkipped)
	if skipped["operatorId"] != "1" || skipped["startedAt"] != current["startedAt"] || skipped["finishedAt"] == nil {
		t.Fatalf("потеряна история пропуска: %+v", skipped)
	}
	errorCode(t, request(t, server, "POST", "/operators/1/skip", `{"ticketId":"2"}`, "application/json", 409), "invalid_ticket_state")
	errorCode(t, request(t, server, "POST", "/operators/1/complete", `{"ticketId":"2"}`, "application/json", 409), "invalid_ticket_state")
	empty := request(t, server, "POST", "/operators/1/next", "", "", 204)
	if len(empty.body) != 0 {
		t.Fatalf("204 должен быть без тела: %s", empty.body)
	}
	for id, want := range map[string]string{"1": queue.StatusCompleted, "2": queue.StatusSkipped} {
		history := ticket(t, request(t, server, "GET", "/tickets/"+id, "", "", 200), want)
		if history["operatorId"] != "1" || history["finishedAt"] == nil {
			t.Fatalf("GET не сохранил историю: %+v", history)
		}
	}
}

func TestHTTPValidation(t *testing.T) {
	db, server := testServer(t, 5*time.Second)
	tests := []struct {
		name, method, path, body, contentType string
		status                                int
		code                                  string
	}{
		{"bad_json", "POST", "/operators/1/complete", `{`, "application/json", 400, "invalid_request"},
		{"unknown_field", "POST", "/operators/1/complete", `{"ticketId":"1","extra":1}`, "application/json", 400, "invalid_request"},
		{"trailing_object", "POST", "/operators/1/complete", `{"ticketId":"1"}{}`, "application/json", 400, "invalid_request"},
		{"trailing_text", "POST", "/operators/1/skip", `{"ticketId":"1"} extra`, "application/json", 400, "invalid_request"},
		{"missing_id", "POST", "/operators/1/complete", `{}`, "application/json", 400, "invalid_request"},
		{"numeric_id", "POST", "/operators/1/complete", `{"ticketId":1}`, "application/json", 400, "invalid_request"},
		{"null_id", "POST", "/operators/1/complete", `{"ticketId":null}`, "application/json", 400, "invalid_request"},
		{"null_object", "POST", "/operators/1/complete", `null`, "application/json", 400, "invalid_request"},
		{"array", "POST", "/operators/1/complete", `[]`, "application/json", 400, "invalid_request"},
		{"empty_json", "POST", "/operators/1/complete", "", "application/json", 400, "invalid_request"},
		{"missing_content_type", "POST", "/operators/1/complete", `{"ticketId":"1"}`, "", 415, "unsupported_media_type"},
		{"wrong_content_type", "POST", "/operators/1/skip", `{"ticketId":"1"}`, "text/plain", 415, "unsupported_media_type"},
		{"invalid_content_type", "POST", "/operators/1/skip", `{"ticketId":"1"}`, "application/json; broken", 415, "unsupported_media_type"},
		{"oversized_json", "POST", "/operators/1/complete", strings.Repeat(" ", 4097), "application/json", 413, "request_too_large"},
		{"oversized_create", "POST", "/tickets", strings.Repeat("x", 4097), "", 413, "request_too_large"},
		{"unexpected_create_body", "POST", "/tickets", `{}`, "application/json", 400, "invalid_request"},
		{"unexpected_next_body", "POST", "/operators/1/next", " ", "", 400, "invalid_request"},
		{"unknown_status", "GET", "/tickets?status=unknown", "", "", 400, "invalid_request"},
		{"empty_status", "GET", "/tickets?status=", "", "", 400, "invalid_request"},
		{"duplicate_status", "GET", "/tickets?status=waiting&status=serving", "", "", 400, "invalid_request"},
		{"limit_zero", "GET", "/tickets?limit=0", "", "", 400, "invalid_request"},
		{"limit_high", "GET", "/tickets?limit=201", "", "", 400, "invalid_request"},
		{"limit_empty", "GET", "/tickets?limit=", "", "", 400, "invalid_request"},
		{"limit_text", "GET", "/tickets?limit=many", "", "", 400, "invalid_request"},
		{"limit_duplicate", "GET", "/tickets?limit=2&limit=3", "", "", 400, "invalid_request"},
		{"offset_negative", "GET", "/tickets?offset=-1", "", "", 400, "invalid_request"},
		{"offset_overflow", "GET", "/tickets?offset=9223372036854775808", "", "", 400, "invalid_request"},
		{"offset_duplicate", "GET", "/tickets?offset=0&offset=1", "", "", 400, "invalid_request"},
		{"bad_query", "GET", "/tickets?status=waiting;limit=1", "", "", 400, "invalid_request"},
		{"ticket_not_found", "GET", "/tickets/999", "", "", 404, "ticket_not_found"},
		{"operator_not_found", "POST", "/operators/999/next", "", "", 404, "operator_not_found"},
		{"finish_ticket_not_found", "POST", "/operators/1/complete", `{"ticketId":"999"}`, "application/json", 404, "ticket_not_found"},
		{"finish_operator_not_found", "POST", "/operators/999/skip", `{"ticketId":"999"}`, "application/json", 404, "operator_not_found"},
		{"unknown_route", "GET", "/missing", "", "", 404, "route_not_found"},
		{"wrong_method", "DELETE", "/tickets", "", "", 405, "method_not_allowed"},
	}
	for _, id := range []string{"0", "-1", "+1", "01", "abc", "9223372036854775808"} {
		tests = append(tests, struct {
			name, method, path, body, contentType string
			status                                int
			code                                  string
		}{"invalid_path_" + id, "GET", "/tickets/" + id, "", "", 400, "invalid_request"})
		tests = append(tests, struct {
			name, method, path, body, contentType string
			status                                int
			code                                  string
		}{"invalid_ticket_id_" + id, "POST", "/operators/1/complete", fmt.Sprintf(`{"ticketId":%q}`, id), "application/json", 400, "invalid_request"})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := request(t, server, tt.method, tt.path, tt.body, tt.contentType, tt.status)
			errorCode(t, got, tt.code)
			if tt.status == 405 && got.header.Get("Allow") != "GET, HEAD, POST" {
				t.Fatalf("неверный Allow: %q", got.header.Get("Allow"))
			}
		})
	}
	var count int
	if err := db.Pool.QueryRow(context.Background(), "SELECT count(*) FROM tickets").Scan(&count); err != nil || count != 0 {
		t.Fatalf("отклонённые запросы изменили БД: count=%d, err=%v", count, err)
	}
}

func TestHTTPLists(t *testing.T) {
	_, server := testServer(t, 5*time.Second)
	empty := request(t, server, "GET", "/tickets", "", "", 200)
	if !bytes.Contains(empty.body, []byte(`"items":[]`)) {
		t.Fatalf("пустой список должен содержать items: []: %s", empty.body)
	}
	for range 3 {
		request(t, server, "POST", "/tickets", "", "", 201)
	}
	request(t, server, "POST", "/operators/1/next", "", "", 200)
	for _, tt := range []struct {
		path   string
		ids    []string
		limit  int
		offset int
	}{
		{"/tickets", []string{"1", "2", "3"}, 50, 0},
		{"/tickets?status=waiting", []string{"2", "3"}, 50, 0},
		{"/tickets?status=serving", []string{"1"}, 50, 0},
		{"/tickets?status=completed", []string{}, 50, 0},
		{"/tickets?status=skipped", []string{}, 50, 0},
		{"/tickets?limit=1&offset=1", []string{"2"}, 1, 1},
		{"/tickets?limit=200&offset=100", []string{}, 200, 100},
	} {
		t.Run(tt.path, func(t *testing.T) {
			got := decode[struct {
				Items  []map[string]any `json:"items"`
				Limit  int              `json:"limit"`
				Offset int              `json:"offset"`
			}](t, request(t, server, "GET", tt.path, "", "", 200))
			if got.Items == nil || len(got.Items) != len(tt.ids) || got.Limit != tt.limit || got.Offset != tt.offset {
				t.Fatalf("неверная страница: %+v", got)
			}
			for i, id := range tt.ids {
				if got.Items[i]["id"] != id {
					t.Fatalf("неверный порядок страницы: %+v", got.Items)
				}
			}
		})
	}
}

func TestHTTPDocumentationAndHealth(t *testing.T) {
	_, server := testServer(t, 5*time.Second)
	health := decode[map[string]string](t, request(t, server, "GET", "/healthz", "", "", 200))
	if health["status"] != "ok" {
		t.Fatalf("неверный health: %+v", health)
	}
	for _, tt := range []struct{ path, contentType, contains string }{
		{"/openapi.yaml", "application/yaml", "openapi: 3.0.3"},
		{"/docs/", "text/html", `url: "/openapi.yaml"`},
		{"/docs/swagger-ui.css", "text/css", "swagger-ui"},
		{"/docs/swagger-ui-bundle.js", "javascript", "SwaggerUIBundle"},
	} {
		t.Run(tt.path, func(t *testing.T) {
			got := request(t, server, "GET", tt.path, "", "", 200)
			if !strings.Contains(got.header.Get("Content-Type"), tt.contentType) || !bytes.Contains(got.body, []byte(tt.contains)) {
				t.Fatalf("неверный документ %s, Content-Type=%q, размер=%d", tt.path, got.header.Get("Content-Type"), len(got.body))
			}
		})
	}
}

func TestHTTPTimeoutRollsBackAndPoolRemainsUsable(t *testing.T) {
	db, server := testServer(t, 150*time.Millisecond)
	request(t, server, "POST", "/tickets", "", "", 201)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "SELECT id FROM operators WHERE id = 1 FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	// Запрос истекает при ожидании оператора, до изменения талона.
	errorCode(t, request(t, server, "POST", "/operators/1/next", "", "", 503), "operation_timeout")
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	waiting := ticket(t, request(t, server, "GET", "/tickets/1", "", "", 200), queue.StatusWaiting)
	if waiting["operatorId"] != nil || waiting["startedAt"] != nil || waiting["finishedAt"] != nil {
		t.Fatalf("timeout оставил промежуточное состояние: %+v", waiting)
	}
	ticket(t, request(t, server, "POST", "/operators/1/next", "", "", 200), queue.StatusServing)
	if err := db.Pool.Ping(ctx); err != nil {
		t.Fatalf("пул не работает после timeout: %v", err)
	}
}

func TestHTTPCanceledContextDoesNotCreateTicket(t *testing.T) {
	db := testutil.New(t, 3)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := httpapi.New(queue.New(db.Pool), db.Pool, logger, time.Second, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest("POST", "/tickets", nil).WithContext(ctx)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	errorCode(t, response{status: recorder.Code, header: recorder.Header(), body: recorder.Body.Bytes()}, "operation_timeout")
	if recorder.Code != 503 {
		t.Fatalf("отменённый контекст: статус %d", recorder.Code)
	}
	var count int
	if err := db.Pool.QueryRow(context.Background(), "SELECT count(*) FROM tickets").Scan(&count); err != nil || count != 0 {
		t.Fatalf("отменённый запрос создал талон: count=%d, err=%v", count, err)
	}
}

func TestHTTPConcurrentTwoOperatorsOneTicket(t *testing.T) {
	db, server := testServer(t, 5*time.Second)
	request(t, server, "POST", "/tickets", "", "", 201)
	type result struct {
		response response
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for operator := 1; operator <= 2; operator++ {
		go func() {
			<-start
			resp, err := send(server.Client(), "POST", fmt.Sprintf("%s/operators/%d/next", server.URL, operator), "", "")
			results <- result{resp, err}
		}()
	}
	close(start)
	success, empty := 0, 0
	var owner string
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		switch result.response.status {
		case 200:
			success++
			got := ticket(t, result.response, queue.StatusServing)
			owner = got["operatorId"].(string)
			if got["id"] != "1" || got["number"] != "A001" {
				t.Fatalf("выдан неизвестный талон: %+v", got)
			}
		case 204:
			empty++
		default:
			t.Fatalf("неожиданный ответ: %d %s", result.response.status, result.response.body)
		}
	}
	if success != 1 || empty != 1 {
		t.Fatalf("один талон: success=%d, empty=%d", success, empty)
	}
	var state, storedOwner string
	if err := db.Pool.QueryRow(context.Background(), "SELECT status, operator_id::text FROM tickets WHERE id = 1").Scan(&state, &storedOwner); err != nil {
		t.Fatal(err)
	}
	if state != queue.StatusServing || storedOwner != owner {
		t.Fatalf("БД не соответствует ответу: state=%s, owner=%s, ответ owner=%s", state, storedOwner, owner)
	}
}
