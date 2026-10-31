package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"electronic-queue-api/internal/queue"
)

func TestUnknownCommitOutcomeHTTPResponse(t *testing.T) {
	for _, cause := range []error{
		io.ErrUnexpectedEOF,
		context.Canceled,
		&queue.Error{Code: "operator_not_found", Message: "internal SQL detail"},
	} {
		t.Run(cause.Error(), func(t *testing.T) {
			handler := &api{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			recorder := httptest.NewRecorder()
			handler.handleError(recorder, httptest.NewRequest("POST", "/operators/1/next", nil),
				errors.Join(queue.ErrCommitOutcomeUnknown, cause))
			var body struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if recorder.Code != http.StatusServiceUnavailable ||
				!strings.HasPrefix(recorder.Header().Get("Content-Type"), "application/json") ||
				json.Unmarshal(recorder.Body.Bytes(), &body) != nil ||
				body.Error.Code != "operation_outcome_unknown" || !strings.Contains(body.Error.Message, "GET") {
				t.Fatalf("неопределённый исход неверно представлен клиенту: %d %s", recorder.Code, recorder.Body.String())
			}
			if strings.Contains(recorder.Body.String(), "internal SQL detail") {
				t.Fatalf("в ответе раскрыты внутренние сведения: %s", recorder.Body.String())
			}
		})
	}
}
