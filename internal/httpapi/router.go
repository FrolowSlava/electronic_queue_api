package httpapi

import (
	"log/slog"
	"net/http"
	"path/filepath"
	"time"

	"electronic-queue-api/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

type api struct {
	store  *queue.Store
	pool   *pgxpool.Pool
	logger *slog.Logger
}

func New(store *queue.Store, pool *pgxpool.Pool, logger *slog.Logger, requestTimeout time.Duration, docsDir string) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	a := &api{store: store, pool: pool, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tickets", a.createTicket)
	mux.HandleFunc("GET /tickets", a.listTickets)
	mux.HandleFunc("GET /tickets/{id}", a.getTicket)
	mux.HandleFunc("GET /operators", a.listOperators)
	mux.HandleFunc("POST /operators", a.createOperator)
	mux.HandleFunc("PATCH /operators/{id}/active", a.setOperatorActive)
	mux.HandleFunc("POST /operators/{id}/next", a.next)
	mux.HandleFunc("POST /operators/{id}/complete", a.complete)
	mux.HandleFunc("POST /operators/{id}/skip", a.skip)
	mux.HandleFunc("GET /healthz", a.health)
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		http.ServeFile(w, r, filepath.Join(docsDir, "openapi.yaml"))
	})
	mux.Handle("GET /docs/", swaggerHandler())

	// Обработчики без метода сохраняют единый JSON-формат ошибки 405.
	for path, allow := range map[string]string{
		"/tickets":                 "GET, HEAD, POST",
		"/tickets/{id}":            "GET, HEAD",
		"/operators":               "GET, HEAD, POST",
		"/operators/{id}/active":   "PATCH",
		"/operators/{id}/next":     "POST",
		"/operators/{id}/complete": "POST",
		"/operators/{id}/skip":     "POST",
		"/healthz":                 "GET, HEAD",
		"/openapi.yaml":            "GET, HEAD",
		"/docs/":                   "GET, HEAD",
	} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Allow", allow)
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Метод не поддерживается", nil)
		})
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "route_not_found", "Маршрут не найден", nil)
	})
	return a.middleware(mux, requestTimeout)
}
