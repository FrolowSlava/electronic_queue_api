package httpapi

import (
	"context"
	"net/http"
	"time"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (a *api) middleware(next http.Handler, requestTimeout time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		writer := &statusWriter{ResponseWriter: w}
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		r = r.WithContext(ctx)
		defer func() {
			if recovered := recover(); recovered != nil {
				a.logger.ErrorContext(ctx, "Паника в HTTP-обработчике", "panic", recovered, "path", r.URL.Path)
				// Если заголовки отправлены, заменить ответ корректным JSON уже нельзя.
				if writer.status == 0 {
					writeError(writer, http.StatusInternalServerError, "internal_error", "Внутренняя ошибка сервера", nil)
				}
			}
			status := writer.status
			if status == 0 {
				status = http.StatusOK
			}
			a.logger.InfoContext(ctx, "HTTP-запрос", "method", r.Method, "path", r.URL.Path,
				"status", status, "duration", time.Since(started))
		}()
		next.ServeHTTP(writer, r)
	})
}
