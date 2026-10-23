package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"electronic-queue-api/internal/config"
	"electronic-queue-api/internal/httpapi"
	"electronic-queue-api/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("Приложение остановлено с ошибкой", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	poolConfig, err := cfg.PoolConfig()
	if err != nil {
		return err
	}
	startup, cancelStartup := context.WithTimeout(context.Background(), 10*time.Second)
	pool, err := pgxpool.NewWithConfig(startup, poolConfig)
	if err == nil {
		err = pool.Ping(startup)
	}
	cancelStartup()
	if err != nil {
		if pool != nil {
			pool.Close()
		}
		return fmt.Errorf("подключение к PostgreSQL не установлено")
	}
	defer pool.Close()

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("запуск HTTP: %w", err)
	}
	defer listener.Close()

	// Сигнал останавливает приём запросов, но не отменяет их контексты сразу.
	// Отдельный контекст отменяется только при принудительной остановке.
	requestsCtx, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()
	var active sync.WaitGroup
	handler := httpapi.New(queue.New(pool), pool, logger, cfg.RequestTimeout, cfg.DocsDir)
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			active.Add(1)
			defer active.Done()
			handler.ServeHTTP(w, r)
		}),
		BaseContext:       func(net.Listener) context.Context { return requestsCtx },
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      cfg.RequestTimeout + 5*time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	logger.Info("HTTP-сервер запущен", "address", listener.Addr().String())
	var serveErr error
	select {
	case <-signalCtx.Done():
		stopSignals()
	case serveErr = <-serveDone:
	}
	logger.Info("Остановка HTTP-сервера")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	err = server.Shutdown(shutdownCtx)
	cancelShutdown()
	if err != nil {
		logger.Warn("Истёк срок завершения запросов", "error", err)
		cancelRequests()
		_ = server.Close()
	}
	// Даже после Close обработчикам нужно закончить rollback и вернуть соединения.
	active.Wait()
	logger.Info("HTTP-сервер остановлен")
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}
