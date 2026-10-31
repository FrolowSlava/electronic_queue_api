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
	closePool := sync.OnceFunc(pool.Close)
	defer closePool()

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
	shutdown(server, &active, cancelRequests, closePool, logger, cfg.ShutdownTimeout)
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}

// shutdown начинает окончательное завершение процесса. Watchdog намеренно
// остаётся активным до выхода из main, включая отложенную очистку и логи.
func shutdown(server *http.Server, active *sync.WaitGroup, cancelRequests context.CancelFunc,
	closePool func(), logger *slog.Logger, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	time.AfterFunc(time.Until(deadline), func() {
		// Логгер и очистка сами могут зависнуть. Последний выход не должен
		// ждать их или выполнять defer: весь бюджет уже израсходован.
		os.Exit(1)
	})
	logger.Info("Остановка HTTP-сервера", "timeout", timeout)

	// Резерв на отмену, rollback и закрытие пула входит в общий срок,
	// а не добавляется после него: при 15s получаем до 13s + до 2s.
	cleanupReserve := min(2*time.Second, timeout/2)
	graceCtx, cancelGrace := context.WithDeadline(context.Background(), deadline.Add(-cleanupReserve))
	err := server.Shutdown(graceCtx)
	cancelGrace()
	cancelRequests()
	if err != nil {
		_ = server.Close()
		logger.Warn("Истёк срок завершения запросов", "error", err)
	}
	// Закрытие HTTP-соединений не останавливает goroutine обработчика.
	// Дожидаемся очистки при всё ещё действующем общем watchdog.
	active.Wait()
	closePool()
	logger.Info("HTTP-сервер остановлен")
}
