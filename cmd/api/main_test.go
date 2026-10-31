package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"electronic-queue-api/internal/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

const shutdownTestBudget = 500 * time.Millisecond

func TestShutdownTotalDeadline(t *testing.T) {
	for _, mode := range []string{"stuck_handler", "held_connection", "blocked_logger"} {
		t.Run(mode, func(t *testing.T) {
			databaseURL := ""
			if mode == "held_connection" {
				// Схема принадлежит родителю: принудительный выход ребёнка
				// не выполнит зарегистрированные в нём cleanup.
				databaseURL = testutil.New(t, 3).URL
			}
			elapsed, output := runShutdownChild(t, mode, databaseURL, 1)
			if elapsed < shutdownTestBudget-50*time.Millisecond || elapsed > shutdownTestBudget+350*time.Millisecond {
				t.Fatalf("выход вне общего бюджета с допуском планировщика: %s, бюджет %s\n%s", elapsed, shutdownTestBudget, output)
			}
			if strings.Contains(output, "SHUTDOWN_RETURNED") {
				t.Fatalf("зависшая остановка неожиданно вернулась без watchdog:\n%s", output)
			}
		})
	}
}

func TestShutdownCooperativeCleanup(t *testing.T) {
	elapsed, output := runShutdownChild(t, "cooperative", "", 0)
	if elapsed > shutdownTestBudget+100*time.Millisecond || !strings.Contains(output, "CLEANUP_AFTER_HANDLER") {
		t.Fatalf("отмена и очистка не уложились в общий бюджет: %s\n%s", elapsed, output)
	}
}

func TestShutdownIdleReturnsEarly(t *testing.T) {
	elapsed, output := runShutdownChild(t, "idle", "", 0)
	if elapsed >= shutdownTestBudget/2 || !strings.Contains(output, "POOL_CLOSED") {
		t.Fatalf("пустой сервер ждал бюджет вместо немедленного закрытия: %s\n%s", elapsed, output)
	}
}

func runShutdownChild(t *testing.T, mode, databaseURL string, wantExit int) (time.Duration, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestShutdownChildProcess$")
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if !strings.EqualFold(key, "GORACE") && !strings.EqualFold(key, "QUEUE_SHUTDOWN_TEST_MODE") && !strings.EqualFold(key, "QUEUE_SHUTDOWN_TEST_DATABASE") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	// Race runtime по умолчанию добавляет секунду при выходе. Тест измеряет
	// срок остановки приложения, а не диагностическую задержку инструмента.
	cmd.Env = append(cmd.Env,
		"GORACE="+strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"),
		"QUEUE_SHUTDOWN_TEST_MODE="+mode,
		"QUEUE_SHUTDOWN_TEST_DATABASE="+databaseURL)
	output, err := cmd.CombinedOutput()
	ended := time.Now()
	if ctx.Err() != nil {
		t.Fatalf("watchdog не завершил дочерний процесс: %v\n%s", ctx.Err(), output)
	}
	exit := 0
	if err != nil {
		failure, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("запуск проверки shutdown: %v\n%s", err, output)
		}
		exit = failure.ExitCode()
	}
	if exit != wantExit {
		t.Fatalf("код выхода %d, ожидался %d: %v\n%s", exit, wantExit, err, output)
	}
	for _, line := range strings.Split(string(output), "\n") {
		if value, ok := strings.CutPrefix(line, "SHUTDOWN_STARTED="); ok {
			started, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
			if err != nil {
				t.Fatal(err)
			}
			return ended.Sub(started), string(output)
		}
	}
	t.Fatalf("не найден момент начала shutdown:\n%s", output)
	return 0, ""
}

type blockedShutdownLog struct{}

func (blockedShutdownLog) Write([]byte) (int, error) { select {} }

// Helper выполняется только в дочернем процессе: shutdown вооружает
// окончательный watchdog, а os.Exit намеренно пропускает defer.
func TestShutdownChildProcess(t *testing.T) {
	mode := os.Getenv("QUEUE_SHUTDOWN_TEST_MODE")
	if mode == "" {
		return
	}
	requestsCtx, cancelRequests := context.WithCancel(context.Background())
	var active sync.WaitGroup
	entered := make(chan struct{})
	handlerDone := make(chan struct{})
	server := &http.Server{
		BaseContext: func(net.Listener) context.Context { return requestsCtx },
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			active.Add(1)
			defer active.Done()
			defer close(handlerDone)
			close(entered)
			if mode == "stuck_handler" {
				select {} // Намеренно игнорирует context и закрытие HTTP-соединения.
			}
			<-r.Context().Done()
		}),
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	if mode == "stuck_handler" || mode == "cooperative" {
		go func() {
			client := &http.Client{Timeout: 3 * time.Second}
			response, err := client.Get("http://" + listener.Addr().String())
			if err == nil {
				_ = response.Body.Close()
			}
			client.CloseIdleConnections()
		}()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("проверочный обработчик не запустился")
		}
	}
	closePool := func() { fmt.Println("POOL_CLOSED") }
	if mode == "cooperative" {
		closePool = func() {
			select {
			case <-handlerDone:
				fmt.Println("CLEANUP_AFTER_HANDLER")
			default:
				fmt.Println("CLEANUP_BEFORE_HANDLER")
				os.Exit(2)
			}
		}
	}
	var held *pgxpool.Conn
	if mode == "held_connection" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		pool, err := pgxpool.New(ctx, os.Getenv("QUEUE_SHUTDOWN_TEST_DATABASE"))
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		held, err = pool.Acquire(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		closePool = pool.Close
	}
	var logOutput io.Writer = io.Discard
	if mode == "blocked_logger" {
		logOutput = blockedShutdownLog{}
	}
	logger := slog.New(slog.NewJSONHandler(logOutput, nil))
	fmt.Println("SHUTDOWN_STARTED=" + time.Now().Format(time.RFC3339Nano))
	shutdown(server, &active, cancelRequests, closePool, logger, shutdownTestBudget)
	if held != nil {
		held.Release()
	}
	fmt.Println("SHUTDOWN_RETURNED")
	os.Exit(0)
}
