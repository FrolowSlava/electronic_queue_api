package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"electronic-queue-api/internal/queue"
	"electronic-queue-api/internal/testutil"
	"github.com/jackc/pgx/v5"
)

var apiBinary string

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	var cleanup func()
	var err error
	apiBinary, cleanup, err = testutil.BuildAPI(ctx, true)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func startAPI(t *testing.T, db *testutil.Database, settings ...string) *testutil.Process {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	process, err := testutil.StartAPI(ctx, apiBinary, db.URL, settings...)
	if err != nil {
		t.Fatalf("запустить процесс API: %v", err)
	}
	// Cleanup зарегистрирован после testutil.New: процесс закроет свои пулы
	// раньше, чем Database.Close удалит принадлежащую тесту схему.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := process.Stop(ctx); err != nil {
			process.Kill()
			t.Errorf("остановить процесс API: %v", err)
		}
	})
	return process
}

type reply struct {
	status int
	body   []byte
	err    error
}

func request(client *http.Client, method, address string, body any) reply {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return reply{err: err}
		}
		reader = bytes.NewReader(data)
	}
	r, err := http.NewRequest(method, address, reader)
	if err != nil {
		return reply{err: err}
	}
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(r)
	if err != nil {
		return reply{err: err}
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	return reply{status: response.StatusCode, body: data, err: err}
}

func checkReply(t *testing.T, result reply, status int, destination any) {
	t.Helper()
	if result.err != nil || result.status != status {
		t.Fatalf("HTTP: статус=%d, ожидается=%d, ошибка=%v, ответ=%s", result.status, status, result.err, result.body)
	}
	if destination != nil {
		if err := json.Unmarshal(result.body, destination); err != nil {
			t.Fatalf("прочитать JSON %s: %v", result.body, err)
		}
	}
}

func clientForTest(t *testing.T) *http.Client {
	t.Helper()
	client := &http.Client{Timeout: 25 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func concurrentNext(client *http.Client, processes []*testutil.Process, sameOperator bool) []reply {
	results := make([]reply, 100)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(len(results))
	done.Add(len(results))
	for i := range results {
		go func(i int) {
			defer done.Done()
			ready.Done()
			<-start
			operator := i + 1
			if sameOperator {
				operator = 1
			}
			address := fmt.Sprintf("%s/operators/%d/next", processes[i%len(processes)].URL, operator)
			results[i] = request(client, http.MethodPost, address, nil)
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()
	return results
}

func TestTwoProcesses100Operators100Tickets(t *testing.T) {
	db := testutil.New(t, 100)
	if _, err := db.Pool.Exec(context.Background(), "INSERT INTO tickets (status) SELECT 'waiting' FROM generate_series(1, 100)"); err != nil {
		t.Fatal(err)
	}
	processes := []*testutil.Process{startAPI(t, db), startAPI(t, db)}
	results := concurrentNext(clientForTest(t), processes, false)
	ids := make(map[int64]bool)
	numbers := make(map[string]bool)
	for i, result := range results {
		var ticket queue.Ticket
		checkReply(t, result, http.StatusOK, &ticket)
		if ids[ticket.ID] || numbers[ticket.Number] {
			t.Fatalf("повторная выдача: %+v", ticket)
		}
		ids[ticket.ID], numbers[ticket.Number] = true, true
		if ticket.OperatorID == nil || *ticket.OperatorID != int64(i+1) || ticket.Status != queue.StatusServing {
			t.Fatalf("неверное назначение: %+v", ticket)
		}
		var storedOperator int64
		var storedStatus string
		if err := db.Pool.QueryRow(context.Background(), "SELECT operator_id, status FROM tickets WHERE id=$1", ticket.ID).Scan(&storedOperator, &storedStatus); err != nil {
			t.Fatal(err)
		}
		if storedOperator != int64(i+1) || storedStatus != queue.StatusServing {
			t.Fatalf("ответ не соответствует БД: %+v, оператор в БД=%d, статус=%s", ticket, storedOperator, storedStatus)
		}
	}
	var count, operators int
	if err := db.Pool.QueryRow(context.Background(), "SELECT count(*), count(DISTINCT operator_id) FROM tickets WHERE status='serving'").Scan(&count, &operators); err != nil {
		t.Fatal(err)
	}
	if count != 100 || operators != 100 {
		t.Fatalf("в БД serving=%d, разных операторов=%d", count, operators)
	}
}

func TestTwoProcessesSameOperator100Requests(t *testing.T) {
	db := testutil.New(t, 3)
	if _, err := db.Pool.Exec(context.Background(), "INSERT INTO tickets (status) SELECT 'waiting' FROM generate_series(1, 100)"); err != nil {
		t.Fatal(err)
	}
	processes := []*testutil.Process{startAPI(t, db), startAPI(t, db)}
	results := concurrentNext(clientForTest(t), processes, true)
	successes, conflicts := 0, 0
	for _, result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		switch result.status {
		case http.StatusOK:
			successes++
		case http.StatusConflict:
			conflicts++
			var errorBody struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			checkReply(t, result, http.StatusConflict, &errorBody)
			if errorBody.Error.Code != "operator_busy" {
				t.Fatalf("неожиданная причина конфликта: %s", result.body)
			}
		default:
			t.Fatalf("неожиданный результат: %d %s", result.status, result.body)
		}
	}
	if successes != 1 || conflicts != 99 {
		t.Fatalf("успешных выдач=%d, конфликтов=%d", successes, conflicts)
	}
	var serving, waiting int
	if err := db.Pool.QueryRow(context.Background(), "SELECT count(*) FILTER (WHERE status='serving'), count(*) FILTER (WHERE status='waiting') FROM tickets").Scan(&serving, &waiting); err != nil {
		t.Fatal(err)
	}
	if serving != 1 || waiting != 99 {
		t.Fatalf("в БД serving=%d, waiting=%d", serving, waiting)
	}
}

func TestRestartPersistence(t *testing.T) {
	db := testutil.New(t, 3)
	first := startAPI(t, db)
	client := clientForTest(t)
	expected := make([]queue.Ticket, 0, 4)
	for i, status := range []string{queue.StatusCompleted, queue.StatusSkipped, queue.StatusServing, queue.StatusWaiting} {
		var ticket queue.Ticket
		checkReply(t, request(client, http.MethodPost, first.URL+"/tickets", nil), http.StatusCreated, &ticket)
		if status != queue.StatusWaiting {
			operator := strconv.Itoa(i + 1)
			checkReply(t, request(client, http.MethodPost, first.URL+"/operators/"+operator+"/next", nil), http.StatusOK, &ticket)
			if status != queue.StatusServing {
				command := "complete"
				if status == queue.StatusSkipped {
					command = "skip"
				}
				checkReply(t, request(client, http.MethodPost, first.URL+"/operators/"+operator+"/"+command,
					map[string]string{"ticketId": strconv.FormatInt(ticket.ID, 10)}), http.StatusOK, &ticket)
			}
		}
		expected = append(expected, ticket)
	}
	var before struct {
		Items []queue.Operator `json:"items"`
	}
	checkReply(t, request(client, http.MethodGet, first.URL+"/operators", nil), http.StatusOK, &before)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	err := first.Stop(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	second := startAPI(t, db)
	for _, original := range expected {
		var restored queue.Ticket
		checkReply(t, request(client, http.MethodGet, second.URL+"/tickets/"+strconv.FormatInt(original.ID, 10), nil), http.StatusOK, &restored)
		if !reflect.DeepEqual(original, restored) {
			t.Fatalf("талон изменился после перезапуска: до=%+v после=%+v", original, restored)
		}
	}
	var after struct {
		Items []queue.Operator `json:"items"`
	}
	checkReply(t, request(client, http.MethodGet, second.URL+"/operators", nil), http.StatusOK, &after)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("операторы изменились после перезапуска: до=%+v после=%+v", before, after)
	}
	checkReply(t, request(client, http.MethodPost, second.URL+"/operators/3/next", nil), http.StatusConflict, nil)
}

func TestGracefulShutdownCompletesInFlightRequest(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM дочернему процессу проверяется в Linux-контейнере")
	}
	db := testutil.New(t, 3)
	process := startAPI(t, db, "SHUTDOWN_TIMEOUT=5s")
	client := clientForTest(t)
	checkReply(t, request(client, http.MethodPost, process.URL+"/tickets", nil), http.StatusCreated, nil)
	lock := lockOperator(t, db)
	result := make(chan reply, 1)
	go func() { result <- request(client, http.MethodPost, process.URL+"/operators/1/next", nil) }()
	waitForOperatorWait(t, db, process)
	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stopped <- process.Stop(ctx)
	}()
	waitForShutdown(t, process)
	select {
	case err := <-stopped:
		t.Fatalf("процесс завершился до окончания активного запроса: %v\n%s", err, process.Logs())
	default:
	}
	if err := lock.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-result:
		var ticket queue.Ticket
		checkReply(t, response, http.StatusOK, &ticket)
		if ticket.Status != queue.StatusServing || ticket.OperatorID == nil || *ticket.OperatorID != 1 {
			t.Fatalf("неверный ответ завершающегося запроса: %+v", ticket)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("активный запрос не завершился")
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("процесс не завершился после успешного запроса")
	}
	var count int
	if err := db.Pool.QueryRow(context.Background(), "SELECT count(*) FROM tickets WHERE status='serving' AND operator_id=1").Scan(&count); err != nil || count != 1 {
		t.Fatalf("результат не сохранён: serving=%d, ошибка=%v", count, err)
	}
}

func TestForcedShutdownRollsBackBlockedRequest(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM дочернему процессу проверяется в Linux-контейнере")
	}
	db := testutil.New(t, 3)
	process := startAPI(t, db, "SHUTDOWN_TIMEOUT=200ms")
	client := clientForTest(t)
	checkReply(t, request(client, http.MethodPost, process.URL+"/tickets", nil), http.StatusCreated, nil)
	lock := lockOperator(t, db)
	result := make(chan reply, 1)
	go func() { result <- request(client, http.MethodPost, process.URL+"/operators/1/next", nil) }()
	waitForOperatorWait(t, db, process)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := process.Stop(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-result:
		if response.err == nil && response.status == http.StatusOK {
			t.Fatalf("заблокированный запрос неожиданно получил талон: %s", response.body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("отменённый HTTP-запрос не завершился")
	}
	if !strings.Contains(process.Logs(), "Истёк срок завершения запросов") {
		t.Fatalf("не сработал принудительный shutdown:\n%s", process.Logs())
	}
	var waiting, assigned, childSessions int
	if err := db.Pool.QueryRow(context.Background(), "SELECT count(*) FILTER (WHERE status='waiting'), count(*) FILTER (WHERE operator_id IS NOT NULL OR started_at IS NOT NULL OR finished_at IS NOT NULL) FROM tickets").Scan(&waiting, &assigned); err != nil {
		t.Fatal(err)
	}
	if waiting != 1 || assigned != 0 {
		t.Fatalf("осталось частичное назначение: waiting=%d, assigned=%d", waiting, assigned)
	}
	if err := db.Pool.QueryRow(context.Background(), "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1", process.ApplicationName).Scan(&childSessions); err != nil {
		t.Fatal(err)
	}
	if childSessions != 0 {
		t.Fatalf("после выхода осталось %d соединений процесса", childSessions)
	}
}

func lockOperator(t *testing.T, db *testutil.Database) pgx.Tx {
	t.Helper()
	tx, err := db.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(context.Background(), "SELECT id FROM operators WHERE id=1 FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	return tx
}

func waitForOperatorWait(t *testing.T, db *testutil.Database, process *testutil.Process) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var blocked bool
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := db.Pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE application_name=$1 AND wait_event_type='Lock' AND state='active'
		)`, process.ApplicationName).Scan(&blocked)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("не подтверждено ожидание блокировки оператором в PostgreSQL:\n%s", process.Logs())
}

func waitForShutdown(t *testing.T, process *testutil.Process) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(process.Logs(), "Остановка HTTP-сервера") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("процесс не начал shutdown после SIGTERM:\n%s", process.Logs())
}
