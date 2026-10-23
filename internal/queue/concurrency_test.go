package queue_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"electronic-queue-api/internal/queue"
	"electronic-queue-api/internal/testutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type concurrentResult struct {
	operatorID int64
	ticket     queue.Ticket
	err        error
}

// Общий барьер не даёт первым работникам закончить до запуска остальных.
// Результаты проверяет тестовая goroutine: Fatal внутри работника не используется.
func runConcurrent(count int, operation func(int) concurrentResult) []concurrentResult {
	start := make(chan struct{})
	results := make(chan concurrentResult, count)
	var ready sync.WaitGroup
	ready.Add(count)
	for i := range count {
		go func() {
			ready.Done()
			<-start
			results <- operation(i)
		}()
	}
	ready.Wait()
	close(start)
	collected := make([]concurrentResult, count)
	for i := range collected {
		collected[i] = <-results
	}
	return collected
}

// Проверяем и возвращённые назначения, и сохранённое состояние. Проверка
// одной лишь БД не заметила бы ошибочный повтор одного талона в двух ответах.
func verifyServingAssignments(t *testing.T, ctx context.Context, db *testutil.Database, assignments map[int64]int64, total int) {
	t.Helper()
	rows, err := db.Pool.Query(ctx, "SELECT id, status, operator_id, started_at, finished_at FROM tickets ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := 0
	serving := 0
	owners := make(map[int64]bool)
	for rows.Next() {
		var id int64
		var status string
		var owner *int64
		var started, finished *time.Time
		if err := rows.Scan(&id, &status, &owner, &started, &finished); err != nil {
			t.Fatal(err)
		}
		seen++
		if expectedOwner, ok := assignments[id]; ok {
			if status != queue.StatusServing || owner == nil || *owner != expectedOwner || started == nil || finished != nil {
				t.Fatalf("назначение id=%d в БД: status=%s owner=%v started=%v finished=%v, ожидался оператор %d", id, status, owner, started, finished, expectedOwner)
			}
			if owners[*owner] {
				t.Fatalf("оператор %d имеет несколько активных талонов", *owner)
			}
			owners[*owner] = true
			serving++
		} else if status != queue.StatusWaiting || owner != nil || started != nil || finished != nil {
			t.Fatalf("невыданный талон id=%d изменён: status=%s owner=%v started=%v finished=%v", id, status, owner, started, finished)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != total || serving != len(assignments) {
		t.Fatalf("в БД всего %d/%d талонов, serving %d/%d", seen, total, serving, len(assignments))
	}
}

func TestConcurrentNext100Operators(t *testing.T) {
	ctx, db, store := setup(t, 100)
	createTickets(t, ctx, store, 100)
	results := runConcurrent(100, func(i int) concurrentResult {
		operatorID := int64(i + 1)
		ticket, err := store.Next(ctx, operatorID)
		return concurrentResult{operatorID: operatorID, ticket: ticket, err: err}
	})
	assignments := make(map[int64]int64)
	numbers := make(map[string]bool)
	owners := make(map[int64]bool)
	for _, result := range results {
		if result.err != nil {
			t.Fatalf("next оператора %d: %v", result.operatorID, result.err)
		}
		if _, duplicate := assignments[result.ticket.ID]; duplicate || numbers[result.ticket.Number] || owners[result.operatorID] {
			t.Fatalf("повтор выдачи: оператор %d, талон %+v", result.operatorID, result.ticket)
		}
		if result.ticket.OperatorID == nil || *result.ticket.OperatorID != result.operatorID || result.ticket.Status != queue.StatusServing {
			t.Fatalf("ответ содержит неверное назначение: %+v", result)
		}
		assignments[result.ticket.ID] = result.operatorID
		numbers[result.ticket.Number] = true
		owners[result.operatorID] = true
	}
	verifyServingAssignments(t, ctx, db, assignments, 100)
	t.Logf("100 одновременных операторов: %d уникальных id, %d уникальных номеров; состояние БД проверено", len(assignments), len(numbers))
}

func TestConcurrentNextSameOperator(t *testing.T) {
	ctx, db, store := setup(t, 1)
	createTickets(t, ctx, store, 100)
	results := runConcurrent(100, func(_ int) concurrentResult {
		ticket, err := store.Next(ctx, 1)
		return concurrentResult{operatorID: 1, ticket: ticket, err: err}
	})
	success, busy := 0, 0
	assignments := make(map[int64]int64)
	var busyTicketIDs []int64
	for _, result := range results {
		if result.err == nil {
			success++
			assignments[result.ticket.ID] = 1
		} else {
			domain := requireCode(t, result.err, "operator_busy")
			busyTicketIDs = append(busyTicketIDs, domain.CurrentTicketID)
			busy++
		}
	}
	if success != 1 || busy != 99 {
		t.Fatalf("успешных выдач %d, конфликтов %d; ожидалось 1 и 99", success, busy)
	}
	for _, id := range busyTicketIDs {
		if _, ok := assignments[id]; !ok {
			t.Fatalf("конфликт сослался на невыданный талон %d", id)
		}
	}
	verifyServingAssignments(t, ctx, db, assignments, 100)
}

func TestConcurrentTwoOperatorsOneTicket(t *testing.T) {
	ctx, db, store := setup(t, 2)
	created := createTickets(t, ctx, store, 1)
	results := runConcurrent(2, func(i int) concurrentResult {
		operatorID := int64(i + 1)
		ticket, err := store.Next(ctx, operatorID)
		return concurrentResult{operatorID: operatorID, ticket: ticket, err: err}
	})
	assignments := make(map[int64]int64)
	empty := 0
	for _, result := range results {
		switch {
		case result.err == nil:
			if result.ticket.ID != created[0].ID {
				t.Fatalf("получен неожиданный талон: %+v", result.ticket)
			}
			if _, duplicate := assignments[result.ticket.ID]; duplicate {
				t.Fatal("единственный талон возвращён дважды")
			}
			assignments[result.ticket.ID] = result.operatorID
		case errors.Is(result.err, queue.ErrEmptyQueue):
			empty++
		default:
			t.Fatalf("неожиданная ошибка: %v", result.err)
		}
	}
	if len(assignments) != 1 || empty != 1 {
		t.Fatalf("назначений %d, пустых ответов %d; ожидалось 1 и 1", len(assignments), empty)
	}
	verifyServingAssignments(t, ctx, db, assignments, 1)
}

func TestConcurrentFinish(t *testing.T) {
	for _, secondOperation := range []string{"complete", "skip"} {
		t.Run("complete_"+secondOperation, func(t *testing.T) {
			ctx, _, store := setup(t, 1)
			createTickets(t, ctx, store, 1)
			serving, err := store.Next(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			results := runConcurrent(2, func(i int) concurrentResult {
				finish := store.Complete
				if i == 1 && secondOperation == "skip" {
					finish = store.Skip
				}
				ticket, err := finish(ctx, 1, serving.ID)
				return concurrentResult{operatorID: 1, ticket: ticket, err: err}
			})
			success, conflicts := 0, 0
			var winner queue.Ticket
			for _, result := range results {
				if result.err == nil {
					if result.ticket.FinishedAt == nil || result.ticket.StartedAt == nil || result.ticket.OperatorID == nil || *result.ticket.OperatorID != 1 {
						t.Fatalf("успешный ответ не содержит историю обслуживания: %+v", result.ticket)
					}
					if result.ticket.Status != queue.StatusCompleted && result.ticket.Status != queue.StatusSkipped {
						t.Fatalf("завершение вернуло неверное состояние: %+v", result.ticket)
					}
					success++
					winner = result.ticket
				} else {
					requireCode(t, result.err, "invalid_ticket_state")
					conflicts++
				}
			}
			if success != 1 || conflicts != 1 {
				t.Fatalf("успешных завершений %d, конфликтов %d; ожидалось 1 и 1", success, conflicts)
			}
			persisted, err := store.GetTicket(ctx, serving.ID)
			if err != nil || persisted.Status != winner.Status || persisted.OperatorID == nil || *persisted.OperatorID != 1 || persisted.FinishedAt == nil || persisted.StartedAt == nil {
				t.Fatalf("конкурентное завершение потеряло историю: %+v, %v", persisted, err)
			}
			if !persisted.StartedAt.Equal(*serving.StartedAt) || !persisted.FinishedAt.Equal(*winner.FinishedAt) {
				t.Fatalf("время победившей операции не сохранилось: %+v", persisted)
			}
			operators, err := store.ListOperators(ctx)
			if err != nil || operators[0].CurrentTicketID != nil {
				t.Fatalf("оператор не освободился: %+v, %v", operators, err)
			}
		})
	}
}

func TestConcurrentNextAndComplete(t *testing.T) {
	ctx, db, store := setup(t, 1)
	created := createTickets(t, ctx, store, 2)
	first, err := store.Next(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	results := runConcurrent(2, func(i int) concurrentResult {
		if i == 0 {
			ticket, err := store.Complete(ctx, 1, first.ID)
			return concurrentResult{operatorID: 0, ticket: ticket, err: err}
		}
		ticket, err := store.Next(ctx, 1)
		return concurrentResult{operatorID: 1, ticket: ticket, err: err}
	})
	for _, result := range results {
		if result.operatorID == 0 {
			if result.err != nil || result.ticket.ID != first.ID || result.ticket.Status != queue.StatusCompleted {
				t.Fatalf("конкурентное завершение: %+v, %v", result.ticket, result.err)
			}
			continue
		}
		// Next либо выполняется после Complete и выдаёт A002, либо раньше
		// и отклоняется как busy. Оба результата соответствуют сериализации.
		if result.err == nil {
			if result.ticket.ID != created[1].ID {
				t.Fatalf("выдан неверный следующий талон: %+v", result.ticket)
			}
		} else {
			busy := requireCode(t, result.err, "operator_busy")
			if busy.CurrentTicketID != first.ID {
				t.Fatalf("конфликт ссылается не на прежний активный талон: %+v", busy)
			}
			second, err := store.Next(ctx, 1)
			if err != nil || second.ID != created[1].ID {
				t.Fatalf("после завершения не удалось получить следующий талон: %+v, %v", second, err)
			}
		}
	}
	var completed, serving int
	if err := db.Pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE status = 'completed'),
		count(*) FILTER (WHERE status = 'serving' AND operator_id = 1)
		FROM tickets`).Scan(&completed, &serving); err != nil || completed != 1 || serving != 1 {
		t.Fatalf("после next/complete нарушены состояния: completed=%d serving=%d, %v", completed, serving, err)
	}
}

func rollbackTestTransaction(t *testing.T, tx pgx.Tx) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Errorf("откатить удерживаемую тестовую транзакцию: %v", err)
	}
}

func TestSkipLocked(t *testing.T) {
	t.Run("first_ticket_locked", func(t *testing.T) {
		ctx, db, store := setup(t, 2)
		created := createTickets(t, ctx, store, 2)
		tx, err := db.Pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer rollbackTestTransaction(t, tx)
		if _, err := tx.Exec(ctx, "SELECT id FROM tickets WHERE id = $1 FOR UPDATE", created[0].ID); err != nil {
			t.Fatal(err)
		}
		// Блокировка удерживается до окончания Next, поэтому тест не зависит
		// от удачного расположения двух goroutine по времени.
		claimCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		ticket, err := store.Next(claimCtx, 1)
		if err != nil || ticket.ID != created[1].ID {
			t.Fatalf("заблокированный первый талон не пропущен: %+v, %v", ticket, err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		next, err := store.Next(ctx, 2)
		if err != nil || next.ID != created[0].ID {
			t.Fatalf("после отката первый талон не вернулся в доступные: %+v, %v", next, err)
		}
		verifyServingAssignments(t, ctx, db, map[int64]int64{created[0].ID: 2, created[1].ID: 1}, 2)
	})
	t.Run("all_waiting_locked", func(t *testing.T) {
		ctx, db, store := setup(t, 1)
		created := createTickets(t, ctx, store, 2)
		tx, err := db.Pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer rollbackTestTransaction(t, tx)
		if _, err := tx.Exec(ctx, "SELECT id FROM tickets ORDER BY id FOR UPDATE"); err != nil {
			t.Fatal(err)
		}
		claimCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err = store.Next(claimCtx, 1)
		if !errors.Is(err, queue.ErrEmptyQueue) {
			t.Fatalf("все строки заблокированы: ожидалось отсутствие доступных талонов, получено %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		ticket, err := store.Next(ctx, 1)
		if err != nil || ticket.ID != created[0].ID {
			t.Fatalf("после освобождения строк выдача не восстановилась: %+v, %v", ticket, err)
		}
		verifyServingAssignments(t, ctx, db, map[int64]int64{created[0].ID: 1}, 2)
	})
}

func TestCancellationWhileOperatorLocked(t *testing.T) {
	ctx, db, store := setup(t, 1)
	created := createTickets(t, ctx, store, 1)
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTestTransaction(t, tx)
	if _, err := tx.Exec(ctx, "SELECT id FROM operators WHERE id = 1 FOR UPDATE"); err != nil {
		t.Fatal(err)
	}

	// Отдельное имя подключения позволяет дождаться фактического ожидания
	// блокировки в PostgreSQL, а не отменять запрос по произвольному sleep.
	config := db.Config.Copy()
	applicationName := "cancel_" + db.Schema
	config.ConnConfig.RuntimeParams["application_name"] = applicationName
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	cancelStore := queue.New(pool)
	claimCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := cancelStore.Next(claimCtx, 1)
		result <- err
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		err := db.Pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE application_name = $1 AND wait_event_type = 'Lock'
		)`, applicationName).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-result:
			t.Fatalf("Next завершился до отмены удерживаемой блокировки: %v", err)
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("запрос не начал ожидать блокировку оператора")
		}
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ожидалась отмена запроса, получено %v", err)
		}
	case <-ctx.Done():
		t.Fatal("отмена не прервала ожидание блокировки")
	}
	verifyServingAssignments(t, ctx, db, map[int64]int64{}, 1)
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// Используем тот же пул, где произошла отмена: он должен освободить
	// транзакцию и остаться пригодным для последующей записи.
	ticket, err := cancelStore.Next(ctx, 1)
	if err != nil || ticket.ID != created[0].ID {
		t.Fatalf("пул после отмены не восстановился: %+v, %v", ticket, err)
	}
	verifyServingAssignments(t, ctx, db, map[int64]int64{created[0].ID: 1}, 1)
}

func TestConcurrentTicketCreation(t *testing.T) {
	ctx, db, store := setup(t, 1)
	results := runConcurrent(1000, func(_ int) concurrentResult {
		ticket, err := store.CreateTicket(ctx)
		return concurrentResult{ticket: ticket, err: err}
	})
	ids := make(map[int64]bool)
	numbers := make(map[string]bool)
	for _, result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		ticket := result.ticket
		if ids[ticket.ID] || numbers[ticket.Number] {
			t.Fatalf("повтор идентификатора или номера: %+v", ticket)
		}
		if ticket.Number != fmt.Sprintf("A%03d", ticket.ID) || ticket.Status != queue.StatusWaiting {
			t.Fatalf("неверный номер или начальный статус: %+v", ticket)
		}
		ids[ticket.ID] = true
		numbers[ticket.Number] = true
	}
	if !numbers["A001"] || !numbers["A999"] || !numbers["A1000"] {
		t.Fatalf("номера не охватывают границу трёх цифр: A001=%t A999=%t A1000=%t", numbers["A001"], numbers["A999"], numbers["A1000"])
	}
	verifyServingAssignments(t, ctx, db, map[int64]int64{}, 1000)
}

func TestFIFOWithConcurrentUncommittedCreation(t *testing.T) {
	ctx, db, store := setup(t, 2)
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTestTransaction(t, tx)
	var firstID int64
	if err := tx.QueryRow(ctx, "INSERT INTO tickets DEFAULT VALUES RETURNING id").Scan(&firstID); err != nil {
		t.Fatal(err)
	}
	second, err := store.CreateTicket(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID <= firstID {
		t.Fatalf("identity не сохранила порядок идентификаторов: first=%d second=%d", firstID, second.ID)
	}
	// Незавершённая вставка A001 невидима другим транзакциям: первым
	// доступным становится уже зафиксированный A002.
	claimed, err := store.Next(ctx, 1)
	if err != nil || claimed.ID != second.ID {
		t.Fatalf("зафиксированный талон недоступен до чужого COMMIT: %+v, %v", claimed, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.Next(ctx, 2)
	if err != nil || claimed.ID != firstID {
		t.Fatalf("после COMMIT ранее созданный талон недоступен: %+v, %v", claimed, err)
	}
	verifyServingAssignments(t, ctx, db, map[int64]int64{firstID: 2, second.ID: 1}, 2)
}

func checkSchemaConnections(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema string, count int) {
	t.Helper()
	backendIDs := make(map[uint32]bool)
	// Соединения удерживаются одновременно: повторное Acquire после Release
	// могло бы проверить одну и ту же физическую сессию три раза.
	for range 3 {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		var actualSchema string
		var actualCount int
		if err := conn.QueryRow(ctx, "SELECT current_schema(), (SELECT count(*) FROM tickets)").Scan(&actualSchema, &actualCount); err != nil {
			t.Fatal(err)
		}
		if actualSchema != schema || actualCount != count {
			t.Fatalf("сессия попала в чужую схему: schema=%s count=%d, ожидалось %s/%d", actualSchema, actualCount, schema, count)
		}
		pid := conn.Conn().PgConn().PID()
		if backendIDs[pid] {
			t.Fatalf("повторно проверено одно соединение pid=%d", pid)
		}
		backendIDs[pid] = true
	}
}

func TestSchemaIsolationAcrossPools(t *testing.T) {
	ctx, first, firstStore := setup(t, 1)
	_, second, secondStore := setup(t, 1)
	if first.Schema == second.Schema {
		t.Fatal("две группы получили одинаковое имя схемы")
	}
	createTickets(t, ctx, firstStore, 1)
	createTickets(t, ctx, secondStore, 2)
	for i, db := range []*testutil.Database{first, second} {
		wantCount := i + 1
		checkSchemaConnections(t, ctx, db.Pool, db.Schema, wantCount)
		config := db.Config.Copy()
		originalName := db.Config.ConnConfig.RuntimeParams["application_name"]
		config.ConnConfig.RuntimeParams["application_name"] = "copied_" + db.Schema
		if db.Config.ConnConfig.RuntimeParams["application_name"] != originalName {
			t.Fatal("Config.Copy изменил RuntimeParams исходной конфигурации")
		}
		copiedPool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		defer copiedPool.Close()
		checkSchemaConnections(t, ctx, copiedPool, db.Schema, wantCount)
		urlPool, err := pgxpool.New(ctx, db.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer urlPool.Close()
		checkSchemaConnections(t, ctx, urlPool, db.Schema, wantCount)
	}
}
