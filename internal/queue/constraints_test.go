package queue_test

import (
	"errors"
	"testing"
	"time"

	"electronic-queue-api/internal/queue"

	"github.com/jackc/pgx/v5/pgconn"
)

func requireSQLState(t *testing.T, err error, code string) *pgconn.PgError {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != code {
		t.Fatalf("ожидалась ошибка PostgreSQL %s, получено: %v", code, err)
	}
	return pgErr
}

func TestDatabaseStateConstraints(t *testing.T) {
	ctx, db, _ := setup(t, 1)
	created := time.Date(2026, time.October, 18, 10, 0, 0, 0, time.UTC)
	started := created.Add(time.Second)
	finished := started.Add(time.Second)
	tests := []struct {
		name     string
		status   any
		operator any
		created  any
		started  any
		finished any
		code     string
	}{
		{"unknown_status", "unknown", nil, created, nil, nil, "23514"},
		{"null_status", nil, nil, created, nil, nil, "23502"},
		{"null_creation_time", queue.StatusWaiting, nil, nil, nil, nil, "23502"},
		{"waiting_with_operator", queue.StatusWaiting, int64(1), created, nil, nil, "23514"},
		{"waiting_with_start", queue.StatusWaiting, nil, created, started, nil, "23514"},
		{"waiting_with_finish", queue.StatusWaiting, nil, created, nil, finished, "23514"},
		{"serving_without_operator", queue.StatusServing, nil, created, started, nil, "23514"},
		{"serving_without_start", queue.StatusServing, int64(1), created, nil, nil, "23514"},
		{"serving_with_finish", queue.StatusServing, int64(1), created, started, finished, "23514"},
		{"completed_without_operator", queue.StatusCompleted, nil, created, started, finished, "23514"},
		{"completed_without_start", queue.StatusCompleted, int64(1), created, nil, finished, "23514"},
		{"completed_without_finish", queue.StatusCompleted, int64(1), created, started, nil, "23514"},
		{"skipped_without_operator", queue.StatusSkipped, nil, created, started, finished, "23514"},
		{"skipped_without_start", queue.StatusSkipped, int64(1), created, nil, finished, "23514"},
		{"skipped_without_finish", queue.StatusSkipped, int64(1), created, started, nil, "23514"},
		{"start_before_creation", queue.StatusServing, int64(1), created, created.Add(-time.Second), nil, "23514"},
		{"finish_before_start", queue.StatusCompleted, int64(1), created, started, created, "23514"},
		{"missing_operator", queue.StatusServing, int64(999), created, started, nil, "23503"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := db.Pool.Exec(ctx, `
				INSERT INTO tickets (status, operator_id, created_at, started_at, finished_at)
				VALUES ($1, $2, $3, $4, $5)`, test.status, test.operator, test.created, test.started, test.finished)
			requireSQLState(t, err, test.code)
		})
	}
	var count int
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM tickets").Scan(&count); err != nil || count != 0 {
		t.Fatalf("некорректные вставки оставили данные: count=%d, %v", count, err)
	}
	for _, name := range []any{"", "   ", nil} {
		_, err := db.Pool.Exec(ctx, "INSERT INTO operators (name) VALUES ($1)", name)
		code := "23514"
		if name == nil {
			code = "23502"
		}
		requireSQLState(t, err, code)
	}
}

func TestDatabaseOneServingPerOperator(t *testing.T) {
	ctx, db, store := setup(t, 1)
	created := createTickets(t, ctx, store, 3)
	first, err := store.Next(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Обходим Store прямой записью: индекс обязан защищать инвариант
	// независимо от дисциплины блокировок в Go-коде.
	_, err = db.Pool.Exec(ctx, `UPDATE tickets
		SET status = 'serving', operator_id = 1, started_at = clock_timestamp()
		WHERE id = $1`, created[1].ID)
	pgErr := requireSQLState(t, err, "23505")
	if pgErr.ConstraintName != "tickets_one_serving_per_operator" {
		t.Fatalf("нарушено неожиданное ограничение: %s", pgErr.ConstraintName)
	}
	verifyServingAssignments(t, ctx, db, map[int64]int64{first.ID: 1}, 3)
	if _, err := store.Complete(ctx, 1, first.ID); err != nil {
		t.Fatal(err)
	}
	second, err := store.Next(ctx, 1)
	if err != nil || second.ID != created[1].ID {
		t.Fatalf("completed не должен занимать уникальный активный слот: %+v, %v", second, err)
	}
	if _, err := store.Skip(ctx, 1, second.ID); err != nil {
		t.Fatal(err)
	}
	third, err := store.Next(ctx, 1)
	if err != nil || third.ID != created[2].ID {
		t.Fatalf("skipped не должен занимать уникальный активный слот: %+v, %v", third, err)
	}
}

func TestDatabaseOperatorDeletionRestricted(t *testing.T) {
	ctx, db, store := setup(t, 1)
	createTickets(t, ctx, store, 1)
	serving, err := store.Next(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Pool.Exec(ctx, "DELETE FROM operators WHERE id = 1")
	requireSQLState(t, err, "23503")
	if _, err := store.Complete(ctx, 1, serving.ID); err != nil {
		t.Fatal(err)
	}
	_, err = db.Pool.Exec(ctx, "DELETE FROM operators WHERE id = 1")
	requireSQLState(t, err, "23503")
	history, err := store.GetTicket(ctx, serving.ID)
	if err != nil || history.Status != queue.StatusCompleted || history.OperatorID == nil || *history.OperatorID != 1 {
		t.Fatalf("попытка удаления потеряла историю: %+v, %v", history, err)
	}
}

func TestNextRollbackOnCommitFailure(t *testing.T) {
	ctx, db, store := setup(t, 1)
	created := createTickets(t, ctx, store, 1)
	// Отложенный триггер срабатывает при COMMIT, уже после успешного UPDATE
	// RETURNING. API не должен вернуть успешную выдачу до фиксации транзакции.
	_, err := db.Pool.Exec(ctx, `
		CREATE FUNCTION test_reject_serving() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.status = 'serving' THEN
				RAISE EXCEPTION 'test: commit rejected';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE CONSTRAINT TRIGGER test_reject_commit
		AFTER UPDATE ON tickets DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE FUNCTION test_reject_serving();`)
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := store.Next(ctx, 1)
	requireSQLState(t, err, "P0001")
	if ticket.ID != 0 {
		t.Fatalf("отклонённая при COMMIT выдача вернула талон: %+v", ticket)
	}
	verifyServingAssignments(t, ctx, db, map[int64]int64{}, 1)
	if _, err := db.Pool.Exec(ctx, "DROP TRIGGER test_reject_commit ON tickets"); err != nil {
		t.Fatal(err)
	}
	ticket, err = store.Next(ctx, 1)
	if err != nil || ticket.ID != created[0].ID {
		t.Fatalf("после отката транзакция или пул не освободились: %+v, %v", ticket, err)
	}
	verifyServingAssignments(t, ctx, db, map[int64]int64{created[0].ID: 1}, 1)
}
