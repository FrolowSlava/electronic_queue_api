package queue

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"electronic-queue-api/internal/testutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Только подтверждение заменено ошибкой: изменения и COMMIT выполняет
// настоящая PostgreSQL. Это моделирует потерю ответа после фиксации.
type commitReplyFault struct {
	pgx.Tx
	commit func(context.Context) error
}

func (tx commitReplyFault) Commit(ctx context.Context) error { return tx.commit(ctx) }

func pendingTicketCommit(t *testing.T, status string) (context.Context, *testutil.Database, *Store, pgx.Tx, Ticket) {
	t.Helper()
	db := testutil.New(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	store := New(db.Pool)
	created, err := store.CreateTicket(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status != StatusServing {
		if _, err := store.Next(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rollback(tx) })
	if _, err := lockOperator(ctx, tx, 1); err != nil {
		t.Fatal(err)
	}
	query := `UPDATE tickets SET status='serving', operator_id=1,
		started_at=clock_timestamp() WHERE id=$1 RETURNING ` + ticketColumns
	args := []any{created.ID}
	if status != StatusServing {
		query = `UPDATE tickets SET status=$2, finished_at=clock_timestamp()
			WHERE id=$1 RETURNING ` + ticketColumns
		args = append(args, status)
	}
	expected, err := scanTicket(tx.QueryRow(ctx, query, args...))
	if err != nil {
		t.Fatal(err)
	}
	return ctx, db, store, tx, expected
}

func requireUnknownCommit(t *testing.T, ticket Ticket, err error) {
	t.Helper()
	if !errors.Is(err, ErrCommitOutcomeUnknown) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ожидался неизвестный исход с исходной ошибкой связи: ticket=%+v err=%v", ticket, err)
	}
	if ticket.ID != 0 {
		t.Fatalf("неподтверждённая операция вернула талон: %+v", ticket)
	}
}

func TestCommitTicketRecoversLostAcknowledgement(t *testing.T) {
	for _, status := range []string{StatusServing, StatusCompleted, StatusSkipped} {
		t.Run(status, func(t *testing.T) {
			ctx, _, store, tx, expected := pendingTicketCommit(t, status)
			commits := 0
			fault := commitReplyFault{Tx: tx, commit: func(ctx context.Context) error {
				commits++
				if err := tx.Commit(ctx); err != nil {
					return err
				}
				return io.ErrUnexpectedEOF
			}}
			got, err := store.commitTicket(ctx, fault, expected, "test")
			if err != nil || !reflect.DeepEqual(got, expected) || commits != 1 {
				t.Fatalf("подтверждённый результат не восстановлен: got=%+v want=%+v commits=%d err=%v", got, expected, commits, err)
			}
			stored, err := store.GetTicket(ctx, expected.ID)
			if err != nil || !reflect.DeepEqual(stored, expected) {
				t.Fatalf("проверка изменила результат операции: %+v, %v", stored, err)
			}
		})
	}
}

func TestCommitTicketRecoversWithCanceledRequestContext(t *testing.T) {
	ctx, _, store, tx, expected := pendingTicketCommit(t, StatusServing)
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	fault := commitReplyFault{Tx: tx, commit: func(ctx context.Context) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		cancel()
		return requestCtx.Err()
	}}
	got, err := store.commitTicket(requestCtx, fault, expected, "next")
	if requestCtx.Err() != context.Canceled || err != nil || !reflect.DeepEqual(got, expected) {
		t.Fatalf("проверка зависела от отменённого HTTP-контекста: %+v, %v", got, err)
	}
}

func TestCommitTicketDoesNotConfirmRollback(t *testing.T) {
	ctx, _, store, tx, expected := pendingTicketCommit(t, StatusServing)
	fault := commitReplyFault{Tx: tx, commit: func(ctx context.Context) error {
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}}
	got, err := store.commitTicket(ctx, fault, expected, "next")
	requireUnknownCommit(t, got, err)
	stored, err := store.GetTicket(ctx, expected.ID)
	if err != nil || stored.Status != StatusWaiting || stored.OperatorID != nil || stored.StartedAt != nil {
		t.Fatalf("проверка повторила назначение после отката: %+v, %v", stored, err)
	}
	if _, err := store.Next(ctx, 1); err != nil {
		t.Fatalf("последующая явная выдача не работает: %v", err)
	}
}

func TestCommitTicketRejectsDifferentStoredResult(t *testing.T) {
	for _, change := range []string{"owner", "started_at", "created_at", "finished_at", "finished_ticket"} {
		t.Run(change, func(t *testing.T) {
			status := StatusServing
			if change == "finished_at" {
				status = StatusCompleted
			}
			ctx, db, store, tx, expected := pendingTicketCommit(t, status)
			fault := commitReplyFault{Tx: tx, commit: func(ctx context.Context) error {
				if err := tx.Commit(ctx); err != nil {
					return err
				}
				query := ""
				var value any
				switch change {
				case "owner":
					query, value = "UPDATE tickets SET operator_id=$2 WHERE id=$1", int64(2)
				case "started_at":
					query, value = "UPDATE tickets SET started_at=$2 WHERE id=$1", expected.StartedAt.Add(time.Second)
				case "created_at":
					query, value = "UPDATE tickets SET created_at=$2 WHERE id=$1", expected.CreatedAt.Add(-time.Second)
				case "finished_at":
					query, value = "UPDATE tickets SET finished_at=$2 WHERE id=$1", expected.FinishedAt.Add(time.Second)
				case "finished_ticket":
					_, err := store.Complete(ctx, 1, expected.ID)
					if err != nil {
						return err
					}
				}
				if query != "" {
					if _, err := db.Pool.Exec(ctx, query, expected.ID, value); err != nil {
						return err
					}
				}
				return io.ErrUnexpectedEOF
			}}
			got, err := store.commitTicket(ctx, fault, expected, "test")
			requireUnknownCommit(t, got, err)
		})
	}
}

func TestCommitTicketDoesNotReturnLaterAssignment(t *testing.T) {
	ctx, _, store, tx, expected := pendingTicketCommit(t, StatusServing)
	var later Ticket
	fault := commitReplyFault{Tx: tx, commit: func(ctx context.Context) error {
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		var err error
		later, err = store.Next(ctx, 1)
		if err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}}
	got, err := store.commitTicket(ctx, fault, expected, "next")
	requireUnknownCommit(t, got, err)
	if later.ID != expected.ID || later.StartedAt.Equal(*expected.StartedAt) {
		t.Fatalf("тест не создал другое назначение того же талона: before=%+v after=%+v", expected, later)
	}
	stored, err := store.GetTicket(ctx, later.ID)
	if err != nil || !reflect.DeepEqual(stored, later) {
		t.Fatalf("проверка затронула более позднюю выдачу: %+v, %v", stored, err)
	}
}

func TestRecoverTicketWaitsForTransactionAndFreshSnapshot(t *testing.T) {
	ctx, db, _, tx, expected := pendingTicketCommit(t, StatusServing)
	config := db.Config.Copy()
	applicationName := "recover_" + db.Schema
	config.ConnConfig.RuntimeParams["application_name"] = applicationName
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	type result struct {
		ticket Ticket
		err    error
	}
	results := make(chan result, 1)
	go func() {
		ticket, err := New(pool).recoverTicket(ctx, expected)
		results <- result{ticket, err}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := db.Pool.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE application_name=$1 AND wait_event_type='Lock'
		)`, applicationName).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case got := <-results:
			t.Fatalf("проверка завершилась до исхода удерживаемой транзакции: %+v, %v", got.ticket, got.err)
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("проверка не начала ждать прежнюю транзакцию")
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-results:
		if got.err != nil || !reflect.DeepEqual(got.ticket, expected) {
			t.Fatalf("после ожидания использован устаревший снимок: %+v, %v", got.ticket, got.err)
		}
	case <-ctx.Done():
		t.Fatal("проверка не завершилась после COMMIT")
	}
}

func TestCommitTicketReportsUnavailableVerification(t *testing.T) {
	ctx, db, _, tx, expected := pendingTicketCommit(t, StatusServing)
	pool, err := pgxpool.NewWithConfig(ctx, db.Config.Copy())
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	fault := commitReplyFault{Tx: tx, commit: func(ctx context.Context) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}}
	got, err := New(pool).commitTicket(ctx, fault, expected, "next")
	requireUnknownCommit(t, got, err)
	stored, err := New(db.Pool).GetTicket(ctx, expected.ID)
	if err != nil || !reflect.DeepEqual(stored, expected) {
		t.Fatalf("потерян уже зафиксированный результат: %+v, %v", stored, err)
	}
}
