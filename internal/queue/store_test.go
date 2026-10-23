package queue_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"electronic-queue-api/internal/queue"
	"electronic-queue-api/internal/testutil"
)

func setup(t *testing.T, operators int) (context.Context, *testutil.Database, *queue.Store) {
	t.Helper()
	db := testutil.New(t, operators)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx, db, queue.New(db.Pool)
}

func createTickets(t *testing.T, ctx context.Context, store *queue.Store, count int) []queue.Ticket {
	t.Helper()
	tickets := make([]queue.Ticket, count)
	for i := range tickets {
		ticket, err := store.CreateTicket(ctx)
		if err != nil {
			t.Fatalf("создать талон %d: %v", i, err)
		}
		tickets[i] = ticket
	}
	return tickets
}

func requireCode(t *testing.T, err error, code string) *queue.Error {
	t.Helper()
	var domain *queue.Error
	if !errors.As(err, &domain) || domain.Code != code {
		t.Fatalf("ожидалась ошибка %q, получено: %v", code, err)
	}
	return domain
}

func TestFIFO(t *testing.T) {
	ctx, _, store := setup(t, 3)
	created := createTickets(t, ctx, store, 3)
	for i, original := range created {
		if original.Status != queue.StatusWaiting || original.OperatorID != nil || original.StartedAt != nil || original.FinishedAt != nil {
			t.Fatalf("неверные поля нового талона: %+v", original)
		}
		if want := fmt.Sprintf("A%03d", i+1); original.Number != want {
			t.Fatalf("номер = %q, ожидался %q", original.Number, want)
		}
		ticket, err := store.Next(ctx, int64(i+1))
		if err != nil {
			t.Fatalf("next оператора %d: %v", i+1, err)
		}
		if ticket.ID != original.ID || ticket.Number != original.Number || ticket.Status != queue.StatusServing {
			t.Fatalf("нарушен FIFO: получен %+v, ожидался id=%d", ticket, original.ID)
		}
		if ticket.OperatorID == nil || *ticket.OperatorID != int64(i+1) || ticket.StartedAt == nil || ticket.FinishedAt != nil {
			t.Fatalf("неверные поля назначения: %+v", ticket)
		}
		if ticket.StartedAt.Before(ticket.CreatedAt) {
			t.Fatal("начало обслуживания раньше создания")
		}
	}
}

func TestEmptyQueue(t *testing.T) {
	ctx, _, store := setup(t, 1)
	_, err := store.Next(ctx, 1)
	if !errors.Is(err, queue.ErrEmptyQueue) {
		t.Fatalf("ожидалась пустая очередь, получено %v", err)
	}
	operators, err := store.ListOperators(ctx)
	if err != nil || operators[0].CurrentTicketID != nil {
		t.Fatalf("пустая выдача изменила оператора: %+v, %v", operators, err)
	}
}

func TestFinishHistoryAndRelease(t *testing.T) {
	for _, finalStatus := range []string{queue.StatusCompleted, queue.StatusSkipped} {
		t.Run(finalStatus, func(t *testing.T) {
			ctx, _, store := setup(t, 1)
			created := createTickets(t, ctx, store, 2)
			serving, err := store.Next(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			finish := store.Complete
			if finalStatus == queue.StatusSkipped {
				finish = store.Skip
			}
			finished, err := finish(ctx, 1, serving.ID)
			if err != nil {
				t.Fatal(err)
			}
			if finished.Status != finalStatus || finished.OperatorID == nil || *finished.OperatorID != 1 || finished.StartedAt == nil || finished.FinishedAt == nil {
				t.Fatalf("неверная история: %+v", finished)
			}
			if !finished.CreatedAt.Equal(created[0].CreatedAt) || !finished.StartedAt.Equal(*serving.StartedAt) || finished.FinishedAt.Before(*finished.StartedAt) {
				t.Fatalf("потеряны или переставлены временные отметки: %+v", finished)
			}
			operators, err := store.ListOperators(ctx)
			if err != nil || operators[0].CurrentTicketID != nil {
				t.Fatalf("оператор не освободился: %+v, %v", operators, err)
			}
			next, err := store.Next(ctx, 1)
			if err != nil || next.ID != created[1].ID {
				t.Fatalf("повторная выдача: %+v, %v", next, err)
			}
			history, err := store.GetTicket(ctx, finished.ID)
			if err != nil || history.Status != finalStatus || history.OperatorID == nil || *history.OperatorID != 1 || history.FinishedAt == nil {
				t.Fatalf("история не сохранилась: %+v, %v", history, err)
			}
		})
	}
}

func TestRejectedTransitions(t *testing.T) {
	ctx, _, store := setup(t, 3)
	created := createTickets(t, ctx, store, 2)
	for name, finish := range map[string]func(context.Context, int64, int64) (queue.Ticket, error){
		"complete": store.Complete, "skip": store.Skip,
	} {
		t.Run("waiting_"+name, func(t *testing.T) {
			_, err := finish(ctx, 1, created[0].ID)
			requireCode(t, err, "invalid_ticket_state")
		})
	}
	serving, err := store.Next(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Next(ctx, 1)
	busy := requireCode(t, err, "operator_busy")
	if busy.CurrentTicketID != serving.ID {
		t.Fatalf("конфликт ссылается на неверный талон: %+v", busy)
	}
	for _, finish := range []func(context.Context, int64, int64) (queue.Ticket, error){store.Complete, store.Skip} {
		_, err = finish(ctx, 2, serving.ID)
		requireCode(t, err, "ticket_not_owned")
	}
	unchanged, err := store.GetTicket(ctx, serving.ID)
	if err != nil || unchanged.Status != queue.StatusServing || unchanged.OperatorID == nil || *unchanged.OperatorID != 1 {
		t.Fatalf("отклонённая команда изменила талон: %+v, %v", unchanged, err)
	}
	if _, err := store.Complete(ctx, 1, serving.ID); err != nil {
		t.Fatal(err)
	}
	newTicket, err := store.Next(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Повтор несёт id предыдущего талона и не должен завершить новый.
	for _, finish := range []func(context.Context, int64, int64) (queue.Ticket, error){store.Complete, store.Skip} {
		_, err := finish(ctx, 1, serving.ID)
		requireCode(t, err, "invalid_ticket_state")
	}
	stillServing, err := store.GetTicket(ctx, newTicket.ID)
	if err != nil || stillServing.Status != queue.StatusServing || stillServing.FinishedAt != nil {
		t.Fatalf("повтор затронул следующий талон: %+v, %v", stillServing, err)
	}
	if _, err := store.Skip(ctx, 1, newTicket.ID); err != nil {
		t.Fatal(err)
	}
	for _, finish := range []func(context.Context, int64, int64) (queue.Ticket, error){store.Complete, store.Skip} {
		_, err := finish(ctx, 1, newTicket.ID)
		requireCode(t, err, "invalid_ticket_state")
	}
	_, err = store.Next(ctx, 1)
	if !errors.Is(err, queue.ErrEmptyQueue) {
		t.Fatalf("конечные талоны снова попали в очередь: %v", err)
	}
}

func TestNotFound(t *testing.T) {
	ctx, _, store := setup(t, 1)
	_, err := store.GetTicket(ctx, 999)
	requireCode(t, err, "ticket_not_found")
	_, err = store.Next(ctx, 999)
	requireCode(t, err, "operator_not_found")
	for _, finish := range []func(context.Context, int64, int64) (queue.Ticket, error){store.Complete, store.Skip} {
		_, err = finish(ctx, 999, 999)
		requireCode(t, err, "operator_not_found")
		_, err = finish(ctx, 1, 999)
		requireCode(t, err, "ticket_not_found")
	}
}

func TestListsAndFiltering(t *testing.T) {
	ctx, _, store := setup(t, 3)
	created := createTickets(t, ctx, store, 4)
	first, err := store.Next(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(ctx, 1, first.ID); err != nil {
		t.Fatal(err)
	}
	second, err := store.Next(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := store.ListTickets(ctx, queue.StatusWaiting, 50, 0)
	if err != nil || len(waiting) != 2 || waiting[0].ID != created[2].ID || waiting[1].ID != created[3].ID {
		t.Fatalf("неверный фильтр: %+v, %v", waiting, err)
	}
	page, err := store.ListTickets(ctx, "", 2, 1)
	if err != nil || len(page) != 2 || page[0].ID != created[1].ID || page[1].ID != created[2].ID {
		t.Fatalf("неверная страница: %+v, %v", page, err)
	}
	empty, err := store.ListTickets(ctx, queue.StatusSkipped, 50, 0)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("пустой список должен быть []: %+v, %v", empty, err)
	}
	operators, err := store.ListOperators(ctx)
	if err != nil || len(operators) != 3 || operators[0].CurrentTicketID != nil || operators[1].CurrentTicketID == nil || *operators[1].CurrentTicketID != second.ID {
		t.Fatalf("неверный список операторов: %+v, %v", operators, err)
	}
}
