package queue_test

import (
	"reflect"
	"testing"

	"electronic-queue-api/internal/queue"
)

func TestOperatorCreationAndActivity(t *testing.T) {
	ctx, db, store := setup(t, 3)
	operator, err := store.CreateOperator(ctx, "Новый оператор")
	if err != nil || operator.ID != 4 || operator.Name != "Новый оператор" || !operator.Active || operator.CurrentTicketID != nil {
		t.Fatalf("новый оператор: %+v, %v", operator, err)
	}
	for _, name := range []string{"", " \t\n"} {
		_, err := store.CreateOperator(ctx, name)
		requireCode(t, err, "invalid_request")
	}
	_, err = store.SetOperatorActive(ctx, 999, false)
	requireCode(t, err, "operator_not_found")
	// Повторная установка не должна создавать новую версию строки PostgreSQL.
	for _, active := range []bool{true, false, true} {
		changed, err := store.SetOperatorActive(ctx, operator.ID, active)
		if err != nil || changed.Active != active || changed.ID != operator.ID || changed.Name != operator.Name {
			t.Fatalf("изменение активности: %+v, %v", changed, err)
		}
		var before, after string
		if err := db.Pool.QueryRow(ctx, "SELECT xmin::text FROM operators WHERE id=$1", operator.ID).Scan(&before); err != nil {
			t.Fatal(err)
		}
		repeated, err := store.SetOperatorActive(ctx, operator.ID, active)
		if err != nil || !reflect.DeepEqual(changed, repeated) {
			t.Fatalf("повторная установка активности: %+v, %v", repeated, err)
		}
		if err := db.Pool.QueryRow(ctx, "SELECT xmin::text FROM operators WHERE id=$1", operator.ID).Scan(&after); err != nil || before != after {
			t.Fatalf("лишний UPDATE: xmin до=%s после=%s, %v", before, after, err)
		}
	}
	if _, err := store.SetOperatorActive(ctx, operator.ID, false); err != nil {
		t.Fatal(err)
	}
	for _, active := range []bool{true, false} {
		operators, err := store.ListOperatorsFiltered(ctx, &active)
		want := 3
		if !active {
			want = 1
		}
		if err != nil || len(operators) != want {
			t.Fatalf("фильтр active=%t: %+v, %v", active, operators, err)
		}
		for _, got := range operators {
			if got.Active != active {
				t.Fatalf("фильтр вернул чужую активность: %+v", got)
			}
		}
	}
	all, err := store.ListOperators(ctx)
	if err != nil || len(all) != 4 {
		t.Fatalf("список без фильтра: %+v, %v", all, err)
	}
}

func TestOperatorActivityPreservesHistory(t *testing.T) {
	ctx, _, store := setup(t, 3)
	if _, err := store.SetOperatorActive(ctx, 1, false); err != nil {
		t.Fatal(err)
	}
	_, err := store.Next(ctx, 1)
	requireCode(t, err, "operator_inactive") // Запрет действует и при пустой очереди.
	createTickets(t, ctx, store, 2)
	_, err = store.Next(ctx, 1)
	requireCode(t, err, "operator_inactive")
	if _, err := store.SetOperatorActive(ctx, 1, true); err != nil {
		t.Fatal(err)
	}
	var history []queue.Ticket
	for _, finish := range []func() (queue.Ticket, error){
		func() (queue.Ticket, error) { return store.Complete(ctx, 1, 1) },
		func() (queue.Ticket, error) { return store.Skip(ctx, 1, 2) },
	} {
		serving, err := store.Next(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		_, err = store.SetOperatorActive(ctx, 1, false)
		busy := requireCode(t, err, "operator_busy")
		if busy.CurrentTicketID != serving.ID {
			t.Fatalf("ошибка занятости: %+v", busy)
		}
		repeated, err := store.SetOperatorActive(ctx, 1, true)
		if err != nil || !repeated.Active || repeated.CurrentTicketID == nil || *repeated.CurrentTicketID != serving.ID {
			t.Fatalf("повторное включение занятого оператора: %+v, %v", repeated, err)
		}
		finished, err := finish()
		if err != nil {
			t.Fatal(err)
		}
		history = append(history, finished)
	}
	if _, err := store.SetOperatorActive(ctx, 1, false); err != nil {
		t.Fatal(err)
	}
	for _, expected := range history {
		actual, err := store.GetTicket(ctx, expected.ID)
		if err != nil || !reflect.DeepEqual(expected, actual) {
			t.Fatalf("отключение изменило историю: до=%+v после=%+v, %v", expected, actual, err)
		}
	}
	all, err := store.ListOperators(ctx)
	if err != nil || len(all) != 3 || all[0].Active || all[0].ID != 1 {
		t.Fatalf("отключение удалило оператора: %+v, %v", all, err)
	}
}

func TestConcurrentNextAndDisable(t *testing.T) {
	ctx, db, store := setup(t, 3)
	for round := range 20 {
		if _, err := store.SetOperatorActive(ctx, 1, true); err != nil {
			t.Fatal(err)
		}
		created := createTickets(t, ctx, store, 1)[0]
		results := runConcurrent(2, func(i int) concurrentResult {
			if i == 0 {
				_, err := store.SetOperatorActive(ctx, 1, false)
				return concurrentResult{operatorID: 0, err: err}
			}
			ticket, err := store.Next(ctx, 1)
			return concurrentResult{operatorID: 1, ticket: ticket, err: err}
		})
		var disable, next concurrentResult
		for _, result := range results {
			if result.operatorID == 0 {
				disable = result
			} else {
				next = result
			}
		}
		if next.err == nil {
			requireCode(t, disable.err, "operator_busy")
			if next.ticket.ID != created.ID {
				t.Fatalf("выдан чужой талон: %+v", next.ticket)
			}
		} else {
			requireCode(t, next.err, "operator_inactive")
			if disable.err != nil {
				t.Fatalf("не отключён оператор: %v", disable.err)
			}
		}
		var invalid int
		if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM tickets t JOIN operators o ON o.id=t.operator_id
			WHERE t.status='serving' AND NOT o.active`).Scan(&invalid); err != nil || invalid != 0 {
			t.Fatalf("раунд %d: неактивных с serving=%d, %v", round, invalid, err)
		}
		if next.err != nil {
			if _, err := store.SetOperatorActive(ctx, 1, true); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Next(ctx, 1); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := store.Complete(ctx, 1, created.ID); err != nil {
			t.Fatal(err)
		}
	}
}
