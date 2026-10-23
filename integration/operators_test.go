package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"electronic-queue-api/internal/queue"
	"electronic-queue-api/internal/testutil"
)

func TestHTTPActiveOperatorCounts(t *testing.T) {
	db := testutil.New(t, 3)
	process := startAPI(t, db)
	client := clientForTest(t)
	for _, name := range []string{"Оператор 4", "Оператор 5"} {
		var created queue.Operator
		checkReply(t, request(client, "POST", process.URL+"/operators", map[string]string{"name": name}), 201, &created)
		if !created.Active || created.Name != name {
			t.Fatalf("неверный новый оператор: %+v", created)
		}
	}
	var history []queue.Ticket
	// Все четыре режима используют одну схему и те же пять записей операторов.
	for _, target := range []int{1, 2, 3, 5} {
		for id := 1; id <= 5; id++ {
			var changed queue.Operator
			checkReply(t, request(client, "PATCH", fmt.Sprintf("%s/operators/%d/active", process.URL, id),
				map[string]bool{"active": id <= target}), 200, &changed)
			if changed.Active != (id <= target) {
				t.Fatalf("неверная активность: %+v", changed)
			}
		}
		for _, filter := range []struct {
			query string
			count int
		}{
			{"", 5}, {"?active=true", target}, {"?active=false", 5 - target},
		} {
			var listed struct{ Items []queue.Operator }
			checkReply(t, request(client, "GET", process.URL+"/operators"+filter.query, nil), 200, &listed)
			if len(listed.Items) != filter.count {
				t.Fatalf("режим %d, фильтр %q: количество=%d, ожидается=%d", target, filter.query, len(listed.Items), filter.count)
			}
			for _, operator := range listed.Items {
				if filter.query != "" && operator.Active != (filter.query == "?active=true") {
					t.Fatalf("фильтр вернул неверного оператора: %+v", operator)
				}
			}
		}
		for range target {
			checkReply(t, request(client, "POST", process.URL+"/tickets", nil), 201, nil)
		}
		for id := 1; id <= 5; id++ {
			address := fmt.Sprintf("%s/operators/%d", process.URL, id)
			if id > target {
				checkOperatorError(t, request(client, "POST", address+"/next", nil), "operator_inactive")
				continue
			}
			var ticket queue.Ticket
			checkReply(t, request(client, "POST", address+"/next", nil), 200, &ticket)
			if ticket.OperatorID == nil || *ticket.OperatorID != int64(id) || ticket.Status != queue.StatusServing {
				t.Fatalf("неверная выдача: %+v", ticket)
			}
			command := "complete"
			if id%2 == 0 {
				command = "skip"
			}
			checkReply(t, request(client, "POST", address+"/"+command,
				map[string]string{"ticketId": strconv.FormatInt(ticket.ID, 10)}), 200, &ticket)
			history = append(history, ticket)
		}
		for _, original := range history {
			var actual queue.Ticket
			checkReply(t, request(client, "GET", fmt.Sprintf("%s/tickets/%d", process.URL, original.ID), nil), 200, &actual)
			if !reflect.DeepEqual(original, actual) {
				t.Fatalf("смена числа активных операторов изменила историю: до=%+v после=%+v", original, actual)
			}
		}
		t.Logf("Реальные HTTP-запросы: активных=%d, всего записей=5, сохранено исторических талонов=%d, схема=%s", target, len(history), db.Schema)
	}
}

func checkOperatorError(t *testing.T, result reply, code string) {
	t.Helper()
	var body struct {
		Error struct{ Code string } `json:"error"`
	}
	checkReply(t, result, http.StatusConflict, &body)
	if body.Error.Code != code {
		t.Fatalf("ожидался конфликт %s: %s", code, result.body)
	}
}

func verifyActivityRace(t *testing.T, db *testutil.Database, disable, next reply, ticketID int64) bool {
	t.Helper()
	if disable.err != nil || next.err != nil {
		t.Fatalf("ошибка HTTP: отключение=%v выдача=%v", disable.err, next.err)
	}
	disabled := disable.status == http.StatusOK
	var operator queue.Operator
	var ticket queue.Ticket
	if disabled {
		checkReply(t, disable, http.StatusOK, &operator)
		checkOperatorError(t, next, "operator_inactive")
		if operator.Active || operator.CurrentTicketID != nil {
			t.Fatalf("отключённый оператор: %+v", operator)
		}
	} else {
		checkReply(t, next, http.StatusOK, &ticket)
		checkOperatorError(t, disable, "operator_busy")
		if ticket.ID != ticketID || ticket.OperatorID == nil || *ticket.OperatorID != 1 {
			t.Fatalf("выдан неверный талон: %+v", ticket)
		}
	}
	var active bool
	var status string
	if err := db.Pool.QueryRow(context.Background(), "SELECT active FROM operators WHERE id=1").Scan(&active); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(context.Background(), "SELECT status FROM tickets WHERE id=$1", ticketID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	wantStatus := queue.StatusServing
	if disabled {
		wantStatus = queue.StatusWaiting
	}
	if active == disabled || status != wantStatus {
		t.Fatalf("несогласованное состояние БД: active=%t status=%s, отключение=%s выдача=%s", active, status, disable.body, next.body)
	}
	return disabled
}

func completeActivityRace(t *testing.T, client *http.Client, process *testutil.Process, ticketID int64, disabled bool) {
	t.Helper()
	if disabled {
		checkReply(t, request(client, "PATCH", process.URL+"/operators/1/active", map[string]bool{"active": true}), 200, nil)
		checkReply(t, request(client, "POST", process.URL+"/operators/1/next", nil), 200, nil)
	}
	checkReply(t, request(client, "POST", process.URL+"/operators/1/complete",
		map[string]string{"ticketId": strconv.FormatInt(ticketID, 10)}), 200, nil)
}

func TestTwoProcessesActivityLockOrders(t *testing.T) {
	db := testutil.New(t, 3)
	processes := []*testutil.Process{startAPI(t, db), startAPI(t, db)}
	client := clientForTest(t)
	for _, disableFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("disable_first_%t", disableFirst), func(t *testing.T) {
			var ticket queue.Ticket
			checkReply(t, request(client, "POST", processes[0].URL+"/tickets", nil), 201, &ticket)
			lock := lockOperator(t, db)
			first, second := make(chan reply, 1), make(chan reply, 1)
			operation := func(process *testutil.Process, disable bool) reply {
				if disable {
					return request(client, "PATCH", process.URL+"/operators/1/active", map[string]bool{"active": false})
				}
				return request(client, "POST", process.URL+"/operators/1/next", nil)
			}
			// Очередь ожидания блокировки формируется явно, без предположений о sleep.
			go func() { first <- operation(processes[0], disableFirst) }()
			waitForOperatorWait(t, db, processes[0])
			go func() { second <- operation(processes[1], !disableFirst) }()
			waitForOperatorWait(t, db, processes[1])
			if err := lock.Rollback(context.Background()); err != nil {
				t.Fatal(err)
			}
			disable, next := <-first, <-second
			if !disableFirst {
				disable, next = next, disable
			}
			disabled := verifyActivityRace(t, db, disable, next, ticket.ID)
			if disabled != disableFirst {
				t.Fatalf("нарушен порядок ожидания блокировки: disableFirst=%t disabled=%t", disableFirst, disabled)
			}
			completeActivityRace(t, client, processes[0], ticket.ID, disabled)
		})
	}
}

func TestTwoProcessesConcurrentActivityAndNext(t *testing.T) {
	db := testutil.New(t, 3)
	processes := []*testutil.Process{startAPI(t, db), startAPI(t, db)}
	client := clientForTest(t)
	counts := map[bool]int{}
	for round := range 20 {
		var ticket queue.Ticket
		checkReply(t, request(client, "POST", processes[0].URL+"/tickets", nil), 201, &ticket)
		start := make(chan struct{})
		var ready sync.WaitGroup
		ready.Add(2)
		results := make([]reply, 2)
		var done sync.WaitGroup
		done.Add(2)
		for i := range 2 {
			go func(i int) {
				defer done.Done()
				ready.Done()
				<-start
				if i == 0 {
					results[i] = request(client, "PATCH", processes[round%2].URL+"/operators/1/active", map[string]bool{"active": false})
				} else {
					results[i] = request(client, "POST", processes[(round+1)%2].URL+"/operators/1/next", nil)
				}
			}(i)
		}
		ready.Wait()
		close(start)
		done.Wait()
		disabled := verifyActivityRace(t, db, results[0], results[1], ticket.ID)
		counts[disabled]++
		completeActivityRace(t, client, processes[0], ticket.ID, disabled)
	}
	t.Logf("Два процесса, 20 одновременных пар PATCH/next: отключение победило=%d, выдача победила=%d", counts[true], counts[false])
}
