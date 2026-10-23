package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"electronic-queue-api/internal/queue"
	"electronic-queue-api/internal/testutil"
)

type result struct {
	operation string
	status    int
	duration  time.Duration
	ticket    queue.Ticket
	operator  int64
	ticketID  int64
	err       error
}

type report struct {
	Tickets           int            `json:"tickets"`
	Operators         int            `json:"operators"`
	Instances         int            `json:"instances"`
	Mode              string         `json:"mode"`
	Requests          int            `json:"requests"`
	NextRequests      int            `json:"nextRequests"`
	CompleteRequests  int            `json:"completeRequests"`
	Issued            int            `json:"issued"`
	Completed         int            `json:"completed"`
	Empty             int            `json:"empty"`
	Conflicts         int            `json:"conflicts"`
	Errors            int            `json:"errors"`
	Duplicates        int            `json:"duplicates"`
	ElapsedMS         float64        `json:"elapsedMs"`
	RequestsPerSecond float64        `json:"requestsPerSecond"`
	P50MS             float64        `json:"p50Ms"`
	P95MS             float64        `json:"p95Ms"`
	P99MS             float64        `json:"p99Ms"`
	Database          map[string]int `json:"database"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	tickets := flag.Int("tickets", 100, "Количество талонов")
	operators := flag.Int("operators", 100, "Количество конкурентных операторов")
	instances := flag.Int("instances", 2, "Количество отдельных процессов API")
	mode := flag.String("mode", "wave", "wave: одна волна next; drain: next → complete до опустошения")
	deadline := flag.Duration("timeout", 2*time.Minute, "Срок измеряемого эксперимента")
	flag.Parse()
	if *tickets < 1 || *operators < 1 || *instances < 1 || *instances > 4 || (*mode != "wave" && *mode != "drain") || *deadline <= 0 {
		return fmt.Errorf("некорректные параметры эксперимента")
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("TEST_DATABASE_URL обязателен: используйте отдельную БД стенда")
	}
	prepareCtx, cancelPrepare := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelPrepare()
	db, err := testutil.Create(prepareCtx, dsn, *operators)
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		runErr = errors.Join(runErr, db.Close(ctx))
	}()
	binary, cleanup, err := testutil.BuildAPI(prepareCtx, false)
	if err != nil {
		return err
	}
	defer cleanup()
	processes := make([]*testutil.Process, 0, *instances)
	defer func() {
		for _, process := range processes {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			if err := process.Stop(ctx); err != nil {
				process.Kill()
				runErr = errors.Join(runErr, err)
			}
			cancel()
		}
	}()
	for i := 0; i < *instances; i++ {
		process, err := testutil.StartAPI(prepareCtx, binary, db.URL)
		if err != nil {
			return err
		}
		processes = append(processes, process)
	}
	transport := &http.Transport{MaxIdleConns: *operators * 2, MaxIdleConnsPerHost: *operators}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 25 * time.Second}
	// Подготовка выполняется через HTTP, но не входит в измеряемый интервал.
	for i := 0; i < *tickets; i++ {
		r := request(prepareCtx, client, processes[i%len(processes)].URL+"/tickets", "create", 0, 0)
		if r.err != nil || r.status != http.StatusCreated {
			return fmt.Errorf("подготовка талона: status=%d error=%v", r.status, r.err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), *deadline)
	defer cancel()
	results := make(chan result, *operators)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := 0; i < *operators; i++ {
		operator := int64(i + 1)
		url := processes[i%len(processes)].URL
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for {
				next := request(ctx, client, fmt.Sprintf("%s/operators/%d/next", url, operator), "next", operator, 0)
				results <- next
				if next.err != nil || next.status != http.StatusOK || *mode == "wave" {
					return
				}
				finish := request(ctx, client, fmt.Sprintf("%s/operators/%d/complete", url, operator), "complete", operator, next.ticket.ID)
				results <- finish
				if finish.err != nil || finish.status != http.StatusOK {
					return
				}
			}
		}()
	}
	go func() { workers.Wait(); close(results) }()
	summary := report{Tickets: *tickets, Operators: *operators, Instances: *instances, Mode: *mode, Database: make(map[string]int)}
	seenIDs := make(map[int64]bool)
	seenNumbers := make(map[string]bool)
	owners := make(map[int64]int64)
	var latencies []time.Duration
	started := time.Now()
	close(start)
	for r := range results {
		summary.Requests++
		latencies = append(latencies, r.duration)
		if r.operation == "next" {
			summary.NextRequests++
		} else {
			summary.CompleteRequests++
		}
		switch {
		case r.err != nil:
			summary.Errors++
		case r.status == http.StatusNoContent:
			summary.Empty++
		case r.status == http.StatusConflict:
			summary.Conflicts++
		case r.status != http.StatusOK:
			summary.Errors++
		case r.operation == "next":
			summary.Issued++
			if seenIDs[r.ticket.ID] || seenNumbers[r.ticket.Number] {
				summary.Duplicates++
			}
			seenIDs[r.ticket.ID], seenNumbers[r.ticket.Number] = true, true
			owners[r.ticket.ID] = r.operator
			if r.ticket.Status != queue.StatusServing || r.ticket.OperatorID == nil || *r.ticket.OperatorID != r.operator {
				summary.Errors++
			}
		default:
			summary.Completed++
			if r.ticket.Status != queue.StatusCompleted || r.ticket.ID != r.ticketID || r.ticket.OperatorID == nil || *r.ticket.OperatorID != r.operator {
				summary.Errors++
			}
		}
	}
	elapsed := time.Since(started)
	summary.ElapsedMS = float64(elapsed) / float64(time.Millisecond)
	summary.RequestsPerSecond = float64(summary.Requests) / elapsed.Seconds()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	summary.P50MS, summary.P95MS, summary.P99MS = percentile(latencies, 50), percentile(latencies, 95), percentile(latencies, 99)
	// Проверяем не только ответы, но и назначения в собственной схеме БД.
	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelVerify()
	rows, err := db.Pool.Query(verifyCtx, "SELECT id,status,operator_id FROM tickets ORDER BY id")
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		var status string
		var owner *int64
		if err := rows.Scan(&id, &status, &owner); err != nil {
			rows.Close()
			return err
		}
		summary.Database[status]++
		if expected, ok := owners[id]; ok && (owner == nil || *owner != expected) {
			summary.Errors++
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(summary); err != nil {
		return err
	}
	expected := min(*tickets, *operators)
	if *mode == "drain" {
		expected = *tickets
	}
	if summary.Issued != expected || summary.Errors != 0 || summary.Conflicts != 0 || summary.Duplicates != 0 {
		return fmt.Errorf("эксперимент не прошёл проверку; подробности в JSON выше")
	}
	if *mode == "drain" && (summary.Completed != *tickets || summary.Database[queue.StatusCompleted] != *tickets) {
		return fmt.Errorf("не все талоны завершены")
	}
	if *mode == "wave" && summary.Database[queue.StatusServing] != expected {
		return fmt.Errorf("число serving не соответствует выдачам")
	}
	return nil
}

func request(ctx context.Context, client *http.Client, url, operation string, operator, ticketID int64) result {
	r := result{operation: operation, operator: operator, ticketID: ticketID}
	var body io.Reader
	if operation == "complete" {
		body = bytes.NewBufferString(fmt.Sprintf(`{"ticketId":"%d"}`, ticketID))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		r.err = err
		return r
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	started := time.Now()
	response, err := client.Do(req)
	if err != nil {
		r.err = err
		r.duration = time.Since(started)
		return r
	}
	defer response.Body.Close()
	r.status = response.StatusCode
	if response.StatusCode == http.StatusOK || response.StatusCode == http.StatusCreated {
		r.err = json.NewDecoder(response.Body).Decode(&r.ticket)
	} else {
		_, r.err = io.Copy(io.Discard, response.Body)
	}
	r.duration = time.Since(started)
	return r
}

func percentile(sorted []time.Duration, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := (len(sorted)*p+99)/100 - 1
	return float64(sorted[index]) / float64(time.Millisecond)
}
