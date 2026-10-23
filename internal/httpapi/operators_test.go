package httpapi_test

import (
	"strings"
	"testing"
	"time"

	"electronic-queue-api/internal/queue"
)

func TestHTTPOperatorActivity(t *testing.T) {
	_, server := testServer(t, 5*time.Second)
	created := request(t, server, "POST", "/operators", `{"name":"Новый оператор"}`, "application/json", 201)
	operator := decode[queue.Operator](t, created)
	if operator.ID != 4 || operator.Name != "Новый оператор" || !operator.Active || operator.CurrentTicketID != nil {
		t.Fatalf("новый оператор: %s", created.body)
	}
	for _, active := range []string{"false", "false", "true", "true", "false"} {
		got := decode[queue.Operator](t, request(t, server, "PATCH", "/operators/4/active", `{"active":`+active+`}`, "application/json", 200))
		if got.ID != 4 || got.Active != (active == "true") {
			t.Fatalf("неверная активность: %+v", got)
		}
	}
	for _, tt := range []struct {
		path string
		want int
	}{
		{"/operators", 4}, {"/operators?active=true", 3}, {"/operators?active=false", 1},
	} {
		got := decode[struct{ Items []queue.Operator }](t, request(t, server, "GET", tt.path, "", "", 200))
		if len(got.Items) != tt.want {
			t.Fatalf("%s: %+v", tt.path, got)
		}
		for _, item := range got.Items {
			if strings.Contains(tt.path, "active=") && item.Active != strings.HasSuffix(tt.path, "true") {
				t.Fatalf("фильтр активности нарушен: %+v", item)
			}
		}
	}
	errorCode(t, request(t, server, "POST", "/operators/4/next", "", "", 409), "operator_inactive")
	request(t, server, "POST", "/tickets", "", "", 201)
	errorCode(t, request(t, server, "POST", "/operators/4/next", "", "", 409), "operator_inactive")
	request(t, server, "PATCH", "/operators/4/active", `{"active":true}`, "application/json", 200)
	request(t, server, "POST", "/operators/4/next", "", "", 200)
	details := errorCode(t, request(t, server, "PATCH", "/operators/4/active", `{"active":false}`, "application/json", 409), "operator_busy")
	if details["currentTicketId"] != "1" {
		t.Fatalf("нет текущего талона в конфликте: %+v", details)
	}
	request(t, server, "POST", "/operators/4/complete", `{"ticketId":"1"}`, "application/json", 200)
	request(t, server, "PATCH", "/operators/4/active", `{"active":false}`, "application/json", 200)
	ticket(t, request(t, server, "GET", "/tickets/1", "", "", 200), queue.StatusCompleted)
}

func TestHTTPOperatorValidation(t *testing.T) {
	_, server := testServer(t, 5*time.Second)
	for _, tt := range []struct {
		method, path, body string
	}{
		{"POST", "/operators", `{}`},
		{"POST", "/operators", `{"name":""}`},
		{"POST", "/operators", `{"name":"  "}`},
		{"POST", "/operators", `{"name":null}`},
		{"POST", "/operators", `{"name":17}`},
		{"POST", "/operators", `{"name":"Оператор","active":false}`},
		{"PATCH", "/operators/1/active", `{}`},
		{"PATCH", "/operators/1/active", `{"active":null}`},
		{"PATCH", "/operators/1/active", `{"active":"false"}`},
		{"PATCH", "/operators/1/active", `{"active":0}`},
		{"PATCH", "/operators/1/active", `{"active":false,"unknown":1}`},
		{"PATCH", "/operators/1/active", `{"active":false}{}`},
		{"PATCH", "/operators/1/active", `null`},
		{"PATCH", "/operators/1/active", `[]`},
		{"PATCH", "/operators/0/active", `{"active":false}`},
		{"GET", "/operators?active=", ""},
		{"GET", "/operators?active=maybe", ""},
		{"GET", "/operators?active=true&active=false", ""},
		{"GET", "/operators?active=true;other=1", ""},
	} {
		contentType := "application/json"
		if tt.method == "GET" {
			contentType = ""
		}
		errorCode(t, request(t, server, tt.method, tt.path, tt.body, contentType, 400), "invalid_request")
	}
	errorCode(t, request(t, server, "PATCH", "/operators/999/active", `{"active":false}`, "application/json", 404), "operator_not_found")
	errorCode(t, request(t, server, "POST", "/operators", `{"name":"Оператор"}`, "text/plain", 415), "unsupported_media_type")
	errorCode(t, request(t, server, "PATCH", "/operators/1/active", `{"active":false}`, "", 415), "unsupported_media_type")
	all := decode[struct{ Items []queue.Operator }](t, request(t, server, "GET", "/operators", "", "", 200))
	if len(all.Items) != 3 {
		t.Fatalf("невалидные запросы создали операторов: %+v", all)
	}
	for _, operator := range all.Items {
		if !operator.Active {
			t.Fatalf("невалидные запросы изменили оператора: %+v", operator)
		}
	}
}

func TestHTTPBusinessDocumentationOnly(t *testing.T) {
	_, server := testServer(t, 5*time.Second)
	spec := string(request(t, server, "GET", "/openapi.yaml", "", "", 200).body)
	_, tags, found := strings.Cut(spec, "\ntags:\n")
	tags, _, ended := strings.Cut(tags, "\npaths:\n")
	if !found || !ended || strings.Count(tags, "- name:") != 2 {
		t.Fatalf("Swagger должен содержать ровно два основных раздела: %s", tags)
	}
	for _, forbidden := range []string{"Состояние сервиса", "\n  /healthz:", "\n  /openapi.yaml:", "\n  /docs/:"} {
		if strings.Contains(spec, forbidden) {
			t.Fatalf("служебная операция осталась в Swagger: %s", forbidden)
		}
	}
	for _, required := range []string{"Талоны", "Операторы", "/operators/{id}/active:", "operator_inactive"} {
		if !strings.Contains(spec, required) {
			t.Fatalf("в OpenAPI отсутствует %s", required)
		}
	}
	docs := string(request(t, server, "GET", "/docs/", "", "", 200).body)
	if !strings.Contains(docs, `url: "/openapi.yaml"`) {
		t.Fatal("Swagger не загружает спецификацию")
	}
	health := decode[map[string]string](t, request(t, server, "GET", "/healthz", "", "", 200))
	if health["status"] != "ok" {
		t.Fatalf("нарушен healthcheck: %+v", health)
	}
}
