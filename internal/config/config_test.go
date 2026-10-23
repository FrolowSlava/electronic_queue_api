package config

import (
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	for _, name := range []string{"HTTP_ADDR", "DB_MAX_CONNS", "REQUEST_TIMEOUT", "SHUTDOWN_TIMEOUT", "DB_STATEMENT_TIMEOUT", "DB_LOCK_TIMEOUT", "DOCS_DIR"} {
		t.Setenv(name, "")
	}
	t.Setenv("DATABASE_URL", "postgres://queue:secret@localhost:5434/test?sslmode=disable&search_path=test_group")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8082" || c.RequestTimeout != 5*time.Second || c.DocsDir != "." {
		t.Fatalf("неверные defaults: %+v", c)
	}
	pc, err := c.PoolConfig()
	if err != nil {
		t.Fatal(err)
	}
	if pc.ConnConfig.RuntimeParams["search_path"] != "test_group" {
		t.Fatal("потерян search_path из DSN")
	}
	for _, tc := range []struct{ name, value string }{
		{"DB_MAX_CONNS", "0"}, {"DB_MAX_CONNS", "bad"}, {"REQUEST_TIMEOUT", "-1s"},
		{"DB_LOCK_TIMEOUT", "0"}, {"DATABASE_URL", ""},
	} {
		t.Run(tc.name+tc.value, func(t *testing.T) {
			t.Setenv(tc.name, tc.value)
			if _, err := Load(); err == nil {
				t.Fatal("ожидалась ошибка конфигурации")
			}
		})
	}
}
