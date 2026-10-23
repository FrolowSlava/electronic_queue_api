package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	HTTPAddr         string
	DatabaseURL      string
	DocsDir          string
	DBMaxConns       int32
	RequestTimeout   time.Duration
	ShutdownTimeout  time.Duration
	StatementTimeout time.Duration
	LockTimeout      time.Duration
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr: env("HTTP_ADDR", ":8082"), DatabaseURL: os.Getenv("DATABASE_URL"),
		DocsDir: env("DOCS_DIR", "."),
	}
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("DATABASE_URL обязателен")
	}
	n, err := strconv.ParseInt(env("DB_MAX_CONNS", "10"), 10, 32)
	if err != nil || n < 1 {
		return c, fmt.Errorf("DB_MAX_CONNS должен быть положительным целым числом")
	}
	c.DBMaxConns = int32(n)
	for _, setting := range []struct {
		name     string
		fallback string
		target   *time.Duration
	}{
		{"REQUEST_TIMEOUT", "5s", &c.RequestTimeout},
		{"SHUTDOWN_TIMEOUT", "15s", &c.ShutdownTimeout},
		{"DB_STATEMENT_TIMEOUT", "3s", &c.StatementTimeout},
		{"DB_LOCK_TIMEOUT", "2s", &c.LockTimeout},
	} {
		d, err := time.ParseDuration(env(setting.name, setting.fallback))
		if err != nil || d < time.Millisecond {
			return c, fmt.Errorf("%s должен быть длительностью не меньше 1ms", setting.name)
		}
		*setting.target = d
	}
	return c, nil
}

func (c Config) PoolConfig() (*pgxpool.Config, error) {
	pc, err := pgxpool.ParseConfig(c.DatabaseURL)
	if err != nil {
		// Строка подключения может содержать пароль: не включаем её в сообщение.
		return nil, fmt.Errorf("некорректный DATABASE_URL")
	}
	pc.MaxConns = c.DBMaxConns
	pc.ConnConfig.ConnectTimeout = c.RequestTimeout
	pc.ConnConfig.RuntimeParams["timezone"] = "UTC"
	pc.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(c.StatementTimeout.Milliseconds(), 10)
	pc.ConnConfig.RuntimeParams["lock_timeout"] = strconv.FormatInt(c.LockTimeout.Milliseconds(), 10)
	pc.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "10000"
	return pc, nil
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
