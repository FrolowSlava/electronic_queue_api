package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Database принадлежит одной группе тестов или одному нагрузочному прогону.
type Database struct {
	Pool        *pgxpool.Pool
	Config      *pgxpool.Config
	URL         string
	Schema      string
	admin       *pgxpool.Pool
	ownedSchema string
}

// Create создаёт собственную схему, не изменяя данные общей схемы public.
func Create(ctx context.Context, baseURL string, operatorCount int) (db *Database, err error) {
	if operatorCount < 1 {
		return nil, fmt.Errorf("число операторов должно быть положительным")
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil || (parsedURL.Scheme != "postgres" && parsedURL.Scheme != "postgresql") {
		return nil, fmt.Errorf("требуется PostgreSQL URL подключения")
	}
	baseConfig, err := pgxpool.ParseConfig(baseURL)
	if err != nil {
		return nil, fmt.Errorf("некорректный PostgreSQL URL подключения")
	}
	var suffix [12]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return nil, fmt.Errorf("сгенерировать имя тестовой схемы: %w", err)
	}
	schema := "eqtest_" + hex.EncodeToString(suffix[:])
	db = &Database{Schema: schema}
	ownedDB := db
	defer func() {
		if err != nil {
			// Исходный context уже мог истечь; очистка получает собственный срок.
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if cleanupErr := ownedDB.Close(cleanupCtx); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("очистить тестовую схему: %w", cleanupErr))
			}
		}
	}()

	db.admin, err = pgxpool.NewWithConfig(ctx, baseConfig.Copy())
	if err != nil {
		return nil, fmt.Errorf("создать административный пул: %w", err)
	}
	schemaIdentifier := pgx.Identifier{db.Schema}.Sanitize()
	if _, err = db.admin.Exec(ctx, "CREATE SCHEMA "+schemaIdentifier); err != nil {
		return nil, fmt.Errorf("создать тестовую схему: %w", err)
	}
	// Владение начинается только после успешного CREATE: при ошибке создания
	// нельзя удалять даже случайно совпавшую по имени существующую схему.
	db.ownedSchema = schema

	// Параметр передаётся при открытии КАЖДОГО соединения пула. Разовый SET
	// через pool.Exec изменил бы только одно произвольно выбранное соединение.
	db.Config = baseConfig.Copy()
	db.Config.ConnConfig.RuntimeParams["search_path"] = schemaIdentifier
	db.Config.ConnConfig.RuntimeParams["timezone"] = "UTC"
	db.Pool, err = pgxpool.NewWithConfig(ctx, db.Config.Copy())
	if err != nil {
		return nil, fmt.Errorf("создать пул тестовой схемы: %w", err)
	}
	query := parsedURL.Query()
	query.Set("search_path", schemaIdentifier)
	query.Set("timezone", "UTC")
	parsedURL.RawQuery = query.Encode()
	db.URL = parsedURL.String()

	if err = applyMigrations(ctx, db.Pool); err != nil {
		return nil, err
	}
	// Миграции создают первых трёх операторов. Малые тесты используют только
	// нужных операторов, большие добавляют остальных в собственной схеме.
	if operatorCount > 3 {
		_, err = db.Pool.Exec(ctx,
			"INSERT INTO operators (name) SELECT 'Оператор ' || n FROM generate_series(4, $1::integer) AS n",
			operatorCount)
		if err != nil {
			return nil, fmt.Errorf("создать тестовых операторов: %w", err)
		}
	}
	return db, nil
}

func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("не удалось определить каталог миграций")
	}
	paths, err := filepath.Glob(filepath.Join(filepath.Dir(sourceFile), "..", "..", "migrations", "*.up.sql"))
	if err != nil || len(paths) == 0 {
		return fmt.Errorf("не найдены SQL-миграции рядом с исходным кодом")
	}
	sort.Strings(paths)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("получить соединение для миграций: %w", err)
	}
	defer conn.Release()
	for _, path := range paths {
		sql, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("прочитать миграцию %s: %w", filepath.Base(path), err)
		}
		// Без аргументов pgx использует simple protocol и выполняет SQL-файл
		// целиком, включая BEGIN/COMMIT. Соединение уже привязано к схеме.
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("применить миграцию %s: %w", filepath.Base(path), err)
		}
	}
	return nil
}

// Close вызывается после остановки использующих схему HTTP-серверов и пулов.
func (db *Database) Close(ctx context.Context) error {
	if db == nil || db.admin == nil {
		return nil
	}
	if db.Pool != nil {
		db.Pool.Close()
	}
	defer func() {
		db.admin.Close()
		db.admin = nil
	}()
	// Удаляем только имя, запомненное при Create, даже если вызывающий код
	// случайно изменил экспортируемое поле Schema.
	if db.ownedSchema == "" {
		return nil
	}
	_, err := db.admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{db.ownedSchema}.Sanitize()+" CASCADE")
	if err != nil {
		return fmt.Errorf("удалить тестовую схему: %w", err)
	}
	return nil
}

func New(t testing.TB, operators int) *Database {
	t.Helper()
	baseURL := os.Getenv("TEST_DATABASE_URL")
	if baseURL == "" {
		t.Fatal("TEST_DATABASE_URL обязателен: запустите тестовую PostgreSQL по инструкции README")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := Create(ctx, baseURL, operators)
	if err != nil {
		t.Fatalf("подготовить тестовую БД: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := db.Close(cleanupCtx); err != nil {
			t.Errorf("очистить тестовую БД: %v", err)
		}
	})
	return db
}
