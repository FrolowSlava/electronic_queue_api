package queue

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (s *Store) Next(ctx context.Context, operatorID int64) (Ticket, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Ticket{}, fmt.Errorf("begin next: %w", err)
	}
	defer rollback(tx)

	// Все команды оператора берут блокировки в одном порядке:
	// сначала оператор, затем талон. Это действует между всеми экземплярами API.
	active, err := lockOperator(ctx, tx, operatorID)
	if err != nil {
		return Ticket{}, err
	}
	if !active {
		return Ticket{}, &Error{Code: "operator_inactive", Message: "Оператор отключён"}
	}

	// Это отдельный SQL-запрос: после ожидания блокировки READ COMMITTED
	// должен увидеть назначение, зафиксированное предыдущим запросом оператора.
	var currentID int64
	err = tx.QueryRow(ctx,
		"SELECT id FROM tickets WHERE operator_id = $1 AND status = 'serving'", operatorID).Scan(&currentID)
	if err == nil {
		return Ticket{}, &Error{
			Code: "operator_busy", Message: "Оператор уже обслуживает талон", CurrentTicketID: currentID,
		}
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Ticket{}, fmt.Errorf("check current ticket: %w", err)
	}

	// SKIP LOCKED выбирает первый доступный талон, не ожидая строки,
	// захваченные другими операторами. Блокировка живёт до Commit/Rollback.
	var ticketID int64
	err = tx.QueryRow(ctx, `
		SELECT id FROM tickets
		WHERE status = 'waiting'
		ORDER BY id LIMIT 1
		FOR UPDATE SKIP LOCKED`).Scan(&ticketID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Ticket{}, ErrEmptyQueue
	}
	if err != nil {
		return Ticket{}, fmt.Errorf("select next ticket: %w", err)
	}

	ticket, err := scanTicket(tx.QueryRow(ctx, `
		UPDATE tickets SET status = 'serving', operator_id = $1, started_at = clock_timestamp()
		WHERE id = $2 AND status = 'waiting'
		RETURNING `+ticketColumns, operatorID, ticketID))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "tickets_one_serving_per_operator" {
			return Ticket{}, &Error{Code: "operator_busy", Message: "Оператор уже обслуживает талон"}
		}
		return Ticket{}, fmt.Errorf("assign ticket: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Ticket{}, fmt.Errorf("commit next: %w", err)
	}
	// Успешная выдача возвращается только после фиксации назначения в БД.
	return ticket, nil
}

func (s *Store) Complete(ctx context.Context, operatorID, ticketID int64) (Ticket, error) {
	return s.finish(ctx, operatorID, ticketID, StatusCompleted)
}

func (s *Store) Skip(ctx context.Context, operatorID, ticketID int64) (Ticket, error) {
	return s.finish(ctx, operatorID, ticketID, StatusSkipped)
}

func (s *Store) finish(ctx context.Context, operatorID, ticketID int64, status string) (Ticket, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Ticket{}, fmt.Errorf("begin finish: %w", err)
	}
	defer rollback(tx)
	if _, err := lockOperator(ctx, tx, operatorID); err != nil {
		return Ticket{}, err
	}

	// Явный ticketID не позволяет запоздалой повторной команде
	// завершить следующий талон, который оператор уже успел получить.
	ticket, err := scanTicket(tx.QueryRow(ctx,
		"SELECT "+ticketColumns+" FROM tickets WHERE id = $1 FOR UPDATE", ticketID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Ticket{}, &Error{Code: "ticket_not_found", Message: "Талон не найден"}
	}
	if err != nil {
		return Ticket{}, fmt.Errorf("lock ticket: %w", err)
	}
	if ticket.Status != StatusServing {
		return Ticket{}, &Error{Code: "invalid_ticket_state", Message: "Талон не находится в обслуживании"}
	}
	if ticket.OperatorID == nil || *ticket.OperatorID != operatorID {
		return Ticket{}, &Error{Code: "ticket_not_owned", Message: "Талон обслуживается другим оператором"}
	}

	ticket, err = scanTicket(tx.QueryRow(ctx, `
		UPDATE tickets SET status = $1, finished_at = clock_timestamp()
		WHERE id = $2 AND operator_id = $3 AND status = 'serving'
		RETURNING `+ticketColumns, status, ticketID, operatorID))
	if err != nil {
		return Ticket{}, fmt.Errorf("finish ticket: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Ticket{}, fmt.Errorf("commit finish: %w", err)
	}
	return ticket, nil
}

func lockOperator(ctx context.Context, tx pgx.Tx, operatorID int64) (bool, error) {
	var active bool
	err := tx.QueryRow(ctx, "SELECT active FROM operators WHERE id = $1 FOR UPDATE", operatorID).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, &Error{Code: "operator_not_found", Message: "Оператор не найден"}
	}
	if err != nil {
		return false, fmt.Errorf("lock operator: %w", err)
	}
	return active, nil
}
