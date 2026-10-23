package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

const ticketColumns = "id, status, operator_id, created_at, started_at, finished_at"

func scanTicket(row pgx.Row) (Ticket, error) {
	var ticket Ticket
	err := row.Scan(&ticket.ID, &ticket.Status, &ticket.OperatorID,
		&ticket.CreatedAt, &ticket.StartedAt, &ticket.FinishedAt)
	if err != nil {
		return Ticket{}, err
	}
	ticket.Number = fmt.Sprintf("A%03d", ticket.ID)
	ticket.CreatedAt = ticket.CreatedAt.UTC()
	if ticket.StartedAt != nil {
		utc := ticket.StartedAt.UTC()
		ticket.StartedAt = &utc
	}
	if ticket.FinishedAt != nil {
		utc := ticket.FinishedAt.UTC()
		ticket.FinishedAt = &utc
	}
	return ticket, nil
}

func rollback(tx pgx.Tx) {
	// Отмена HTTP-запроса не должна отменять очистку транзакции.
	// После успешного Commit Rollback просто вернёт pgx.ErrTxClosed.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
