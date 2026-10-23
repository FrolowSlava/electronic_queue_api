package queue

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (s *Store) CreateTicket(ctx context.Context) (Ticket, error) {
	ticket, err := scanTicket(s.pool.QueryRow(ctx,
		"INSERT INTO tickets DEFAULT VALUES RETURNING "+ticketColumns))
	if err != nil {
		return Ticket{}, fmt.Errorf("create ticket: %w", err)
	}
	return ticket, nil
}

func (s *Store) GetTicket(ctx context.Context, id int64) (Ticket, error) {
	ticket, err := scanTicket(s.pool.QueryRow(ctx,
		"SELECT "+ticketColumns+" FROM tickets WHERE id = $1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Ticket{}, &Error{Code: "ticket_not_found", Message: "Талон не найден"}
	}
	if err != nil {
		return Ticket{}, fmt.Errorf("get ticket: %w", err)
	}
	return ticket, nil
}

func (s *Store) ListTickets(ctx context.Context, status string, limit, offset int) ([]Ticket, error) {
	query := "SELECT " + ticketColumns + " FROM tickets ORDER BY id LIMIT $1 OFFSET $2"
	args := []any{limit, offset}
	if status != "" {
		query = "SELECT " + ticketColumns + " FROM tickets WHERE status = $1 ORDER BY id LIMIT $2 OFFSET $3"
		args = []any{status, limit, offset}
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list tickets: %w", err)
	}
	defer rows.Close()

	tickets := make([]Ticket, 0)
	for rows.Next() {
		ticket, err := scanTicket(rows)
		if err != nil {
			return nil, fmt.Errorf("scan ticket: %w", err)
		}
		tickets = append(tickets, ticket)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tickets: %w", err)
	}
	return tickets, nil
}
