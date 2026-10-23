package queue

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (s *Store) ListOperators(ctx context.Context) ([]Operator, error) {
	return s.ListOperatorsFiltered(ctx, nil)
}

func (s *Store) ListOperatorsFiltered(ctx context.Context, active *bool) ([]Operator, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT o.id, o.name, o.active, t.id
		FROM operators AS o
		LEFT JOIN tickets AS t ON t.operator_id = o.id AND t.status = 'serving'
		WHERE $1::boolean IS NULL OR o.active = $1
		ORDER BY o.id`, active)
	if err != nil {
		return nil, fmt.Errorf("list operators: %w", err)
	}
	defer rows.Close()

	operators := make([]Operator, 0)
	for rows.Next() {
		var operator Operator
		if err := rows.Scan(&operator.ID, &operator.Name, &operator.Active, &operator.CurrentTicketID); err != nil {
			return nil, fmt.Errorf("scan operator: %w", err)
		}
		operators = append(operators, operator)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operators: %w", err)
	}
	return operators, nil
}

func (s *Store) CreateOperator(ctx context.Context, name string) (Operator, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Operator{}, &Error{Code: "invalid_request", Message: "Имя оператора не должно быть пустым"}
	}
	var operator Operator
	err := s.pool.QueryRow(ctx, `
		INSERT INTO operators (name) VALUES ($1)
		RETURNING id, name, active`, name).Scan(&operator.ID, &operator.Name, &operator.Active)
	if err != nil {
		return Operator{}, fmt.Errorf("create operator: %w", err)
	}
	return operator, nil
}

func (s *Store) SetOperatorActive(ctx context.Context, operatorID int64, active bool) (Operator, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Operator{}, fmt.Errorf("begin operator activation: %w", err)
	}
	defer rollback(tx)
	if _, err := lockOperator(ctx, tx, operatorID); err != nil {
		return Operator{}, err
	}

	// После ожидания блокировки отдельный запрос видит талон, который
	// мог назначить предыдущий /next. Проверка и изменение неразделимы.
	var operator Operator
	err = tx.QueryRow(ctx, `
		SELECT o.id, o.name, o.active, t.id
		FROM operators AS o
		LEFT JOIN tickets AS t ON t.operator_id = o.id AND t.status = 'serving'
		WHERE o.id = $1`, operatorID).Scan(
		&operator.ID, &operator.Name, &operator.Active, &operator.CurrentTicketID)
	if err != nil {
		return Operator{}, fmt.Errorf("check operator activation: %w", err)
	}
	if !active && operator.CurrentTicketID != nil {
		return Operator{}, &Error{
			Code: "operator_busy", Message: "Нельзя отключить оператора, который обслуживает талон",
			CurrentTicketID: *operator.CurrentTicketID,
		}
	}
	// Повторная установка того же значения не создаёт новую версию строки.
	if operator.Active != active {
		if _, err := tx.Exec(ctx, "UPDATE operators SET active = $2 WHERE id = $1", operatorID, active); err != nil {
			return Operator{}, fmt.Errorf("set operator activation: %w", err)
		}
		operator.Active = active
	}
	if err := tx.Commit(ctx); err != nil {
		return Operator{}, fmt.Errorf("commit operator activation: %w", err)
	}
	return operator, nil
}
