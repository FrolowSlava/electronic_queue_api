package queue

import "errors"

var ErrEmptyQueue = errors.New("нет доступных талонов")

// ErrCommitOutcomeUnknown требует проверки состояния через GET перед повтором.
var ErrCommitOutcomeUnknown = errors.New("результат COMMIT не подтверждён")

type Error struct {
	Code            string
	Message         string
	CurrentTicketID int64
}

func (e *Error) Error() string {
	return e.Message
}
