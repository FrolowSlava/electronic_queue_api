package queue

import "errors"

var ErrEmptyQueue = errors.New("нет доступных талонов")

type Error struct {
	Code            string
	Message         string
	CurrentTicketID int64
}

func (e *Error) Error() string {
	return e.Message
}
