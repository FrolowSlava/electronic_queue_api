package queue

import "time"

const (
	StatusWaiting   = "waiting"
	StatusServing   = "serving"
	StatusCompleted = "completed"
	StatusSkipped   = "skipped"
)

type Ticket struct {
	ID         int64      `json:"id,string"`
	Number     string     `json:"number"`
	Status     string     `json:"status"`
	OperatorID *int64     `json:"operatorId,string"`
	CreatedAt  time.Time  `json:"createdAt"`
	StartedAt  *time.Time `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt"`
}

type Operator struct {
	ID              int64  `json:"id,string"`
	Name            string `json:"name"`
	Active          bool   `json:"active"`
	CurrentTicketID *int64 `json:"currentTicketId,string"`
}

func ValidStatus(status string) bool {
	switch status {
	case StatusWaiting, StatusServing, StatusCompleted, StatusSkipped:
		return true
	default:
		return false
	}
}
