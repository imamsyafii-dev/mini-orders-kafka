package event

import "time"

type OrderCreated struct {
	EventID    string           `json:"event_id"`
	EventType  string           `json:"event_type"`
	OccurredAt time.Time        `json:"occurred_at"`
	Payload    OrderCreatedData `json:"payload"`
}

type OrderCreatedData struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	Amount     int64  `json:"amount"`
	Sequence   int    `json:"sequence"`
}
