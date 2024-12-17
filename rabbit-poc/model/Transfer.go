package model

import "time"

type Transfer struct {
	Guid             string    `json:"guid"`
	Status           string    `json:"status"`
	Amount           float64   `json:"amount"`
	Sender           string    `json:"sender"`
	Recipient        string    `json:"recipient"`
	TransferDateTime time.Time `json:"transferDateTime"`
}
