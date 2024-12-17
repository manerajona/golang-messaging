package service

import (
	"golang-messaging/pubsub-poc/model"
	"log"
)

func Process(transfer model.Transfer) {
	// Set the status to Pending
	transfer.Status = model.TransferPending

	// For now, we just log the consumed message.
	log.Printf("Consumed Transfer: GUID=%s Amount=%.2f Sender=%s Recipient=%s Status=%s Time=%v",
		transfer.Guid, transfer.Amount, transfer.Sender, transfer.Recipient, transfer.Status, transfer.TransferDateTime)
}
