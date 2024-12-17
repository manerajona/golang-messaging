package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"golang-messaging/pubsub-poc/config"
	"golang-messaging/pubsub-poc/model"
	"golang-messaging/pubsub-poc/service"
	"log"
	"net/http"
	"os"
)

func main() {

	// Set PUBSUB_EMULATOR_HOST environment variable
	if err := os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085"); err != nil {
		log.Panicf("Error setting PUBSUB_EMULATOR_HOST: %v", err)
	}

	conn, err := config.NewConnection("transfers-non-cde")
	if err != nil {
		log.Panicf("Error connecting to PubSub Emulator: %v", err)
	}

	// Find or create the topic
	topic, err := conn.FindTopic("transfers")

	// Find or create a subscription
	filter := fmt.Sprintf(`attributes.status = "%s"`, model.TransferStarted)
	subscription, err := conn.Subscribe(topic, filter)

	// Use a callback to receive messages
	go func() {
		err := subscription.Receive(context.TODO(), func(ctx context.Context, message *pubsub.Message) {
			var transfer model.Transfer
			if err := json.Unmarshal(message.Data, &transfer); err != nil {
				log.Fatalf("Error unmarshaling message: %v", err)
			}

			// Apply business logic
			service.Process(transfer)

			// Acknowledge that we've consumed the message
			message.Ack()
		})
		if err != nil {
			log.Panicf("Failed to receive messages: %v", err)
		}
	}()

	// Define endpoint
	router := gin.Default()
	router.POST("/transfers", func(c *gin.Context) {
		var transfer model.Transfer
		if err := c.ShouldBindJSON(&transfer); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Set ID and the status to Started
		transfer.Guid = uuid.New().String()
		transfer.Status = model.TransferStarted

		// Marshal transfer to JSON
		body, err := json.Marshal(transfer)
		if err != nil {
			log.Printf("Failed to marshal transfer to JSON: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Publish message on topic
		res := topic.Publish(context.TODO(), &pubsub.Message{
			Data:       body,
			Attributes: map[string]string{"status": transfer.Status},
		})
		msgID, err := res.Get(context.TODO())
		if err != nil {
			log.Printf("Error publishing message: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		log.Printf("Published message ID=%s, transfer.Guid=%s", msgID, transfer.Guid)
		c.JSON(http.StatusAccepted, transfer)
	})

	// Start server
	if err = router.Run(":8080"); err != nil {
		log.Panicf("Error starting the server: %v", err)
	}
}
