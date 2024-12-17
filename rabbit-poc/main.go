package main

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"golang-messaging/rabbit-poc/config"
	"golang-messaging/rabbit-poc/model"
	"golang-messaging/rabbit-poc/service"
	"log"
	"net/http"
)

func main() {

	client, err := config.NewClient("guest", "guest", "localhost", "5672")
	if err != nil {
		log.Panicf("Error connecting to Rabbit: %v", err)
	}

	// Bind queue
	exchange := "transfers"
	routingKey := model.TransferStarted
	queue, err := client.BindQueue(exchange, routingKey)
	if err != nil {
		log.Panicf("Failed to bind the queue: %v", err)
	}

	// Start a consumer to read messages from the queue
	go func() {
		messages, err := client.Consume(queue)
		if err != nil {
			log.Panicf("Failed to register a consumer: %v", err)
		}
		for message := range messages {
			var transfer model.Transfer
			if err := json.Unmarshal(message.Body, &transfer); err != nil {
				log.Fatalf("Error unmarshaling message: %v", err)
			}

			// Apply business logic
			service.Process(transfer)
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

		// Publish message
		if err = client.Send(exchange, transfer.Status, "application/json", body); err != nil {
			log.Printf("Error publishing message: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		log.Printf("Published message transfer.Guid=%s", transfer.Guid)
		c.JSON(http.StatusAccepted, transfer)
	})

	// Start server
	if err = router.Run(":8080"); err != nil {
		log.Panicf("Error starting the server: %v", err)
	}
}
