package config

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

type RabbitMQClient struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

// NewClient initializes the RabbitMQ client
func NewClient(username, password, host, port string) (*RabbitMQClient, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", username, password, host, port)

	// Connect to RabbitMQ
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	// Create a channel
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &RabbitMQClient{connection: conn, channel: ch}, nil
}

// BindQueue bind the queue to the exchange with a routing key
func (client *RabbitMQClient) BindQueue(exchangeName, routingKey string) (*amqp.Queue, error) {

	err := client.channel.ExchangeDeclare(
		exchangeName,
		"topic", // type
		true,    // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return nil, err
	}

	queue, err := client.channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	err = client.channel.QueueBind(
		queue.Name,
		routingKey,
		exchangeName,
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return nil, err
	}

	log.Printf("Binding queue [%s] to exchange [%s] with routing key [%s]",
		queue.Name, exchangeName, routingKey)

	return &queue, nil
}

// Send sends a message to the exchange
func (client *RabbitMQClient) Send(exchange, routingKey, contentType string, body []byte) error {
	contextWithTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.channel.PublishWithContext(
		contextWithTimeout,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: contentType,
			Body:        body,
			Timestamp:   time.Now(),
		},
	)
}

// Consume consumes messages from the exchange
func (client *RabbitMQClient) Consume(queue *amqp.Queue) (<-chan amqp.Delivery, error) {

	return client.channel.Consume(
		queue.Name,
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
}
