package config

import (
	"cloud.google.com/go/pubsub"
	"context"
	"log"
)

type PubSubConnection struct {
	client *pubsub.Client
}

// NewConnection initializes the PubSub client
func NewConnection(projectID string) (*PubSubConnection, error) {
	client, err := pubsub.NewClient(context.TODO(), projectID)
	if err != nil {
		return nil, err
	}
	return &PubSubConnection{client: client}, nil
}

// FindTopic finds or creates a topic
func (conn *PubSubConnection) FindTopic(topicName string) (*pubsub.Topic, error) {
	topic := conn.client.Topic(topicName)
	exists, err := topic.Exists(context.TODO())
	if err != nil {
		return nil, err
	}
	if !exists {
		log.Printf("Creating topic: %s", topic.String())
		return conn.client.CreateTopic(context.TODO(), topic.ID())
	}
	return topic, nil
}

// Subscribe creates a subscription to a topic
func (conn *PubSubConnection) Subscribe(topic *pubsub.Topic, filter string) (*pubsub.Subscription, error) {
	subscription := conn.client.Subscription(topic.ID() + ".sub")
	exists, err := subscription.Exists(context.TODO())
	if err != nil {
		return nil, err
	}
	if !exists {
		log.Printf("Creating subscription: %s", subscription.String())
		config := pubsub.SubscriptionConfig{Topic: topic, Filter: filter}
		return conn.client.CreateSubscription(context.TODO(), subscription.ID(), config)
	}
	return subscription, nil
}
