package gorealtime

import (
	"encoding/json"
	"github.com/golfz/gorealtime/rabbitmq"
	"github.com/streadway/amqp"
	"strings"
	"time"
)

type RealtimePayload struct {
	Scope     string            `json:"scope"`
	Topic     string            `json:"topic"`
	Timestamp time.Time         `json:"timestamp"`
	Args      map[string]string `json:"args"`
	Payload   string            `json:"payload"`
}

type Publisher interface {
	Publish(topic string, payload string, args map[string]string) error
}

type AMQPPublisher struct {
	amqpEndpoint string
	scope        string
}

func NewAMQPPublisher(amqpEndpoint string, scope string) *AMQPPublisher {
	return &AMQPPublisher{
		amqpEndpoint: strings.TrimSpace(amqpEndpoint),
		scope:        strings.TrimSpace(scope),
	}
}

func (p *AMQPPublisher) Publish(topic string, payload string, args map[string]string) error {
	rabbitMQPayload := RealtimePayload{
		Scope:     p.scope,
		Topic:     strings.TrimSpace(topic),
		Timestamp: time.Now(),
		Args:      args,
		Payload:   payload,
	}

	err := rabbitmq.DeclareRealtimeExchange(p.amqpEndpoint)
	if err != nil {
		return err
	}

	headers := amqp.Table{
		"scope": rabbitMQPayload.Scope,
		"topic": rabbitMQPayload.Topic,
	}
	bBody, _ := json.Marshal(rabbitMQPayload)

	err = rabbitmq.Publish(
		p.amqpEndpoint,
		bBody,
		headers,
	)
	if err != nil {
		return err
	}

	return nil
}
