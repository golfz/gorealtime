package gorealtime

import (
	"context"
	"encoding/json"
	"github.com/golfz/gorealtime/rabbitmq"
	"github.com/streadway/amqp"
	"log"
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

func Subscribe(ctxWithCancel context.Context, amqpEndpoint string, scope string, topic string, subscriberID string, filterArgs map[string][]string, returnDataCh chan<- RealtimePayload) error {
	defer func() {
		log.Printf("[subscriber: %s] stopped subscriber for topic (%s) \n", subscriberID, topic)
	}()

	conn, ch, err := rabbitmq.DeclareSubscriberQueue(amqpEndpoint, scope, topic, subscriberID)
	if err != nil {
		return err
	}
	defer conn.Close()
	defer ch.Close()

	err = ch.Qos(
		1,
		0,
		false,
	)
	if err != nil {
		log.Printf("failed to set QOS, err: %v", err)
		return err
	}

	msgs, err := ch.Consume(
		rabbitmq.QueueNameSubscriber(scope, topic, subscriberID), // queue
		"",    // consumer
		false, // auto-ack
		true,  // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Printf("failed to register a consumer, err: %v", err)

		return err
	}

	go func() {
		for d := range msgs {
			b := d.Body

			var payload RealtimePayload
			err := json.Unmarshal(b, &payload)
			if err != nil {
				d.Ack(false)
				continue
			}

			if isMatchWithArgs(payload, filterArgs) {
				returnDataCh <- payload
			}

			d.Ack(false)
		}
	}()

	log.Printf("[subscriber: %s] waiting for msg from topic (%s) \n", subscriberID, topic)

	<-ctxWithCancel.Done()

	return nil
}

func isMatchWithArgs(payload RealtimePayload, conditionArgs map[string][]string) bool {
	if len(conditionArgs) == 0 {
		log.Println("isMatchWithArgs: [matched], condition args is empty")
		return true
	}

	// if no data args, then not matched
	dataArgs := payload.Args
	if len(dataArgs) == 0 {
		log.Println("isMatchWithArgs: [not matched], data args is empty")
		return false
	}

	hasConditionValues := false

	for argField, valueList := range conditionArgs {
		// if this field no value to check, then skip
		if len(valueList) == 0 {
			log.Printf("isMatchWithArgs: [skip], condition args field (%s) has no value to check", argField)
			continue
		}

		hasConditionValues = true

		// if this field have value to check, but the data does not have this field, then [not matched]
		if dataArgs[argField] == "" {
			log.Printf("isMatchWithArgs: [not matched], data args does not have field (%s)", argField)
			return false
		}

		for _, value := range valueList {
			if dataArgs[argField] == value {
				log.Printf("isMatchWithArgs: [matched], data args field (%s) value (%s) matched with condition args field (%s) value (%s)", argField, dataArgs[argField], argField, value)
				return true
			}
		}
	}

	if hasConditionValues {
		log.Printf("isMatchWithArgs: [not matched], data args not matched with any condition args")
		return false
	}

	log.Printf("isMatchWithArgs: [matched], default matched: no condition values")
	return true
}
