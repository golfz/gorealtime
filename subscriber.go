package gorealtime

import (
	"context"
	"encoding/json"
	"github.com/golfz/gorealtime/rabbitmq"
	"log"
	"sync"
)

func Subscribe(wg *sync.WaitGroup, ctx context.Context, amqpEndpoint string, scope string, topic string, subscriberID string, filterArgs map[string][]string, returnDataCh chan<- []byte) error {
	defer wg.Done()
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
				returnDataCh <- b
			}

			d.Ack(false)
		}
	}()

	log.Printf("[subscriber: %s] waiting for msg from topic (%s) \n", subscriberID, topic)

	<-ctx.Done()

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
