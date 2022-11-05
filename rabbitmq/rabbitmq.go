package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"strings"
)

func ExchangeNameRealtime() string {
	return exchangeNameRealtime
}

func ExchangeNameTopic(scope string, topic string) string {
	scope = strings.TrimSpace(scope)
	topic = strings.TrimSpace(topic)
	return fmt.Sprintf(exchangeNameTopic, scope, topic)
}

func QueueNameSubscriber(scope string, topic string, subscriber string) string {
	scope = strings.TrimSpace(scope)
	topic = strings.TrimSpace(topic)
	subscriber = strings.TrimSpace(subscriber)
	return fmt.Sprintf(queueNameSubscriber, scope, topic, subscriber)
}

func DeclareRealtimeExchange(amqpEndpoint string) error {
	// connect
	conn, err := amqp.Dial(amqpEndpoint)
	if err != nil {
		log.Printf("failed to connect to RabbitMQ: %s, err: %v", amqpEndpoint, err)
		return err
	}
	defer conn.Close()

	// channel
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("failed to open a channel, err: %v", err)
		return err
	}
	defer ch.Close()

	// declare Realtime exchange
	err = ch.ExchangeDeclare(
		ExchangeNameRealtime(), // name
		amqp.ExchangeHeaders,   // type
		true,                   // durable
		false,                  // auto-deleted
		false,                  // internal
		false,                  // no-wait
		nil,                    // arguments
	)
	if err != nil {
		log.Printf("failed to declare realtime exchange: %s, err: %v", ExchangeNameRealtime(), err)
		return err
	}

	return nil
}

func DeclareSubscriberQueue(amqpEndpoint string, scope string, topic string, subscriber string) (*amqp.Connection, *amqp.Channel, error) {
	// declare realtime exchange
	err := DeclareRealtimeExchange(amqpEndpoint)
	if err != nil {
		return nil, nil, err
	}

	// connect
	conn, err := amqp.Dial(amqpEndpoint)
	if err != nil {
		log.Printf("failed to connect to RabbitMQ: %s, err: %v", amqpEndpoint, err)
		return nil, nil, err
	}

	// channel
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("failed to open a channel, err: %v", err)
		conn.Close()
		return nil, nil, err
	}

	// declare Topic exchange
	err = ch.ExchangeDeclare(
		ExchangeNameTopic(scope, topic), // name
		amqp.ExchangeFanout,             // type
		true,                            // durable
		true,                            // auto-deleted
		false,                           // internal
		false,                           // no-wait
		nil,                             // arguments
	)
	if err != nil {
		log.Printf("failed to declare topic exchange: %s, err: %v", ExchangeNameTopic(scope, topic), err)
		ch.Close()
		conn.Close()
		return nil, nil, err
	}

	// bind Topic exchange to Realtime exchange
	err = ch.ExchangeBind(
		ExchangeNameTopic(scope, topic), // destination
		"",                              // routing key
		ExchangeNameRealtime(),          // source
		false,                           // no-wait
		amqp.Table{
			"scope": strings.TrimSpace(scope),
			"topic": strings.TrimSpace(topic),
		},
	)
	if err != nil {
		log.Printf("failed to bind topic exchange: %s to realtime exchange: %s, err: %v", ExchangeNameTopic(scope, topic), ExchangeNameRealtime(), err)
		ch.Close()
		conn.Close()
		return nil, nil, err
	}

	// declare queue
	_, err = ch.QueueDeclare(
		QueueNameSubscriber(scope, topic, subscriber), // name
		true,  // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Printf("failed to declare queue: %s, err: %v", QueueNameSubscriber(scope, topic, subscriber), err)
		ch.Close()
		conn.Close()
		return nil, nil, err
	}

	err = ch.QueueBind(
		QueueNameSubscriber(scope, topic, subscriber), // queue name
		"",                              // routing key
		ExchangeNameTopic(scope, topic), // exchange
		false,                           // no-wait
		nil,                             // arguments
	)
	if err != nil {
		log.Printf("failed to bind queue: %s to exchange: %s, err: %v", QueueNameSubscriber(scope, topic, subscriber), ExchangeNameTopic(scope, topic), err)
		ch.Close()
		conn.Close()
		return nil, nil, err
	}

	return conn, ch, nil
}

func Publish(amqpEndpoint string, body []byte, headers amqp.Table) error {
	// connect
	conn, err := amqp.Dial(amqpEndpoint)
	if err != nil {
		log.Printf("failed to connect to RabbitMQ: %s, err: %v", amqpEndpoint, err)
		return err
	}
	defer conn.Close()

	// channel
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("failed to open a channel, err: %v", err)
		return err
	}
	defer ch.Close()

	err = ch.Publish(
		ExchangeNameRealtime(), // exchange
		"",                     // routing key
		false,                  // mandatory
		false,                  // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
			Headers:      headers,
		},
	)
	if err != nil {
		log.Printf("failed to pubish, err: %v", err)
		return err
	}

	return nil
}
