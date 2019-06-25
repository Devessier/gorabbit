package gorabbit

import (
	"github.com/streadway/amqp"
)

// Consumer is a function that takes a amqp.Delivery struct as parameter
type Consumer func(amqp.Delivery)

// MessageBroker holds RabbitMQ connections.
type MessageBroker struct {
	*amqp.Channel

	conn *amqp.Connection
	q    amqp.Queue
}

// NewMessageBroker tries to instantiate a new MessageBroker.
// This MessageBroker can consume messages from or send messages to a queue.
func NewMessageBroker(amqpURL, queue string) (*MessageBroker, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		queue,
		true,
		false,
		false,
		true,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &MessageBroker{
		ch,

		conn,
		q,
	}, nil
}

// Listen permits to consume messages from the declared queue.
// The message has to be acknowledged
func (mb *MessageBroker) Listen(consumer Consumer) error {
	messages, err := mb.Consume(
		mb.q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	done := make(chan bool)

	go func() {
		for request := range messages {
			go consumer(request)
		}
	}()

	<-done

	return nil
}

// Close closes all open connections
func (mb *MessageBroker) Close() {
	mb.Channel.Close()
	mb.conn.Close()
}
