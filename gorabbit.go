package gorabbit

import (
	"github.com/streadway/amqp"
)

// Consumer is a function that takes a amqp.Delivery struct as parameter
type Consumer func(amqp.Delivery)

// MessageBroker holds RabbitMQ connections.
type MessageBroker struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	q    amqp.Queue
}

// NewMessageBroker tries to instantiate a new MessageBroker.
// This MessageBroker can consume messages from or send messages to a queue.
func NewMessageBroker(url string) (*MessageBroker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		"af_pdf",
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
		conn,
		ch,
		q,
	}, nil
}

// Listen permits to consume messages from the declared queue.
// The message has to be acknowledged
func (mb *MessageBroker) Listen(consumer Consumer) error {
	messages, err := mb.ch.Consume(
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
	mb.ch.Close()
	mb.conn.Close()
}
