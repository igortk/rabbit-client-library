package common

import "github.com/streadway/amqp"

func ExchangeDeclare(ex string, ch *amqp.Channel) error {
	return ch.ExchangeDeclare(
		ex,
		amqp.ExchangeTopic,
		true,
		false,
		false,
		false,
		nil,
	)
}

func QueueDeclare(qName string, ch *amqp.Channel) (amqp.Queue, error) {
	return ch.QueueDeclare(
		qName,
		false,
		false,
		false,
		false,
		nil,
	)
}

func QueueBind(qName, rk, ex string, ch *amqp.Channel) error {
	return ch.QueueBind(
		qName,
		rk,
		ex,
		false,
		nil,
	)
}

func ChannelConsume(qName string, ch *amqp.Channel) (<-chan amqp.Delivery, error) {
	return ch.Consume(
		qName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
}

func Publish(exchange, routingKey string, message []byte, ch *amqp.Channel) error {
	return ch.Publish(
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Body: message,
		},
	)
}
