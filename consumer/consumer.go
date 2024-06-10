package consumer

import "github.com/streadway/amqp"

type Consumer struct {
	Connection  *amqp.Connection
	Channel     *amqp.Channel
	Queue       amqp.Queue
	MessageChan chan []byte
}

func NewConsumer(connection *amqp.Connection, exchange, routingKey, queueName string, ch chan []byte) (*Consumer, error) {
	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}
	err = channel.ExchangeDeclare(
		exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	queue, err := channel.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)

	err = channel.QueueBind(
		queue.Name,
		routingKey,
		exchange,
		false,
		nil,
	)

	return &Consumer{
		Connection:  connection,
		Channel:     channel,
		Queue:       queue,
		MessageChan: ch,
	}, nil
}

func (c *Consumer) ConsumeMessages() error {
	mes, err := c.Channel.Consume(
		c.Queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	go func() {
		for d := range mes {
			c.MessageChan <- d.Body
		}
	}()

	return nil
}
