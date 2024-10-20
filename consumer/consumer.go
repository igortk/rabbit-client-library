package consumer

import "github.com/streadway/amqp"

type Consumer struct {
	connection  *amqp.Connection
	channel     *amqp.Channel
	queue       amqp.Queue
	messageChan chan []byte
}

func NewConsumer(connection *amqp.Connection, exchange, routingKey, queueName string) (*Consumer, error) {
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
		connection:  connection,
		channel:     channel,
		queue:       queue,
		messageChan: make(chan []byte),
	}, nil
}

func (c *Consumer) ConsumeMessages() error {
	mes, err := c.channel.Consume(
		c.queue.Name,
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
			c.messageChan <- d.Body
		}
	}()

	return nil
}

func (c *Consumer) GetMessageChan() chan []byte {
	return c.messageChan
}
