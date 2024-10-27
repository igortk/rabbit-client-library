package consumer

import (
	"errors"
	"fmt"
	"github.com/igortk/rabbit-client-library/common"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

// Consumer represents a RabbitMQ message consumer.
type Consumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      *amqp.Queue
}

func NewConsumer(connection *amqp.Connection) (*Consumer, error) {
	if channel, err := connection.Channel(); err == nil {
		return &Consumer{
			connection: connection,
			channel:    channel,
		}, nil
	} else {
		return nil, err
	}
}

func NewConsumerWithRouts(exchange, routingKey, queueName string, connection *amqp.Connection) (*Consumer, error) {
	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		connection: connection,
		channel:    channel,
	}

	if err := common.ExchangeDeclare(exchange, channel); err != nil {
		return nil, err
	}

	if queue, err := common.QueueDeclare(queueName, channel); err == nil {
		consumer.queue = &queue
	} else {
		return nil, err
	}

	if err := common.QueueBind(queueName, routingKey, exchange, channel); err != nil {
		return nil, err
	}

	return consumer, nil
}

func (c *Consumer) ConsumeMessages(handler func([]byte) error) error {
	dlv, err := common.ChannelConsume(c.queue.Name, c.channel)
	if err != nil {
		return err
	}

	go func() {
		for mes := range dlv {
			if err := handler(mes.Body); err != nil {
				log.Errorf(common.ErrHandlerMessage, err)
			}
		}
	}()

	return nil
}

func (c *Consumer) ConsumeWithCondition(
	queueName, routingKey, exchange string,
	timeout time.Duration,
	condition func([]byte) bool) ([]byte, error) {

	if _, err := common.QueueDeclare(queueName, c.channel); err != nil {
		return nil, err
	}

	if err := common.QueueBind(queueName, routingKey, exchange, c.channel); err != nil {
		return nil, err
	}

	dlv, err := common.ChannelConsume(queueName, c.channel)
	if err != nil {
		return nil, err
	}

	timeoutChan := time.After(timeout * time.Second)

	for {
		select {
		case msg := <-dlv:
			if condition(msg.Body) {
				return msg.Body, nil
			}
		case <-timeoutChan:
			return nil, errors.New(fmt.Sprintf(common.TimeoutException, timeout))
		}
	}
}
