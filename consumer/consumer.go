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
	connection  *amqp.Connection
	channel     *amqp.Channel
	queue       amqp.Queue
	messageChan chan []byte //Deprecated
}

func NewConsumer(connection *amqp.Connection, exchange, routingKey, queueName string) (*Consumer, error) {
	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		connection:  connection,
		channel:     channel,
		messageChan: make(chan []byte),
	}

	if err := common.ExchangeDeclare(exchange, channel); err != nil {
		return nil, err
	}

	if queue, err := common.QueueDeclare(queueName, channel); err == nil {
		consumer.queue = queue
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
	duration time.Duration,
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

	timeoutChan := time.After(duration)

	for {
		select {
		case msg := <-dlv:
			if condition(msg.Body) {
				return msg.Body, nil
			}
		case <-timeoutChan:
			return nil, errors.New(fmt.Sprintf(common.TimeoutException, duration.Seconds()))
		}
	}
}

func (c *Consumer) GetMessageChan() chan []byte {
	return c.messageChan
}
