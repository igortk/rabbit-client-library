package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/igortk/rabbit-client-library/common"
	"github.com/igortk/rabbit-client-library/handler"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

// Consumer represents a RabbitMQ message consumer.
type Consumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      *amqp.Queue
	handler    handler.Handler
}

func NewConsumer(connection *amqp.Connection) (*Consumer, error) {
	channel, err := connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}
	return &Consumer{
		connection: connection,
		channel:    channel,
	}, nil
}

func NewConsumerWithRoutes(
	exchange, routingKey, queueName string,
	connection *amqp.Connection,
	handler handler.Handler,
) (*Consumer, error) {
	channel, err := connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	consumer := &Consumer{
		connection: connection,
		channel:    channel,
		handler:    handler,
	}

	if err := common.ExchangeDeclare(exchange, channel); err != nil {
		return nil, fmt.Errorf("failed to declare exchange '%s': %w", exchange, err)
	}

	queue, err := common.QueueDeclare(queueName, channel)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue '%s': %w", queueName, err)
	}
	consumer.queue = &queue

	if err := common.QueueBind(queueName, routingKey, exchange, channel); err != nil {
		return nil, fmt.Errorf("failed to bind queue '%s' to routing key '%s': %w", queueName, routingKey, err)
	}

	return consumer, nil
}

func (c *Consumer) ConsumeMessages(ctx context.Context) error {
	if c.queue == nil {
		return errors.New("queue is not initialized")
	}

	dlv, err := common.ChannelConsume(c.queue.Name, c.channel)
	if err != nil {
		return fmt.Errorf("failed to start consuming messages: %w", err)
	}

	go func(ctx context.Context) {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("panic recovered in message consumer: %v", r)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				log.Info("message consumer stopped")
				return
			case mes, ok := <-dlv:
				if !ok {
					log.Warn("delivery channel closed")
					return
				}
				if err := c.handler.HandleMessage(mes.Body); err != nil {
					log.Errorf("error handling message: %v", err)
				}
			}
		}
	}(ctx)

	return nil
}

func (c *Consumer) ConsumeWithCondition(
	ctx context.Context,
	q, rk, ex string,
	timeout time.Duration,
	condition func([]byte) bool,
) ([]byte, error) {
	if _, err := common.QueueDeclare(q, c.channel); err != nil {
		return nil, fmt.Errorf("failed to declare queue '%s': %w", q, err)
	}
	defer c.channel.QueueDelete(q, false, false, false)

	if err := common.QueueBind(q, rk, ex, c.channel); err != nil {
		return nil, fmt.Errorf("failed to bind queue '%s': %w", q, err)
	}

	dlv, err := common.ChannelConsume(q, c.channel)
	if err != nil {
		return nil, fmt.Errorf("failed to start consuming messages: %w", err)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, errors.New("timeout reached while waiting for condition")
		case mes, ok := <-dlv:
			if !ok {
				return nil, errors.New("delivery channel closed")
			}
			if condition(mes.Body) {
				return mes.Body, nil
			}
		}
	}
}

func (c *Consumer) AddHandler(h handler.Handler) {
	c.handler = h
}

func (c *Consumer) AddRouting(ex, rk, q string, ch *amqp.Channel) error {
	channel := ch

	if ch == nil {
		channel = c.channel
	}

	if queue, err := common.QueueDeclare(q, channel); err == nil {
		c.queue = &queue
	} else {
		return err
	}

	if err := common.QueueBind(q, rk, ex, channel); err != nil {
		return err
	}

	return nil
}
