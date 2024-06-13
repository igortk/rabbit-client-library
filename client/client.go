package client

import (
	"github.com/igortk/rabbit-client-library/consumer"
	"github.com/igortk/rabbit-client-library/sender"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Client struct {
	consumer *consumer.Consumer
	sender   *sender.Sender
}

func NewRabbitClient(connUrl, exchange, routingKey, queueName string) *Client {
	connection, err := amqp.Dial(connUrl)
	if err != nil {
		log.Errorf("err init connection: %s", err)
		return nil
	}

	sen, err := sender.NewSender(connection)
	if err != nil {
		log.Errorf("err create sender: %s", err)
		return nil
	}

	con, err := consumer.NewConsumer(connection, exchange, routingKey, queueName, make(chan []byte))
	if err != nil {
		log.Errorf("err create consumer: %s", err)
		return nil
	}

	return &Client{
		consumer: con,
		sender:   sen,
	}
}
