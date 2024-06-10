package sender

import (
	"github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
)

type Sender struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
}

func NewSender(connection *amqp.Connection) (*Sender, error) {
	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}

	return &Sender{
		Connection: connection,
		Channel:    channel,
	}, nil
}

func (s Sender) SendMessage(exchange, routingKey, kind string, mes proto.Message) error {
	message, err := proto.Marshal(mes)
	if err != nil {
		return err
	}

	err = s.Channel.ExchangeDeclare(
		exchange,
		kind,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	err = s.Channel.Publish(
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Body: message,
		},
	)

	return err
}
