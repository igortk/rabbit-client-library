package sender

import (
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
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

func (s *Sender) SendMessage(exchange, routingKey string, mes proto.Message) {
	messByte, err := proto.Marshal(mes)
	if err != nil {
		log.Errorf("err, marshal message for send message. Error: %s", err)
	}

	err = s.ExchangeDeclare(exchange)
	if err != nil {
		log.Errorf("err, exchange declare for send message. Error: %s", err)
	}

	err = s.Publish(exchange, routingKey, messByte)
	if err != nil {
		log.Errorf("err, publish message. Error: %s", err)
	}

}

func (s *Sender) ExchangeDeclare(exchange string) error {
	err := s.Channel.ExchangeDeclare(
		exchange,
		"topic", //TODO fix magic number
		true,
		false,
		false,
		false,
		nil,
	)

	return err
}

func (s *Sender) Publish(exchange, routingKey string, message []byte) error {
	err := s.Channel.Publish(
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
