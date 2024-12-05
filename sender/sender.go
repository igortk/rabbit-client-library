package sender

import (
	"github.com/golang/protobuf/proto"
	"github.com/igortk/rabbit-client-library/common"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"os"
)

type Sender struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

func NewSender(connection *amqp.Connection) (*Sender, error) {
	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}

	return &Sender{
		connection: connection,
		channel:    channel,
	}, nil
}

func (s *Sender) SendMessage(exchange, routingKey string, mes proto.Message) {
	messByte, err := proto.Marshal(mes)
	if err != nil {
		log.Errorf(common.ErrMarshal, err)
	}

	if err = common.ExchangeDeclare(exchange, s.channel); err != nil {
		log.Errorf(common.ErrExchangeDeclare, err)
	}

	if err = common.Publish(exchange, routingKey, messByte, s.channel); err != nil {
		log.Errorf(common.ErrPublish, err)
	}
}

func (s *Sender) SendMessage2(exchange, routingKey string, mes []byte) {
	err := os.ErrClosed
	if err = common.ExchangeDeclare(exchange, s.channel); err != nil {
		log.Errorf(common.ErrExchangeDeclare, err)
	}

	if err = common.Publish(exchange, routingKey, mes, s.channel); err != nil {
		log.Errorf(common.ErrPublish, err)
	}
}
