package consumer

import (
	"context"
	"fmt"
	"github.com/igortk/rabbit-client-library/common"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	connectUrl     = "amqp://guest:guest@localhost:5672/"
	exchangeTest   = "e.test"
	routingKeyTest = "r.rabbit-client-library.test"
	queueTest      = "q.rabbit-client-library.test"
	timeoutTest    = 10
)

const (
	errIsNotNil   = "err is not nil"
	consumerIsNil = "consumer is nil"
)

const (
	receivedMessage = "received message"
)

type funcHandler func(body []byte) error

func (f funcHandler) HandleMessage(body []byte) error {
	return f(body)
}

func TestInitConsumer(t *testing.T) {
	conn, _ := common.Connect(connectUrl)
	consumer, err := NewConsumerWithRoutes(exchangeTest,
		routingKeyTest,
		queueTest,
		conn,
		funcHandler(func(body []byte) error {
			fmt.Println(receivedMessage)
			fmt.Println(string(body))

			return nil
		}))

	assert.NoError(t, err, errIsNotNil)
	assert.NotNil(t, consumer, consumerIsNil)

	forever := make(chan bool)

	go func() {
		err = consumer.ConsumeMessages(context.Background())
		if err != nil {
			fmt.Print(err)
		}
	}()

	<-forever
}

func TestConsumerWithCondition(t *testing.T) {
	conn, _ := common.Connect(connectUrl)
	consumer, err := NewConsumer(conn)
	if err != nil {
		fmt.Println(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	mes, err := consumer.ConsumeWithCondition(ctx,
		queueTest+"condition",
		routingKeyTest+"condition",
		exchangeTest,
		timeoutTest*time.Second,
		func(bytes []byte) bool {
			if bytes != nil {
				return true
			}
			return false
		})

	fmt.Println(mes)
}
