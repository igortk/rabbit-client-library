package consumer

import (
	"fmt"
	"github.com/igortk/rabbit-client-library/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	connectUrl     = "amqp://guest:guest@localhost:5672/"
	exchangeTest   = "e.test"
	routingKeyTest = "r.rabbit-client-library.test"
	queueTest      = "q.rabbit-client-library.test"
	timeoutTest    = 5
)

const (
	errIsNotNil   = "err is not nil"
	consumerIsNil = "consumer is nil"
)

const (
	receivedMessage = "received message"
)

func TestInitConsumer(t *testing.T) {
	conn, _ := common.Connect(connectUrl)
	consumer, err := NewConsumerWithRouts(exchangeTest,
		routingKeyTest,
		queueTest,
		conn)

	assert.NoError(t, err, errIsNotNil)
	assert.NotNil(t, consumer, consumerIsNil)

	forever := make(chan bool)

	go func() {
		err = consumer.ConsumeMessages(
			func(body []byte) error {
				fmt.Println(receivedMessage)
				fmt.Println(body)

				return nil
			})
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

	mes, err := consumer.ConsumeWithCondition(queueTest,
		routingKeyTest,
		exchangeTest,
		timeoutTest,
		func(bytes []byte) bool {
			if bytes != nil {
				return true
			}
			return false
		})

	fmt.Println(mes)
}
