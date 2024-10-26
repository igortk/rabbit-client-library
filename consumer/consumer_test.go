package consumer

import (
	"fmt"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInitConsumer(t *testing.T) {
	conn, _ := amqp.Dial("amqp://guest:guest@localhost:5672/")
	consumer, err := NewConsumer(conn,
		"e.test",
		"r.rabbit-client-library.test",
		"q.rabbit-client-library.test")
	assert.NoError(t, err, "err is not nil")
	assert.NotNil(t, consumer, "consumer is nil")
	handler := func(body []byte) error {
		fmt.Println("get message")
		fmt.Println(body)

		return nil
	}
	forever := make(chan bool)

	go func() {
		err = consumer.ConsumeMessages(handler)
		if err != nil {
			fmt.Print(err)
		}
	}()

	<-forever
}
