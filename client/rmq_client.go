package client

import (
	"github.com/igortk/rabbit-client-library/consumer"
	"github.com/igortk/rabbit-client-library/sender"
)

type RmqClient struct {
	consumer *consumer.Consumer
	sender   *sender.Sender
}
