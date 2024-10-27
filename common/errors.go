package common

// consumers error
const (
	TimeoutException  = "operation timed out after %d seconds while waiting for a message from the RabbitMQ queue"
	ErrHandlerMessage = "err handle message: %s"
)

// senders error
const (
	ErrPublish         = "err, publish message. Error: %s"
	ErrMarshal         = "err, marshal message for send message. Error: %s"
	ErrExchangeDeclare = "err, exchange declare for send message. Error: %s"
)
