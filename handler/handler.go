package handler

type Handler interface {
	HandleMessage(msg []byte) error
}
