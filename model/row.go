package model

type Message struct {
	Data []byte
}

func NewMessage(data []byte) Message {
	return Message{Data: data}
}
