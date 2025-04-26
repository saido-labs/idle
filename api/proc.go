package api

type Processor interface {
	Process(msg []byte) error
}
