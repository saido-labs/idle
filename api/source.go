package api

type Source interface {
	// Read consumes from the input source
	Read() ([]byte, error)
}
