package api

import (
	"log"
	"os"
)

type Sink interface {
	Write([]byte) error
}

// Printer will write to stdout
type Printer struct {
	logger *log.Logger
}

func NewPrinter() *Printer {
	return &Printer{
		logger: log.New(os.Stdout, "", 0),
	}
}

func (p *Printer) Write(blob []byte) error {
	p.logger.Println(string(blob))
	return nil
}
