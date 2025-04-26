package api

import (
	"github.com/saido-labs/idle/model"
	"log"
	"sync"
)

type PipelineConfig struct {
	Input      []Source
	Processors []Processor
	Output     Sink

	// Configuration
	Parallelism int

	// Ideally this would be picked up somehow
	// but for now we manually specify it
	Schemas []model.RowSchema
}

type Pipeline struct {
	Config PipelineConfig
}

func (p Pipeline) Start() {
	messages := make(chan []byte)
	processed := make(chan []byte)

	var wg sync.WaitGroup

	for _, input := range p.Config.Input {
		go func(src Source) {
			for {
				msg, err := src.Read()
				if err != nil {
					// log.Printf("input error: %v", err)
					continue
				}
				messages <- msg
			}
		}(input)
	}

	workers := p.Config.Parallelism
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()

			for msg := range messages {
				message := model.RowFromBlob(msg)

				for _, proc := range p.Config.Processors {
					processedMessage, err := proc.Process(&p, message)
					if err != nil {
						log.Println(err)

						// FIXME(FELIX): what do we do if a msg fails?
						// skips remaining processors
						break
					}

					message = processedMessage
				}

				processed <- message.Data
			}
		}(i)
	}

	go func() {
		for processed := range processed {
			err := p.Config.Output.Write(processed)
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Wait()
	close(processed)
}
