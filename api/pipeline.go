package api

import (
	"github.com/saido-labs/idle/model"
	"log"
	"sync"
)

type PipelineConfig struct {
	Input      []Source
	Processors []PipelineStep
	Output     Sink

	// Configuration
	Parallelism int
}

type Pipeline struct {
	Config PipelineConfig
}

func (p Pipeline) Start() {
	messages := make(chan model.Message)
	processed := make(chan model.Message)

	var wg sync.WaitGroup

	for _, input := range p.Config.Input {
		go func(src Source) {
			for {
				msg, err := src.Read()
				if err != nil {
					// log.Printf("input error: %v", err)
					continue
				}
				messages <- model.NewMessage(msg)
			}
		}(input)
	}

	workers := p.Config.Parallelism
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()

			for currentMessage := range messages {
				message := currentMessage

				for _, step := range p.Config.Processors {
					processedMessage, err := step.Proc.Process(&p, step.Schema, message)
					if err != nil {
						log.Println(err)
						break
					}

					message = processedMessage
				}

				processed <- message
			}
		}(i)
	}

	go func() {
		for processed := range processed {
			err := p.Config.Output.Write(processed.Data)
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Wait()
	close(processed)
}
