package api

import (
	"github.com/saido-labs/idle/model"
	"log"
	"sync"
)

type Pipeline struct {
	Input       []Source
	Processors  []PipelineStep
	Output      Sink
	SideOutput  Sink
	Parallelism int
}

func (p Pipeline) Start() {
	messages := make(chan model.Message)
	processed := make(chan model.Message)
	failed := make(chan model.Message)

	var wg sync.WaitGroup

	for _, input := range p.Input {
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

	workers := p.Parallelism
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()

			for currentMessage := range messages {
				message := currentMessage

				processingFailed := false

				var inSchema RowSchema

				for _, step := range p.Processors {
					log.Println(step.Name+":", inSchema)
					log.Println("produces", step.Schema)
					log.Println("Message:", string(message.Data))

					var err error
					message, err = step.Proc.Process(&p, inSchema, step.Schema, message)

					inSchema = step.Schema

					if err != nil {
						log.Printf("Error processing message %v: %v", string(message.Data), err)
						processingFailed = true
						break
					}
				}

				if processingFailed {
					failed <- message
				} else {
					processed <- message
				}
			}
		}(i)
	}

	go func() {
		for failed := range failed {
			if p.SideOutput != nil {
				err := p.SideOutput.Write(failed.Data)
				if err != nil {
					log.Println("Failed to write to side output", err)
				}
			}
		}
	}()

	go func() {
		for processed := range processed {
			err := p.Output.Write(processed.Data)
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Wait()
	close(processed)
}
