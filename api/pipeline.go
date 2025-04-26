package api

import (
	"log"
	"sync"
)

type Pipeline struct {
	Input      Source
	Processors []Processor
	Output     Sink

	// Configuration
	Parallelism int
}

func (p Pipeline) Start() {
	messages := make(chan []byte)
	processed := make(chan []byte)

	var wg sync.WaitGroup

	go func() {
		defer close(messages)
		for {
			message, err := p.Input.Read()
			if err != nil {
				log.Println(err)
				return
			}
			messages <- message
		}
	}()

	workers := p.Parallelism
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for msg := range messages {

				// what do we do if a msg fails?
				for _, proc := range p.Processors {
					err := proc.Process(msg)
					if err != nil {
						log.Println(err)
						break
					}
				}

				processed <- msg
			}
		}(i)
	}

	go func() {
		for processed := range processed {
			err := p.Output.Write(processed)
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Wait()
	close(processed)
}
