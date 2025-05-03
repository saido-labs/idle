package api

import "github.com/saido-labs/idle/model"

type Processor interface {
	Process(p *Pipeline, msg model.Message) (model.Message, error)
}
