package api

import "github.com/saido-labs/idle/model"

type Processor interface {
	Process(p *Pipeline, in, out RowSchema, msg model.Message) (model.Message, error)
}

type PipelineStep struct {
	Name   string
	Proc   Processor
	Schema RowSchema
}

func NewPipelineStep(name string, proc Processor, schema RowSchema) PipelineStep {
	return PipelineStep{
		Name:   name,
		Proc:   proc,
		Schema: schema,
	}
}
