package api

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"github.com/PaesslerAG/jsonpath"
	"github.com/saido-labs/idle/model"
)

type JqMessageParser struct {
	schema model.RowSchema
}

func (j *JqMessageParser) Process(p *Pipeline, msg model.Message) (model.Message, error) {
	v := interface{}(nil)
	if err := json.Unmarshal(msg.Data, &v); err != nil {
		return model.Message{}, err
	}

	res := model.RowData{
		Values: []interface{}{},
	}

	for _, column := range j.schema.Column {
		e, _ := jsonpath.New(column)

		// eval depends on type
		value, err := e.EvalString(context.Background(), v)
		if err != nil {
			return model.Message{}, err
		}

		// whats best way to package this data?
		res.Values = append(res.Values, value)
	}

	var buff bytes.Buffer
	if err := gob.NewEncoder(&buff).Encode(res); err != nil {
		return model.Message{}, err
	}

	return model.Message{Data: buff.Bytes()}, nil
}

func NewMessageParser(schema model.RowSchema) Processor {
	return &JqMessageParser{
		schema: schema,
	}
}
