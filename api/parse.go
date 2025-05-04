package api

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/PaesslerAG/jsonpath"
	"github.com/saido-labs/idle/model"
)

type JqMessageParser struct{}

func (j *JqMessageParser) Process(p *Pipeline, schema RowSchema, msg model.Message) (model.Message, error) {
	v := interface{}(nil)
	if err := json.Unmarshal(msg.Data, &v); err != nil {
		return model.Message{}, err
	}

	res := Row{
		Values: []Value{},
	}

	for idx, column := range schema.Column {
		e, _ := jsonpath.New(column)

		// eval depends on type
		value, err := e.EvalString(context.Background(), v)
		if err != nil {
			return model.Message{}, err
		}

		// whats best way to package this data?
		res.Values = append(res.Values, ValueFromType(schema.Types[idx], value))
	}

	var buff bytes.Buffer
	if err := gob.NewEncoder(&buff).Encode(res); err != nil {
		return model.Message{}, err
	}

	return model.Message{Data: buff.Bytes()}, nil
}

func ValueFromType(s Type, value string) Value {
	switch s {
	case TypeString:
		return &StringValue{Value: value}
	default:
		panic(fmt.Sprintf("unknown type: %v", s))
	}
}

func NewMessageParser() Processor {
	return &JqMessageParser{}
}
