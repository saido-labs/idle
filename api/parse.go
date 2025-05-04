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

type JqMessageParser struct {
	params JqParserParams
}

func (j *JqMessageParser) Process(p *Pipeline, in, out RowSchema, msg model.Message) (model.Message, error) {
	v := interface{}(nil)
	if err := json.Unmarshal(msg.Data, &v); err != nil {
		return msg, err
	}

	res := Row{
		Values: make([]Value, 0, len(j.params.queries)),
	}

	for column, path := range j.params.queries {
		idx := out.ColIndex(column)
		if idx == -1 {
			return msg, fmt.Errorf("unknown column: %s", column)
		}

		e, err := jsonpath.New(path)
		if err != nil {
			return msg, fmt.Errorf("invalid jq path: %s", path)
		}

		value, err := e.EvalString(context.Background(), v)
		if err != nil {
			return msg, fmt.Errorf("jq evaluation error: %v", err.Error())
		}

		// whats best way to package this data?
		res.Values = append(res.Values, ValueFromType(out.Types[idx], value))
	}

	var buff bytes.Buffer
	if err := gob.NewEncoder(&buff).Encode(res); err != nil {
		return msg, err
	}

	return model.Message{Data: buff.Bytes()}, nil
}

func ValueFromType(s Type, value string) Value {
	switch s {
	case TypeString:
		return &StringValue{Value: value}
	default:
		panic(fmt.Sprintf("unknown type: %v '%v'", s, value))
	}
}

type JqParserParams struct {
	queries map[string]string
}

func NewJqParams(queries map[string]string) JqParserParams {
	return JqParserParams{
		queries: queries,
	}
}

func NewJqMessageParser(p JqParserParams) Processor {
	return &JqMessageParser{
		params: p,
	}
}
