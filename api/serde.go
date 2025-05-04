package api

import (
	"encoding/json"
	"errors"
	"github.com/saido-labs/idle/model"
	"io"
	"log"
)

type JsonEncoder struct{}

func (j *JsonEncoder) Process(p *Pipeline, schema RowSchema, msg model.Message) (model.Message, error) {
	rd, err := RowDataFromBlob(msg.Data)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			log.Fatalf("JsonEncoder(): Error %v\n", err.Error())
			return model.Message{}, err
		}
		return model.Message{}, nil
	}

	// coalesce all values into string
	out := make([]string, 0, len(rd.Values))
	for _, val := range rd.Values {
		res, ok := val.Cast(TypeString).(*StringValue)
		if !ok {
			log.Fatalf("JsonEncoder(): Error %v\n", val)
		}

		out = append(out, res.Value)
	}

	res, err := json.Marshal(out)
	if err != nil {
		return model.Message{}, err
	}

	return model.Message{Data: res}, nil
}

func NewJsonEncoder() *JsonEncoder {
	return &JsonEncoder{}
}
