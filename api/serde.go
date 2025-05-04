package api

import (
	"encoding/json"
	"github.com/saido-labs/idle/model"
)

type JsonEncoder struct{}

func (j *JsonEncoder) Process(p *Pipeline, schema model.RowSchema, msg model.Message) (model.Message, error) {
	rd := RowDataFromBlob(msg.Data)

	res, err := json.Marshal(rd.Values)
	if err != nil {
		return model.Message{}, err
	}

	return model.Message{Data: res}, nil
}

func NewJsonEncoder() *JsonEncoder {
	return &JsonEncoder{}
}
