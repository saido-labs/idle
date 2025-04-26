package api

import (
	"encoding/json"
	"github.com/saido-labs/idle/model"
)

type JsonEncoder struct{}

func (j *JsonEncoder) Process(p *Pipeline, msg model.Row) (model.Row, error) {
	rd := model.RowDataFromBlob(msg.Data)

	res, err := json.Marshal(rd.Values)
	if err != nil {
		return model.Row{}, err
	}

	return model.Row{Data: res}, nil
}

func NewJsonEncoder() *JsonEncoder {
	return &JsonEncoder{}
}
