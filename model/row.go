package model

import (
	"bytes"
	"encoding/gob"
)

type RowData struct {
	Values []interface{}
}

func (r *RowData) GetColumn(col int) any {
	return r.Values[col]
}

func (r *RowData) SetColumn(idx int, val any) {
	r.Values[idx] = val
}

func RowDataFromBlob(blob []byte) RowData {
	var rd RowData
	err := gob.NewDecoder(bytes.NewReader(blob)).Decode(&rd)
	if err != nil {
		panic(err)
	}
	return rd
}

type Message struct {
	Data []byte
}

func NewMessage(data []byte) Message {
	return Message{Data: data}
}
