package api

import (
	"bytes"
	"encoding/gob"
)

type Row struct {
	Values []Value
}

func (r *Row) GetColumn(col int) Value {
	return r.Values[col]
}

func (r *Row) SetColumn(idx int, val Value) {
	r.Values[idx] = val
}

func RowDataToBlob(rd *Row) []byte {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(rd); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func RowDataFromBlob(blob []byte) Row {
	var rd Row
	err := gob.NewDecoder(bytes.NewReader(blob)).Decode(&rd)
	if err != nil {
		panic(err)
	}
	return rd
}
