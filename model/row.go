package model

import (
	"bytes"
	"encoding/gob"
)

type RowData struct {
	Values []interface{}
}

func RowDataFromBlob(blob []byte) RowData {
	var rd RowData
	err := gob.NewDecoder(bytes.NewReader(blob)).Decode(&rd)
	if err != nil {
		panic(err)
	}
	return rd
}

type Row struct {
	// data block
	// extract cols by index
	// how do we align data?

	Data []byte

	// Add headers for serde?
}

func (r *RowData) GetColumn(col int) any {
	return r.Values[col]
}

func (r *RowData) SetColumn(idx int, val any) {
	r.Values[idx] = val
}

func RowFromBlob(blob []byte) Row {
	return Row{Data: blob}
}
