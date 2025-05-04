package api

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"slices"
	"strings"
)

type RowSchema struct {
	Column []string
	Types  []Type
}

func (s RowSchema) ColIndex(name string) int {
	return slices.Index(s.Column, name)
}

func (s RowSchema) String() string {
	return fmt.Sprintf("%v", strings.Join(s.Column, ";"))
}

type Row struct {
	Values []Value
}

func (r *Row) GetColumn(col int) Value {
	if col >= len(r.Values) || col < 0 {
		return nil
	}
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

func RowDataFromBlob(blob []byte) (*Row, error) {
	rd := &Row{}
	err := gob.NewDecoder(bytes.NewReader(blob)).Decode(&rd)
	if err != nil {
		return nil, err
	}
	return rd, nil
}
