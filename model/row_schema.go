package model

import "slices"

type RowSchema struct {
	Column []string
	Types  []string
}

func (s RowSchema) ColIndex(name string) int {
	return slices.Index(s.Column, name)
}
