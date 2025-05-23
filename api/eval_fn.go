package api

import (
	"strings"
)

func left(schema RowSchema, fn *Function, in *Row) Value {
	if len(fn.Params) != 2 {
		panic("function needs exactly two parameters")
	}

	firstParam, err := evaluate(schema, fn.Params[0], in)
	if err != nil {
		panic(err)
	}

	res, ok := firstParam.Cast(TypeString).(*StringValue)
	if !ok {
		panic("expected string evaluation")
	}

	// todo: support negative values and out of bounds properly
	secondParam, err := evaluate(schema, fn.Params[1], in)
	if err != nil {
		panic(err)
	}

	cut, ok := secondParam.Cast(TypeInteger).(*IntegerValue)
	if !ok {
		panic("expected int evaluation")
	}

	return &StringValue{Value: res.Value[:cut.Value]}
}

func lower(schema RowSchema, fn *Function, in *Row) Value {
	if len(fn.Params) != 1 {
		panic("function needs exactly one parameter")
	}

	firstParam, err := evaluate(schema, fn.Params[0], in)
	if err != nil {
		panic(err)
	}

	res, ok := firstParam.Cast(TypeString).(*StringValue)
	if !ok {
		panic("expected string evaluation")
	}

	return &StringValue{Value: strings.ToLower(res.Value)}
}

func upper(schema RowSchema, fn *Function, in *Row) Value {
	if len(fn.Params) != 1 {
		panic("function needs exactly one parameter")
	}

	firstParam, err := evaluate(schema, fn.Params[0], in)
	if err != nil {
		panic(err)
	}

	res, ok := firstParam.Cast(TypeString).(*StringValue)
	if !ok {
		panic("expected string evaluation")
	}

	return &StringValue{Value: strings.ToUpper(res.Value)}
}

func concat(schema RowSchema, fn *Function, rd *Row) Value {
	var sb strings.Builder

	for _, p := range fn.Params {
		result, err := evaluate(schema, p, rd)
		if err != nil {
			panic(err)
		}

		conv, ok := result.Cast(TypeString).(*StringValue)
		if !ok {
			panic("cannot cast to string")
		}

		if _, err := sb.WriteString(conv.Value); err != nil {
			panic(err)
		}
	}

	return &StringValue{Value: sb.String()}
}
