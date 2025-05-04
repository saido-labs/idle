package api

import (
	"github.com/saido-labs/idle/model"
	"strings"
)

func concat(schema model.RowSchema, fn *Function, rd Row) Value {
	var sb strings.Builder

	for _, p := range fn.Params {
		result := evaluate(schema, p, rd)

		conv, ok := result.Cast(TypeString).(StringValue)
		if !ok {
			panic("cannot cast to string")
		}

		if _, err := sb.WriteString(conv.Value); err != nil {
			panic(err)
		}
	}

	return StringValue{Value: sb.String()}
}
