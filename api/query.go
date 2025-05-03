package api

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/saido-labs/idle/model"
	"github.com/xwb1989/sqlparser"
	"log"
	"reflect"
	"strings"
)

type Value interface{}

type IntegerValue struct {
	Value int64
}

type FloatValue struct {
	Value float64
}

type StringValue struct {
	Value string
}

type RowIdentifier struct {
	Name string
}

func NewRowIdentifier(name string) *RowIdentifier {
	return &RowIdentifier{name}
}
func (r *RowIdentifier) String() string {
	return fmt.Sprintf("Iden(%s)", r.Name)
}

//func NewValue(value []byte) *Value {
//	return &Value{value}
//}
//func (v *Value) String() string {
//	if v == nil {
//		return "<nil>"
//	}
//	return fmt.Sprintf("Value(%v)", v.Data)
//}

type Function struct {
	Name   string
	Params []Value
}

func NewFunction(name string, params []Value) *Function {
	return &Function{name, params}
}

// Query represents an ANSI(?) SQL
// statement
type Query struct {
	query string
}

func NewQuery(query string) *Query {
	return &Query{
		query: query,
	}
}

func (q *Query) Process(p *Pipeline, msg model.Message) (model.Message, error) {
	// take the msg and do something with it based on the query
	stmt, err := sqlparser.Parse(q.query)
	if err != nil {
		return model.Message{}, err
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return q.processSelect(p, stmt, msg)
	case *sqlparser.Insert:
	}

	return msg, nil
}

func (q *Query) processExpr(expr sqlparser.Expr) any {
	switch param := expr.(type) {
	case *sqlparser.ColName:
		return NewRowIdentifier(param.Name.CompliantName())
	case *sqlparser.SQLVal:
		return parseValue(param)
	default:
		log.Println(param, "expr not yet implemented", reflect.TypeOf(param))
		panic("not yet implemented")
	}
}

func parseValue(param *sqlparser.SQLVal) any {
	switch param.Type {
	case sqlparser.IntVal:
		// decode bytes into integer
		return IntegerValue{}
	case sqlparser.StrVal:
		return StringValue{}
	case sqlparser.HexVal:
		panic("unsupported")
	case sqlparser.FloatVal:
		return FloatValue{}
	default:
		log.Println(param, "expr not yet implemented", reflect.TypeOf(param))
		panic("not yet implemented")
	}
}

func (q *Query) processFunctionExpr(name string, expr *sqlparser.FuncExpr) *Function {
	var params []Value
	for _, param := range expr.Exprs {
		switch param := param.(type) {
		case *sqlparser.AliasedExpr:
			params = append(params, q.processExpr(param.Expr))
		default:
			log.Println(param, "not yet implemented", reflect.TypeOf(param))
			panic("not yet implemented")
		}
	}
	return NewFunction(name, params)
}

func (q *Query) processSelect(p *Pipeline, stmt *sqlparser.Select, msg model.Message) (model.Message, error) {
	// load RowData from prev msg.
	record := model.RowDataFromBlob(msg.Data)

	for _, expr := range stmt.SelectExprs {
		switch col := expr.(type) {
		case *sqlparser.AliasedExpr:
			switch expr := col.Expr.(type) {
			case *sqlparser.ColName:
				column := expr.Name.String()
				columnVal := record.GetColumn(q.indexOfColumn(p, column))

				//alias := col.As.String()
				record.SetColumn(0, columnVal)

			case *sqlparser.FuncExpr:
				fn := q.processFunctionExpr(expr.Name.CompliantName(), expr)
				result := evaluate(fn)
				record.SetColumn(0, result)

			case *sqlparser.SubstrExpr:
				return model.Message{}, errors.New("substring not implemented")
			}
		default:
			return model.Message{}, errors.New("not implemented")
		}
	}

	var buff bytes.Buffer
	encoder := gob.NewEncoder(&buff)
	if err := encoder.Encode(record); err != nil {
		return model.Message{}, err
	}

	return model.NewMessage(buff.Bytes()), nil
}

func evaluate(e any) any {
	switch e := e.(type) {
	case *Function:
		return evaluateFunc(e)
	default:
		log.Println(reflect.TypeOf(e), "not yet implemented")
		panic("not yet handled")
	}
}

func evaluateFunc(e *Function) any {
	switch strings.ToLower(e.Name) {
	case "left":
		log.Println(e.Params)
		return 123

	default:
		return 456
	}
}

func (q *Query) indexOfColumn(p *Pipeline, column string) int {
	log.Println("Index lookup for", column)
	return 0
}
