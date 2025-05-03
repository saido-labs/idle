package api

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
	"log"
	"reflect"
	"strconv"
	"strings"
)

type Evaluation struct {
	Reads   []Value
	Filters []Value

	// this is probably quite hard to pull off.
	Joins []any
}

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

type Function struct {
	Name   string
	Params []Value
}

func NewFunction(name string, params []Value) *Function {
	return &Function{name, params}
}

// ----

type Evaluator struct{}

func (q *Evaluator) processExpr(expr sqlparser.Expr) any {
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
		val, err := strconv.ParseInt(string(param.Val), 10, 64)
		if err != nil {
			panic(err)
		}

		return &IntegerValue{
			Value: val,
		}

	case sqlparser.StrVal:
		return &StringValue{}

	case sqlparser.HexVal:
		panic("unsupported")

	case sqlparser.FloatVal:
		return &FloatValue{}
		
	default:
		log.Println(param, "expr not yet implemented", reflect.TypeOf(param))
		panic("not yet implemented")
	}
}

func (q *Evaluator) processFunctionExpr(name string, expr *sqlparser.FuncExpr) *Function {
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

func (q *Query) indexOfColumn(column string) int {
	log.Println("Index lookup for", column)
	return 0
}

func (q *Evaluator) processSelect(stmt *sqlparser.Select) *Evaluation {
	eval := &Evaluation{
		Reads:   []Value{},
		Filters: []Value{},
		Joins:   nil,
	}

	// 1. parse the reads from select
	for _, expr := range stmt.SelectExprs {
		switch col := expr.(type) {
		case *sqlparser.AliasedExpr:
			switch expr := col.Expr.(type) {
			case *sqlparser.ColName:
				column := expr.Name.String()
				eval.Reads = append(eval.Reads, NewRowIdentifier(column))

			case *sqlparser.FuncExpr:
				fn := q.processFunctionExpr(expr.Name.CompliantName(), expr)
				eval.Reads = append(eval.Reads, fn)

			default:
				log.Println(reflect.TypeOf(expr), "is not yet implemented")
				panic("not yet implemented")

			}
		default:
			panic("not yet implemented!")
		}
	}

	// 2. parse the where to form our filters.

	return eval
}

func (q *Evaluator) Eval(query string) *Evaluation {
	// take the msg and do something with it based on the query
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		panic(err)
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return q.processSelect(stmt)
	default:
		panic("not supported!")
	}
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
