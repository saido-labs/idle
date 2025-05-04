package api

import (
	"errors"
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

type BinaryExpr struct {
	Operator    string
	Left, Right Value
}

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

func (q *Evaluator) parseValue(param *sqlparser.SQLVal) any {
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
		return &StringValue{
			Value: string(param.Val),
		}

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
			expr, err := q.parseExpr(param.Expr)
			if err != nil {
				panic(err)
			}
			params = append(params, expr)
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
			evaluation, err := q.parseExpr(col.Expr)
			if err != nil {
				panic(err)
			}
			eval.Reads = append(eval.Reads, evaluation)
		default:
			panic("not yet implemented!")
		}
	}

	// 2. parse the where to form our filters.
	if stmt.Where != nil {
		whereExpr, err := q.parseExpr(stmt.Where.Expr)
		if err != nil {
			panic(err)
		}

		eval.Filters = append(eval.Filters, whereExpr)
	}

	return eval
}

func (q *Evaluator) parseExpr(col sqlparser.Expr) (Value, error) {
	switch expr := col.(type) {
	case *sqlparser.ColName:
		column := expr.Name.String()
		return NewRowIdentifier(column), nil
	case *sqlparser.FuncExpr:
		return q.processFunctionExpr(expr.Name.CompliantName(), expr), nil
	case *sqlparser.SQLVal:
		return q.parseValue(expr), nil
	case *sqlparser.ComparisonExpr:
		lhand, err := q.parseExpr(expr.Left)
		if err != nil {
			panic(err)
		}

		rhand, err := q.parseExpr(expr.Right)
		if err != nil {
			panic(err)
		}

		if expr.Escape != nil {
			panic("ESCAPE is not implemented")
		}

		return &BinaryExpr{
			Operator: expr.Operator,
			Left:     lhand,
			Right:    rhand,
		}, nil
	default:
		log.Println(reflect.TypeOf(expr), "is not yet implemented")

	}
	return nil, errors.New("not yet implemented")
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
