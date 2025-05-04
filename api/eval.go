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
	Filters Value

	// this is probably quite hard to pull off.
	Joins []any
}

type Type int

const (
	TypeUnspecified Type = iota
	TypeString
	TypeInteger
	TypeFloat
	TypeBoolean
)

type Value interface {
	Cast(target Type) Value
	GetType() Type
	Equals(other Value) bool
}

type BinaryExpr struct {
	Operator    string
	Left, Right Value
}

func (e *BinaryExpr) Equals(other Value) bool {
	panic("implement me")
}

func (e *BinaryExpr) GetType() Type {
	return e.Left.GetType()
}

func (e *BinaryExpr) Cast(target Type) Value {
	panic("illegal state")
}

type BooleanValue struct {
	Value bool
}

func (b BooleanValue) Equals(other Value) bool {
	panic("implement me")
}

func (b BooleanValue) Cast(target Type) Value {
	panic("implement me")
}

func (b BooleanValue) GetType() Type {
	return TypeBoolean
}

var (
	True  BooleanValue = BooleanValue{true}
	False              = BooleanValue{false}
)

type IntegerValue struct {
	Value int64
}

func (v *IntegerValue) Equals(other Value) bool {
	panic("implement me")
}

func (i IntegerValue) GetType() Type {
	return TypeInteger
}

func (i IntegerValue) Cast(target Type) Value {
	panic("implement me")
}

type FloatValue struct {
	Value float64
}

func (v *FloatValue) Equals(other Value) bool {
	panic("implement me")
}

func (i FloatValue) GetType() Type {
	return TypeFloat
}

func (i FloatValue) Cast(target Type) Value {
	panic("implement me")
}

type StringValue struct {
	Value string
}

func (v *StringValue) Equals(other Value) bool {
	switch t := other.(type) {
	case *StringValue:
		return v.Value == t.Value
	default:
		return false
	}
}

func (i StringValue) GetType() Type {
	return TypeString
}

func (i *StringValue) Cast(target Type) Value {
	switch target {
	case TypeString:
		return i
	default:
		panic("not yet implemented")
	}
}

type RowIdentifier struct {
	Name string
}

func (v *RowIdentifier) Equals(other Value) bool {
	panic("implement me")
}

func (i RowIdentifier) GetType() Type {
	panic("implement me")
}

func (i RowIdentifier) Cast(target Type) Value {
	panic("implement me")
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

func (v *Function) Equals(other Value) bool {
	panic("implement me")
}

func (f *Function) GetType() Type {
	// FIXME?
	return TypeUnspecified
}

func (f *Function) Cast(Type) Value {
	panic("illegal state")
}

func NewFunction(name string, params []Value) *Function {
	return &Function{name, params}
}

// ----

type Evaluator struct{}

func (q *Evaluator) parseValue(param *sqlparser.SQLVal) Value {
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
		Filters: nil,
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
		eval.Filters = whereExpr
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

func evaluateRowIdentifier(schema RowSchema, e *RowIdentifier, in *Row) Value {
	// return the value from the message
	idx := schema.ColIndex(e.Name)
	if idx == -1 {
		panic("no column found " + e.Name)
	}
	return in.GetColumn(idx)
}

func evaluateFunc(schema RowSchema, e *Function, in *Row) Value {
	switch fnName := strings.ToLower(e.Name); fnName {
	case "concat":
		return concat(schema, e, in)
	default:
		panic(fnName + " is not yet implemented")
	}
}

// depends on context? in a select
// this means grab the column at e
func evaluateIntegerValue(_ RowSchema, e *IntegerValue, in *Row) Value {
	// downsizing edge-case.
	index := int(e.Value)

	// the queries are not 0-indexed but 1-indexed
	// so we have to normalize them
	adjustedIndex := index - 1

	return in.GetColumn(adjustedIndex)
}

func evaluateConstant(schema RowSchema, e *StringValue, in *Row) Value {
	return e
}

func evaluateBinaryExpr(schema RowSchema, expr *BinaryExpr, in *Row) Value {
	lhand := evaluate(schema, expr.Left, in)
	rhand := evaluate(schema, expr.Right, in)

	switch expr.Operator {
	case "=":
		return BooleanValue{
			Value: lhand.Equals(rhand),
		}

	default:
		panic("not yet implemented")
	}
}

func evaluate(schema RowSchema, e Value, in *Row) Value {
	switch e := e.(type) {
	case *Function:
		return evaluateFunc(schema, e, in)
	case *BinaryExpr:
		return evaluateBinaryExpr(schema, e, in)
	case *RowIdentifier:
		return evaluateRowIdentifier(schema, e, in)
	case *IntegerValue:
		return evaluateIntegerValue(schema, e, in)
	case *StringValue:
		return evaluateConstant(schema, e, in)
	default:
		log.Println(reflect.TypeOf(e), "not yet implemented")
		panic("not yet handled")
	}
}
