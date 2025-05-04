package api

import (
	"github.com/saido-labs/idle/model"
)

// Query represents an ANSI(?) SQL
// statement
type Query struct {
	query    string
	evalTree *Evaluation
}

func NewQuery(query string) *Query {
	res := &Query{
		query: query,
	}
	res.buildEvalTree()
	return res
}

func (q *Query) Process(p *Pipeline, schema model.RowSchema, msg model.Message) (model.Message, error) {
	in := RowDataFromBlob(msg.Data)

	rd := &Row{
		Values: make([]Value, len(q.GetEvaluation().Reads)),
	}

	for idx, value := range q.GetEvaluation().Reads {
		rd.SetColumn(idx, evaluate(schema, value, in))
	}

	out := model.NewMessage(RowDataToBlob(rd))
	return out, nil
}

func (q *Query) GetEvaluation() *Evaluation {
	if q.evalTree == nil {
		q.evalTree = q.buildEvalTree()
	}
	return q.evalTree
}

func (q *Query) buildEvalTree() *Evaluation {
	evaluator := Evaluator{}
	return evaluator.Eval(q.query)
}
