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
	return &Query{
		query: query,
	}
}

func (q *Query) Process(p *Pipeline, msg model.Message) (model.Message, error) {
	// do something with the eval tree
	return msg, nil
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
