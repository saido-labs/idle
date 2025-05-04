package api

import (
	"errors"
	"github.com/saido-labs/idle/model"
	"io"
	"log"
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

func (q *Query) Process(p *Pipeline, schema RowSchema, msg model.Message) (model.Message, error) {
	in, err := RowDataFromBlob(msg.Data)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			log.Fatalf("Query(%s): Error: %v\n", q.query, err.Error())
			return model.Message{}, err
		}
		return model.Message{}, nil
	}

	rd := &Row{
		Values: make([]Value, len(q.GetEvaluation().Reads)),
	}

	for idx, value := range q.GetEvaluation().Reads {
		rootExpr, err := evaluateRootLevelExpr(schema, value, in)
		if err != nil {
			log.Fatalf("Query(%s): Error: %v\n", q.query, err.Error())
		}
		
		rd.SetColumn(idx, rootExpr)
	}

	out := model.NewMessage(RowDataToBlob(rd))

	// compute if we filter or not for this
	// specific row.
	if filterExpr := q.GetEvaluation().Filter; filterExpr != nil {
		res, err := evaluateRootLevelExpr(schema, filterExpr, in)
		if err != nil {
			log.Fatalf("Query(%s): Error: %v\n", q.query, err.Error())
		}

		if res == False {
			return model.Message{}, nil
		} else {
			// filtered out
			log.Printf("Query(%s): Filter expression false\n", q.query)
		}
	}

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
