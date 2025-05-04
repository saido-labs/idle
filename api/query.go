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

func (q *Query) Process(p *Pipeline, in, out RowSchema, msg model.Message) (model.Message, error) {
	incomingMessage, err := RowDataFromBlob(msg.Data)
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
		rootExpr, err := evaluateRootLevelExpr(in, value, incomingMessage)
		if err != nil {
			log.Fatalf("Query(%s): Error: %v\n", q.query, err.Error())
		}

		rd.SetColumn(idx, rootExpr)
	}

	message := model.NewMessage(RowDataToBlob(rd))

	// compute if we filter or not for this
	// specific row.
	if filterExpr := q.GetEvaluation().Filter; filterExpr != nil {
		res, err := evaluateRootLevelExpr(in, filterExpr, incomingMessage)
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

	return message, nil
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
