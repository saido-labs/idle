package api

import (
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/saido-labs/idle/model"
	"github.com/xwb1989/sqlparser"
	"log"
)

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

func (q *Query) Process(p *Pipeline, msg model.Row) (model.Row, error) {
	// take the msg and do something with it based on the query
	stmt, err := sqlparser.Parse(q.query)
	if err != nil {
		return model.Row{}, err
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return q.processSelect(p, stmt, msg)
	case *sqlparser.Insert:
	}

	return msg, nil
}

func (q *Query) processSelect(p *Pipeline, stmt *sqlparser.Select, msg model.Row) (model.Row, error) {
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
				// TODO(FELIX): Implement this.
				log.Println("Function:", expr.Name, expr.Qualifier, expr.Distinct, expr.Exprs)
				return model.Row{}, errors.New("functions not yet implemented")
			case *sqlparser.SubstrExpr:
				return model.Row{}, errors.New("substring not implemented")
			}
		default:
			return model.Row{}, errors.New("not implemented")
		}
	}

	var buff bytes.Buffer
	encoder := gob.NewEncoder(&buff)
	if err := encoder.Encode(record); err != nil {
		return model.Row{}, err
	}

	return model.RowFromBlob(buff.Bytes()), nil
}

func (q *Query) indexOfColumn(p *Pipeline, column string) int {
	log.Println("Index lookup for", column)
	return 0
}
