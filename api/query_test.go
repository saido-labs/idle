package api

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// TODO float values
// TODO string values

func TestQuery_BuildEvalTree_SingleIdentifierRead(t *testing.T) {
	query := NewQuery("SELECT content")

	res := query.GetEvaluation()
	assert.NotNil(t, res)
	assert.NotEmpty(t, res.Reads)

	read := res.Reads[0]
	assert.NotNil(t, read)

	ri, ok := read.(*RowIdentifier)
	assert.True(t, ok)

	assert.Equal(t, "content", ri.Name)
}

func TestQuery_BuildEvalTree_SelectWithFunction_AndFilter(t *testing.T) {
	query := NewQuery("SELECT content, author WHERE author = 'felix angell'")

	res := query.GetEvaluation()
	assert.NotNil(t, res)
	assert.Len(t, res.Reads, 2)

	assert.NotEmptyf(t, res.Filters, "Expected a filter by author name")

	comp, ok := res.Filters[0].(*BinaryExpr)
	assert.True(t, ok)

	assert.Equal(t, "=", comp.Operator)
	assert.Equal(t, NewRowIdentifier("author"), comp.Left)
	assert.Equal(t, &StringValue{Value: "felix angell"}, comp.Right)
}

func TestQuery_BuildEvalTree_SelectWithFunction(t *testing.T) {
	query := NewQuery("SELECT LEFT(content, 2)")

	res := query.GetEvaluation()
	assert.NotNil(t, res)
	assert.NotEmpty(t, res.Reads)

	// read is LEFT(content, 1)
	read := res.Reads[0]
	assert.NotNil(t, read)

	ri, ok := read.(*Function)
	assert.True(t, ok)
	assert.Len(t, ri.Params, 2)

	assert.Equal(t, "left", ri.Name)

	iden, ok := ri.Params[0].(*RowIdentifier)
	assert.True(t, ok)
	assert.Equal(t, "content", iden.Name)

	param, ok := ri.Params[1].(*IntegerValue)
	assert.True(t, ok)
	assert.Equal(t, int64(2), param.Value)
}
