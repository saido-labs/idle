package internal

import (
	"errors"
	"github.com/saido-labs/idle/api"
	"github.com/saido-labs/idle/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type mockedSource struct {
	messages []string
}

func (m *mockedSource) Read() ([]byte, error) {
	if len(m.messages) == 0 {
		return []byte{}, errors.New("EOS")
	}

	msg := m.messages[0]
	m.messages = m.messages[1:]

	return []byte(msg), nil
}

func Test_PipelineToStdout_BasicMapAndFilter(t *testing.T) {
	output := &mocks.MockLogger{}

	cfg := api.Pipeline{
		// we only want one input but allow for multiple
		// if we have something like a re-try

		Input: []api.Source{
			&mockedSource{
				messages: []string{
					`{ "content": "hello!", "author_name": "felix angell" }`,

					// fixme: felix error handling strat for missing keys.
					//`{ "content": "foo" }`,
					//`{ "content": "bar" }`,

					`{ "content": "baz", "author_name": "john doe" }`,
					`{ "content": "toast", "author_name": "john doe" }`,
				},
			},
		},

		Output: output,

		// simple processor to take the first char
		Processors: []api.PipelineStep{
			api.NewPipelineStep("input.parser", api.NewJqMessageParser(), api.RowSchema{
				Column: []string{"$.content", "$.author_name"},
				Types:  []api.Type{api.TypeString, api.TypeString},
			}),

			// where author name is Felix
			// pass along the content and author_name fields
			api.NewPipelineStep("author.filter", api.NewQuery("SELECT content, author_name WHERE author_name = 'felix angell'"), api.RowSchema{
				Column: []string{"content", "author_name"},
				Types:  []api.Type{api.TypeString, api.TypeString},
			}),

			// we need to alias with a name for mapping against a schema output?
			api.NewPipelineStep("article.join", api.NewQuery("SELECT concat(2, ':', 1)"), api.RowSchema{
				Column: []string{"result"},
				Types:  []api.Type{api.TypeString},
			}),

			api.NewPipelineStep("output.processor", api.NewJsonEncoder(), api.RowSchema{
				Column: []string{"result"},
				Types:  []api.Type{api.TypeString},
			}),
		},

		Parallelism: 4,
	}

	Start(cfg, 1*time.Second)

	result := output.Output()

	assert.Equal(t, `["felix angell:hello!"]`, result)
}
