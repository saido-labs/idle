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

func Test_PipelineToStdout_BasicMapAndFilter_GracefulErrors(t *testing.T) {
	output := &mocks.MockLogger{}
	sideOutput := &mocks.MockLogger{}

	cfg := api.Pipeline{
		// we only want one input but allow for multiple
		// if we have something like a re-try

		Input: []api.Source{
			&mockedSource{
				messages: []string{
					`{ "content": "foo" }`,
				},
			},
		},

		Output:     output,
		SideOutput: sideOutput,

		Processors: []api.PipelineStep{
			api.NewPipelineStep("input.parser", api.NewJqMessageParser(api.NewJqParams(map[string]string{
				"content":     "$.content",
				"author_name": "$.author_name",
			})), api.RowSchema{
				Column: []string{"content", "author_name"},
				Types:  []api.Type{api.TypeString, api.TypeString},
			}),

			// where author name is Felix
			// pass along the content and author_name fields
			api.NewPipelineStep("author.filter", api.NewQuery("SELECT author_name WHERE author_name = 'felix angell'"), api.RowSchema{
				Column: []string{"content", "author_name"},
				Types:  []api.Type{api.TypeString, api.TypeString},
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
	assert.Emptyf(t, result, "Expected output to be empty.")

	result = sideOutput.Output()
	assert.Equal(t, `{ "content": "foo" }`, result)
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
			api.NewPipelineStep("input.parser", api.NewJqMessageParser(api.NewJqParams(map[string]string{
				"content":     "$.content",
				"author_name": "$.author_name",
			})), api.RowSchema{
				Column: []string{"content", "author_name"},
				Types:  []api.Type{api.TypeString, api.TypeString},
			}),

			// where author name is Felix
			// pass along the content and author_name fields
			api.NewPipelineStep("author.filter", api.NewQuery("SELECT content, author_name WHERE author_name = 'felix angell'"), api.RowSchema{
				// can we extract these from postgres casts or interpret them otherwise?
				Column: []string{"content", "author_name"},
				Types:  []api.Type{api.TypeString, api.TypeString},
			}),

			// we need to alias with a name for mapping against a schema output?
			// e.g. concat() as result
			api.NewPipelineStep("article.join", api.NewQuery("SELECT concat(content, ':', upper(author_name))"), api.RowSchema{
				Column: []string{"result"},
				Types:  []api.Type{api.TypeString},
			}),

			api.NewPipelineStep("output.processor", api.NewJsonEncoder(), api.RowSchema{
				Column: []string{"result"},
				Types:  []api.Type{api.TypeString},
			}),
		},

		Parallelism: 1,
	}

	Start(cfg, 1*time.Second)

	result := output.Output()

	assert.Equal(t, `["hello!:FELIX ANGELL"]`, result)
}
