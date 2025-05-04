package internal

import (
	"errors"
	"github.com/saido-labs/idle/api"
	"github.com/saido-labs/idle/mocks"
	"github.com/saido-labs/idle/model"
	"log"
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

func Test_PipelineToStdout(t *testing.T) {
	output := &mocks.MockLogger{}

	cfg := api.PipelineConfig{
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
			api.NewPipelineStep("input.parser", api.NewMessageParser(), model.RowSchema{
				Column: []string{"$.content", "$.author_name"},
				Types:  []string{"STRING", "STRING"},
			}),

			// where author name is Felix
			// pass along the content and author_name fields
			api.NewPipelineStep("author.filter", api.NewQuery("SELECT content, author_name WHERE author_name = 'felix angell'"), model.RowSchema{
				Column: []string{"content", "author_name"},
				Types:  []string{"STRING", "STRING"},
			}),

			// we need to alias with a name for mapping against a schema output?
			api.NewPipelineStep("article.join", api.NewQuery("SELECT concat(2, ':', 1)"), model.RowSchema{
				Column: []string{"result"},
				Types:  []string{"STRING"},
			}),

			api.NewPipelineStep("output.processor", api.NewJsonEncoder(), model.RowSchema{
				Column: []string{"result"},
				Types:  []string{"STRING"},
			}),
		},

		Parallelism: 4,
	}

	Start(cfg, 1*time.Second)

	result := output.Output()

	log.Println("Result is", result)
}
