package internal

import (
	"errors"
	"github.com/saido-labs/idle/api"
	"github.com/saido-labs/idle/mocks"
	"github.com/saido-labs/idle/model"
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

func Test_PipelineToStdout(t *testing.T) {
	output := &mocks.MockLogger{}

	schema := model.RowSchema{
		Name:   "article",
		Column: []string{"$.content"},
		Types:  []string{"STRING"},
	}

	cfg := api.PipelineConfig{
		// we only want one input but allow for multiple
		// if we have something like a re-try

		Input: []api.Source{
			&mockedSource{
				messages: []string{
					`{ "content": "hello!", "title": "test" }`,
					`{ "content": "world" }`,
				},
			},
		},

		Output: output,

		Schemas: []model.RowSchema{schema},

		// simple processor to take the first char
		Processors: []api.Processor{
			api.NewMessageParser(schema),

			//api.NewQuery("SELECT SUBSTRING(word from 1 for 1)"),
			//api.NewQuery("SELECT SUBSTRING(word, 1, 1)"),
			api.NewQuery("SELECT content"),
			//api.NewQuery("SELECT LEFT(content, 1) as char, content as old_content"),

			api.NewJsonEncoder(),
		},

		Parallelism: 4,
	}

	Start(cfg, 1*time.Second)

	assert.Equal(t, "helloworld!", output.Output())
}
