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

func Test_PipelineToStdout(t *testing.T) {
	output := &mocks.MockLogger{}

	Start(api.Pipeline{
		Input: &mockedSource{
			messages: []string{
				"hello",
				"world",
			},
		},
		Output:      output,
		Parallelism: 4,
	}, 1*time.Second)

	assert.Equal(t, "helloworld", output.Output())
}
