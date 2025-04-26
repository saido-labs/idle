package mocks

import (
	"bytes"
)

type MockLogger struct {
	buf bytes.Buffer
}

func (m *MockLogger) Write(i []byte) error {
	_, err := m.buf.Write(i)
	return err
}

func (m *MockLogger) Output() string {
	return m.buf.String()
}
