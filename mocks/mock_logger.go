package mocks

import (
	"bytes"
	"log"
)

type MockLogger struct {
	buf bytes.Buffer
}

func (m *MockLogger) Write(i []byte) error {
	log.Println("MockLogger", string(i))

	_, err := m.buf.Write(i)
	return err
}

func (m *MockLogger) Output() string {
	return m.buf.String()
}
