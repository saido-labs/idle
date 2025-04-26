package main

import (
	"github.com/saido-labs/idle/api"
	"github.com/saido-labs/idle/internal"
)

func main() {
	internal.Start(api.Pipeline{})
}
