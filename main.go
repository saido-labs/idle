package main

import (
	"github.com/saido-labs/idle/api"
	"github.com/saido-labs/idle/internal"
)

func main() {
	// run forever...
	internal.Start(api.Pipeline{}, 0)
}
