package internal

import (
	"context"
	"encoding/gob"
	"errors"
	"github.com/saido-labs/idle/api"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func init() {
	gob.Register([]interface{}{})
	gob.Register(api.Row{Values: []api.Value{}})
	gob.Register(api.Function{})
	gob.Register(api.RowIdentifier{})
	gob.Register(&api.StringValue{})
	gob.Register(&api.IntegerValue{})
	gob.Register(&api.FloatValue{})
}

func Start(pipeline api.Pipeline, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		pipeline.Start()
	}()

	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			log.Println("Timeout reached, shutting down...")
		} else {
			log.Println("Pipeline completed successfully")
		}
	case sig := <-sigChan:
		log.Printf("Received signal: %s, shutting down...\n", sig)
	}
}
