package internal

import (
	"context"
	"encoding/gob"
	"errors"
	"github.com/saido-labs/idle/api"
	"github.com/saido-labs/idle/model"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func init() {
	gob.Register([]interface{}{})
	gob.Register(model.RowData{Values: []interface{}{}})
	gob.Register(api.Function{})
	gob.Register(api.RowIdentifier{})
}

func Start(cfg api.PipelineConfig, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	pipeline := api.Pipeline{
		Config: cfg,
	}
	go func() {
		log.Println("Starting pipeline")
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
