package internal

import (
	"context"
	"errors"
	"github.com/saido-labs/idle/api"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func Start(pipeline api.Pipeline, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Println("Starting the pipeline")
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
