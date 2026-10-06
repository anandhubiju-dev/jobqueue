package main

import (
	"context"
	"sync"

	"log"
	"net/http"
	"os"
	"time"

	"github.com/anandhubiju-dev/jobqueue/internal/api"
	"github.com/anandhubiju-dev/jobqueue/internal/queue"
	"github.com/anandhubiju-dev/jobqueue/internal/service"
	"github.com/anandhubiju-dev/jobqueue/internal/store"
	"github.com/anandhubiju-dev/jobqueue/internal/worker"
	"github.com/gin-gonic/gin"
)

func main() {

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	st, err := store.New(ctx, dbURL)
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	q := queue.NewInMemory(100)
	svc := service.New(st, q)

	const numWorkers = 4

	var wg sync.WaitGroup
	for i := 1; i <= numWorkers; i++ {
		w := worker.New(i, q, st)
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Run(context.Background())
		}()
	}

	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	api.New(svc).Register(r)

	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Failed to run the server : %v", err)
	}
}
