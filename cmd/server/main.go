package main

import (
	"context"

	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/anandhubiju-dev/jobqueue/internal/api"
	"github.com/anandhubiju-dev/jobqueue/internal/queue"
	"github.com/anandhubiju-dev/jobqueue/internal/service"
	"github.com/anandhubiju-dev/jobqueue/internal/store"
	"github.com/anandhubiju-dev/jobqueue/internal/worker"
	"github.com/gin-gonic/gin"
)

func main() {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	st, err := store.New(dbCtx, dbURL)
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
			w.Run(ctx)
		}()
	}

	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	api.New(svc).Register(r)

	srv := &http.Server{Addr: ":8080", Handler: r}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server: %v", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}

	wg.Wait()
	log.Println("shutdown complete")
}
