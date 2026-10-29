package main

import (
	"context"

	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/anandhubiju-dev/jobqueue/internal/api"
	"github.com/anandhubiju-dev/jobqueue/internal/queue"
	"github.com/anandhubiju-dev/jobqueue/internal/scheduler"
	"github.com/anandhubiju-dev/jobqueue/internal/service"
	"github.com/anandhubiju-dev/jobqueue/internal/store"
	"github.com/anandhubiju-dev/jobqueue/internal/worker"
	"github.com/gin-gonic/gin"
)

func main() {
	host, _ := os.Hostname()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("instance", host)
	slog.SetDefault(logger)

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

	var q queue.Queue
	switch os.Getenv("QUEUE") {
	case "redis":
		rq, err := queue.NewRedis(context.Background(), os.Getenv("REDIS_URL"), 100)
		if err != nil {
			log.Fatal(err)
		}
		defer rq.Close()
		q = rq
		slog.Info("using redis queue")
	default:
		q = queue.NewInMemory(100)
		slog.Info("using in-memory queue")
	}

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

	sched := scheduler.New(st, q, time.Second) // NEW
	wg.Add(1)
	go func() {
		defer wg.Done()
		sched.Run(ctx)
	}()

	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	api.New(svc).Register(r)

	srv := &http.Server{Addr: ":8080", Handler: r}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown failed", "err", err)
	}

	wg.Wait()
	slog.Info("shutdown complete")
}
