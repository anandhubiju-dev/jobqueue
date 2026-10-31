package api

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

// RegisterHealth installs liveness and readiness endpoints.
func RegisterHealth(r *gin.Engine, db, queue Pinger) {
	// Liveness: "is this process able to answer at all?"
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Readiness: "can this instance actually do work right now?"
	r.GET("/readyz", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()

		checks := gin.H{}
		ready := true
		for name, p := range map[string]Pinger{"postgres": db, "queue": queue} {
			if err := p.Ping(ctx); err != nil {
				checks[name] = "unavailable"
				ready = false
			} else {
				checks[name] = "ok"
			}
		}

		status := http.StatusOK
		if !ready {
			status = http.StatusServiceUnavailable
		}
		c.JSON(status, gin.H{"ready": ready, "checks": checks})
	})
}
