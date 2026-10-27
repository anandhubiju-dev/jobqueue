package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/anandhubiju-dev/jobqueue/internal/job"
	"github.com/anandhubiju-dev/jobqueue/internal/queue"
	"github.com/anandhubiju-dev/jobqueue/internal/service"
)

type Handler struct {
	svc *service.Service
}

func New(svc *service.Service) *Handler {
	return &Handler{svc: svc}
}

type createJobRequest struct {
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload"`
	DelaySeconds *int            `json:"delay_seconds"`
	RunAt        *time.Time      `json:"run_at"`
}

func (h *Handler) createJob(c *gin.Context) {
	var req createJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "malformed JSON body"})
		return
	}

	if req.DelaySeconds != nil && req.RunAt != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "use either delay_seconds or run_at, not both"})
		return
	}

	runAt := time.Now()
	switch {
	case req.DelaySeconds != nil:
		d := *req.DelaySeconds
		if d < 0 || d > 365*24*3600 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "delay_seconds must be between 0 and 31536000"})
			return
		}
		runAt = runAt.Add(time.Duration(d) * time.Second)
	case req.RunAt != nil:
		runAt = *req.RunAt
	}

	key := c.GetHeader("Idempotency-Key")
	j, err := h.svc.Create(c.Request.Context(), req.Type, req.Payload, key, runAt)
	if errors.Is(err, service.ErrInvalidJob) {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if errors.Is(err, queue.ErrFull) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "server busy, retry later"})
		return
	}

	if err != nil {
		log.Printf("create job: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"id": j.ID, "status": j.Status})
}

func (h *Handler) getJob(c *gin.Context) {
	j, err := h.svc.Get(c.Request.Context(), c.Param("id"))
	if errors.Is(err, job.ErrNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	if err != nil {
		log.Printf("get job: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	c.JSON(http.StatusOK, j)
}

func (h *Handler) Register(r *gin.Engine) {
	r.POST("/jobs", h.createJob)
	r.GET("/jobs/:id", h.getJob)
}
