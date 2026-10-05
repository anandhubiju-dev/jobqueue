package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/anandhubiju-dev/jobqueue/internal/job"
	"github.com/anandhubiju-dev/jobqueue/internal/service"
)

type Handler struct {
	svc *service.Service
}

func New(svc *service.Service) *Handler {
	return &Handler{svc: svc}
}

type createJobRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func (h *Handler) createJob(c *gin.Context) {
	var req createJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "malformed JSON body"})
		return
	}

	j, err := h.svc.Create(c.Request.Context(), req.Type, req.Payload)
	if errors.Is(err, service.ErrInvalidJob) {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
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
