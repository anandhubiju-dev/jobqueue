package api

import (
	"context"
	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const requestIDHeader = "X-Request-ID"

type ctxKey struct{}

// RequestID returns the request ID stored in ctx, or "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

func validRequestID(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
		default:
			return false
		}
	}
	return true
}

func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(requestIDHeader)
		if !validRequestID(id) {
			id = uuid.NewString()
		}
		c.Header(requestIDHeader, id)
		ctx := context.WithValue(c.Request.Context(), ctxKey{}, id)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

func reqLog(ctx context.Context) *slog.Logger {
	return slog.With("request_id", RequestID(ctx))
}
