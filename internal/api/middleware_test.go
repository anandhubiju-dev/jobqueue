package api

import (
	"strings"
	"testing"
)

func TestValidRequestID(t *testing.T) {
	ok := []string{"abc", "A-1_b.2", strings.Repeat("a", 64)}
	bad := []string{"", strings.Repeat("a", 65), "has space", "new\nline", "emoji\U0001F600", "semi;colon", `quo"te`}

	for _, s := range ok {
		if !validRequestID(s) {
			t.Errorf("%q should be valid", s)
		}
	}
	for _, s := range bad {
		if validRequestID(s) {
			t.Errorf("%q should be invalid", s)
		}
	}
}
