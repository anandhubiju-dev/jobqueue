package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestIsPermanent(t *testing.T) {
	base := errors.New("bad payload")

	if IsPermanent(base) {
		t.Fatal("plain error should be transient")
	}
	if !IsPermanent(Permanent(base)) {
		t.Fatal("marked error should be permanent")
	}
	if !IsPermanent(fmt.Errorf("handling: %w", Permanent(base))) {
		t.Fatal("marker should survive wrapping")
	}
	if !errors.Is(Permanent(base), base) {
		t.Fatal("marker should keep the original cause reachable")
	}
	if IsPermanent(context.DeadlineExceeded) {
		t.Fatal("timeout are transient")
	}

}
