package logit_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/bpcoder16/pixiu/rotatefile"
)

func TestExternalRotateFileAsLogitWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	f, err := rotatefile.New(path)
	if err != nil {
		t.Fatal(err)
	}
	w := logit.NewWriter(f)
	l := logit.MustNew(logit.OptWriter(w))
	l.Info(context.Background(), "external rotation")
	if err := logit.Close(l); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || !strings.Contains(string(got), "external rotation") {
		t.Fatalf("stable path = %q, %v", got, err)
	}
}
