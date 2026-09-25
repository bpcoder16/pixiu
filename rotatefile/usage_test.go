package rotatefile_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/rotatefile"
)

func TestFileCanBeUsedWithoutLogit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	f, err := rotatefile.New(path, rotatefile.OptEvery(24*time.Hour), rotatefile.OptMaxFiles(3))
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.Write([]byte("standalone\n")); err != nil || n != len("standalone\n") {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "standalone\n" {
		t.Fatalf("stable path = %q, %v", got, err)
	}
}
