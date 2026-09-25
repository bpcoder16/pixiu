//go:build darwin || linux

package logit

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHookStderrCapturesWrites(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	saved, err := saveFd(2)
	if err != nil {
		t.Fatal(err)
	}
	// 不用 t.Cleanup:必须先恢复 fd 2,否则它一直占用管道写端,ReadAll 等不到 EOF
	defer func() {
		_ = restoreFd(saved, 2)
		_ = closeFd(saved)
	}()

	// 用 NewWriter 适配管道写端，保留 Fd 透传能力。
	if err := HookStderr(NewWriter(w)); err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(os.Stderr, "captured via hook")

	// 先恢复再读:fd 2 归还原始 stderr,管道写端仅剩 w,关闭后 ReadAll 得到 EOF
	if err := restoreFd(saved, 2); err != nil {
		t.Fatal(err)
	}
	if err := closeFd(saved); err != nil {
		t.Fatal(err)
	}
	w.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if !strings.Contains(string(data), "captured via hook") {
		t.Errorf("hook did not capture stderr write: %q", data)
	}
}

func TestHookStdoutWithRotateFile(t *testing.T) {
	dir := t.TempDir()
	rf, err := NewRotateFile(filepath.Join(dir, "stdout.log"))
	if err != nil {
		t.Fatal(err)
	}

	saved, err := saveFd(1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = restoreFd(saved, 1)
		_ = closeFd(saved)
	})

	if err := HookStdout(rf); err != nil {
		t.Fatalf("rotateFile should expose Fd: %v", err)
	}
	fmt.Println("stdout goes to file")

	// 恢复后再读文件,避免与劫持态竞争
	if err := restoreFd(saved, 1); err != nil {
		t.Fatal(err)
	}
	if err := closeFd(saved); err != nil {
		t.Fatal(err)
	}

	if err := rf.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "stdout.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "stdout goes to file") {
		t.Errorf("rotated stdout file missing content: %q", data)
	}
}

func TestHookRequiresFd(t *testing.T) {
	if err := HookStderr(NewWriter(&bytes.Buffer{})); err == nil {
		t.Error("bytes.Buffer-backed writer has no Fd, hook should fail")
	}
}

func TestHookWithPlainFile(t *testing.T) {
	for _, constructor := range []string{"OpenFile", "NewWriter"} {
		for _, stream := range []struct {
			name string
			fd   int
			file *os.File
			hook func(Writer) error
		}{
			{"stdout", 1, os.Stdout, HookStdout},
			{"stderr", 2, os.Stderr, HookStderr},
		} {
			t.Run(constructor+"/"+stream.name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "std.log")
				var w Writer
				var err error
				if constructor == "OpenFile" {
					w, err = OpenFile(path)
				} else {
					var f *os.File
					f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
					if err == nil {
						w = NewWriter(f)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = w.Close() })
				// 在检查文件或报告结果前恢复进程 fd，失败分支也只恢复一次。
				func() {
					saved, err := saveFd(stream.fd)
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := restoreFd(saved, stream.fd); err != nil {
							t.Error(err)
						}
						if err := closeFd(saved); err != nil {
							t.Error(err)
						}
					}()
					if err := stream.hook(w); err != nil {
						t.Fatal(err)
					}
					if _, err := fmt.Fprintln(stream.file, "captured plain file"); err != nil {
						t.Fatal(err)
					}
				}()
				if data, err := os.ReadFile(path); err != nil || string(data) != "captured plain file\n" {
					t.Fatalf("redirected output: %q, %v", data, err)
				}
			})
		}
	}
}
