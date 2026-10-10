//go:build !darwin && !linux

package bootstrap_test

import "testing"

func requireLogFileOpen(t *testing.T, _ string, _ bool) {
	t.Helper()
	t.Skip("文件句柄检查仅支持 macOS/Linux")
}
