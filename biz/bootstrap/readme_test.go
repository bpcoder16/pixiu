package bootstrap_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestREADMEExamplesCompile(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, block := range strings.Split(string(data), "```go\n")[1:] {
		source, _, closed := strings.Cut(block, "\n```")
		if !closed {
			t.Fatal("README Go 代码块缺少结束标记")
		}
		// 只编译可直接复制运行的完整程序，API 片段不独立构建。
		if !strings.HasPrefix(strings.TrimSpace(source), "package main\n") {
			continue
		}
		count++
		t.Run(fmt.Sprintf("example_%d", count), func(t *testing.T) {
			dir := t.TempDir()
			file := filepath.Join(dir, "main.go")
			if err := os.WriteFile(file, []byte(source), 0o600); err != nil {
				t.Fatal(err)
			}
			// 从仓库根目录构建临时源码，使用当前工作区实现；不执行示例的副作用。
			cmd := exec.CommandContext(t.Context(), "go", "build", "-o", filepath.Join(dir, "example"), file)
			cmd.Dir = root
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("README 完整示例编译失败: %v\n%s", err, output)
			}
		})
	}
	if count == 0 {
		t.Fatal("README 中未找到完整 Go 示例")
	}
}
