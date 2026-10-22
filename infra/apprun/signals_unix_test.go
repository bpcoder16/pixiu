//go:build unix

package apprun

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/lifecycle"
)

func TestProcessSignals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		first  os.Signal
		second bool
	}{
		{
			name:  "SIGINT平滑关闭",
			first: syscall.SIGINT,
		},
		{
			name:  "SIGTERM平滑关闭",
			first: syscall.SIGTERM,
		},
		{
			name:   "第二次SIGINT直接终止",
			first:  syscall.SIGINT,
			second: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSignalChild$")
			cmd.Env = append(os.Environ(), "PIXIU_APPRUN_SIGNAL_CHILD=1")
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer stdin.Close()
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer cmd.Process.Kill()
			lines := make(chan string, 32)
			go func() {
				defer close(lines)
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					lines <- scanner.Text()
				}
			}()
			waitLine(t, lines, "ready")
			if err := cmd.Process.Signal(tc.first); err != nil {
				t.Fatal(err)
			}
			waitLine(t, lines, "draining")
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			if tc.second {
				if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
					t.Fatal(err)
				}
				err := await(t, done)
				exit, ok := err.(*exec.ExitError)
				if !ok {
					t.Fatalf("第二次中断未终止子进程: %v", err)
				}
				status := exit.Sys().(syscall.WaitStatus)
				if !status.Signaled() || status.Signal() != syscall.SIGINT {
					t.Fatalf("退出原因不是第二次中断: %v", status)
				}
				return
			}
			select {
			case err := <-done:
				t.Fatalf("收到信号后未等待排空: %v", err)
			case <-time.After(30 * time.Millisecond):
			}
			if _, err := fmt.Fprintln(stdin, "complete"); err != nil {
				t.Fatal(err)
			}
			if err := await(t, done); err != nil {
				t.Fatalf("正常停机返回错误: %v", err)
			}
		})
	}
}

func TestSignalChild(t *testing.T) {
	if os.Getenv("PIXIU_APPRUN_SIGNAL_CHILD") != "1" {
		return
	}
	var manager Manager
	stop := make(chan struct{})
	register(t, &manager, Service{
		Name: "service",
		Run: func() error {
			fmt.Println("ready")
			<-stop
			return nil
		},
		Stop: func() error {
			fmt.Println("draining")
			_, err := bufio.NewReader(os.Stdin).ReadString('\n')
			close(stop)
			return err
		},
		Wait: func(context.Context) error {
			return nil
		},
		Cancel: func() {},
	})
	if err := manager.RunSignals(); err != nil {
		t.Fatal(err)
	}
}

func waitLine(t *testing.T, lines <-chan string, want string) {
	t.Helper()
	for {
		line, ok := <-lines
		if !ok {
			t.Fatalf("子进程提前退出，未收到 %q", want)
		}
		if line == want {
			return
		}
	}
}

// 子进程故意留下不响应取消的工作与关闭回调，验证入口仍能按两个期限退出。
func TestProcessExitsAfterDrainAndCleanupTimeout(t *testing.T) {
	const childEnv = "PIXIU_APPRUN_BOUNDED_EXIT_CHILD"
	if os.Getenv(childEnv) == "1" {
		manager, err := New(40 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		started, canceled := make(chan struct{}), make(chan struct{})
		register(t, manager, Service{
			Name: "unresponsive",
			Run: func() error {
				close(started)
				select {}
			},
			Stop: func() error { return nil },
			Wait: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			Cancel: func() { close(canceled) },
		})
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-started
			cancel()
		}()
		if err := manager.Run(ctx); !errors.Is(err, ErrShutdownTimeout) {
			t.Fatalf("排空未超时: %v", err)
		}
		select {
		case <-canceled:
		default:
			t.Fatal("未取消剩余服务")
		}
		resources, err := lifecycle.New(40 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		closing := make(chan struct{})
		_ = resources.Register(func() error {
			close(closing)
			select {}
		})
		if err := resources.Close(); !errors.Is(err, lifecycle.ErrCloseTimeout) {
			t.Fatalf("资源回收未超时: %v", err)
		}
		select {
		case <-closing:
		default:
			t.Fatal("排空超时后跳过了资源回收")
		}
		os.Exit(0)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessExitsAfterDrainAndCleanupTimeout$")
	cmd.Env = append(os.Environ(), childEnv+"=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("有界退出失败: %v\n%s", err, output)
	}
}
