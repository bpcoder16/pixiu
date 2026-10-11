//go:build unix

package apprun

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestProcessSignals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		first  os.Signal
		second bool
	}{
		{name: "SIGINT平滑关闭", first: syscall.SIGINT},
		{name: "SIGTERM平滑关闭", first: syscall.SIGTERM},
		{name: "第二次SIGINT直接终止", first: syscall.SIGINT, second: true},
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
		Shutdown: func() error {
			fmt.Println("draining")
			_, err := bufio.NewReader(os.Stdin).ReadString('\n')
			close(stop)
			return err
		},
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
