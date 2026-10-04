package redisx

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/redis/go-redis/v9"
)

func TestCommandCancelDuringReadPreservesDriverResult(t *testing.T) {
	received := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	client := newWireClient(t, false, func(string) string {
		close(received)
		<-release
		return "$5\r\nvalue\r\n"
	})
	t.Cleanup(unblock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- client.Client().Get(ctx, "key").Err()
	}()
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("服务端未收到命令")
	}
	cancel()
	// 命令已发送；主动取消不会中断驱动正在等待响应的读取。
	select {
	case err := <-done:
		t.Fatalf("响应前意外结束: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-done:
		if err != nil || !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("应保留驱动的成功结果: err=%v ctx.Err()=%v", err, ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("响应后命令未返回")
	}
}

func TestCommandLogsRedisErrorCode(t *testing.T) {
	for _, tt := range []struct {
		name   string
		reply  string
		code   string
		status string
	}{
		{
			name:   "wrong type",
			reply:  "-WRONGTYPE private-error\r\n",
			code:   "WRONGTYPE",
			status: "error",
		},
		{
			name:   "permission",
			reply:  "-NOPERM private-error\r\n",
			code:   "NOPERM",
			status: "error",
		},
		{
			name:   "code only",
			reply:  "-OOM\r\n",
			code:   "OOM",
			status: "error",
		},
		{
			name:   "generic",
			reply:  "-ERR private-error\r\n",
			code:   "ERR",
			status: "error",
		},
		{
			name:   "unknown",
			reply:  "-PRIVATE_CODE private-error\r\n",
			status: "error",
		},
		{
			name:   "miss",
			reply:  "$-1\r\n",
			status: "miss",
		},
		{
			name:   "success",
			reply:  "+OK\r\n",
			status: "ok",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			useLogger(t, &buf, logit.DebugLevel)
			client := newWireClient(t, true, func(string) string { return tt.reply })
			err := client.Client().Get(context.Background(), "key").Err()
			switch tt.status {
			case "error":
				if err == nil || err.Error() != strings.TrimSuffix(tt.reply[1:], "\r\n") {
					t.Fatalf("服务端错误被改变: %v", err)
				}
			case "miss":
				if !errors.Is(err, redis.Nil) {
					t.Fatalf("未命中错误被改变: %v", err)
				}
			case "ok":
				if err != nil {
					t.Fatal(err)
				}
			}
			record := onlyRecord(t, &buf)
			details := record["downstream_details"].(map[string]any)
			if details["error_code"] != tt.code || details["status"] != tt.status {
				t.Fatalf("错误码或状态不正确: %v", details)
			}
			if tt.status == "error" && (details["error_type"] != "redis" || record["level"] != "ERROR") {
				t.Fatalf("既有错误分类或级别改变: %v", record)
			}
			if strings.Contains(buf.String(), "private-error") || strings.Contains(buf.String(), "PRIVATE_CODE") {
				t.Fatalf("日志泄露了错误正文或未知错误码: %q", buf.String())
			}
		})
	}
}

func TestPipelineLogsFirstRedisErrorCode(t *testing.T) {
	var buf bytes.Buffer
	useLogger(t, &buf, logit.DebugLevel)
	commands := 0
	client := newWireClient(t, false, func(string) string {
		commands++
		// 先读完批量请求再写响应，避免 net.Pipe 两端同时阻塞在写入。
		if commands < 3 {
			return ""
		}
		return "$-1\r\n-WRONGTYPE private-error\r\n-NOPERM private-error\r\n"
	})
	ctx := context.Background()
	_, err := client.Client().Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Get(ctx, "missing")
		pipe.Get(ctx, "wrong-type")
		pipe.Get(ctx, "forbidden")
		return nil
	})
	if !errors.Is(err, redis.Nil) {
		t.Fatalf("批量返回的首个错误被改变: %v", err)
	}
	record := onlyRecord(t, &buf)
	details := record["downstream_details"].(map[string]any)
	if record["level"] != "ERROR" || details["error_type"] != "redis" || details["error_code"] != "WRONGTYPE" ||
		details["misses"] != float64(1) || details["errors"] != float64(2) {
		t.Fatalf("批量日志未使用首个真实错误: %v", record)
	}
	if strings.Contains(buf.String(), "private-error") {
		t.Fatalf("批量日志泄露了错误正文: %q", buf.String())
	}
}

// 复用 RESP 请求解析，通过底层客户端验证命令执行与日志的完整链路。
func newWireClient(t *testing.T, logCommands bool, reply func(string) string) *Client {
	t.Helper()
	dialer := func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			reader := bufio.NewReader(server)
			for {
				name, err := readCommandName(reader)
				if err != nil {
					return
				}
				var response string
				switch name {
				case "HELLO":
					response = "-ERR unknown command 'HELLO'\r\n"
				case "PING":
					response = "+PONG\r\n"
				case "CLIENT":
					response = "+OK\r\n"
				default:
					response = reply(name)
				}
				if response != "" {
					if _, err := io.WriteString(server, response); err != nil {
						return
					}
				}
			}
		}()
		return client, nil
	}
	client, err := New(Config{
		Name:          "cache",
		SlowThreshold: time.Hour,
		LogCommands:   logCommands,
		Options: redis.Options{
			Addr:            "test",
			Dialer:          dialer,
			Protocol:        2,
			DisableIdentity: true,
			MaxRetries:      -1,
			ReadTimeout:     time.Second,
			WriteTimeout:    time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
