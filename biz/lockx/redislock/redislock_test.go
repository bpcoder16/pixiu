package redislock

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/lockx"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/redis/go-redis/v9"
)

func TestNewRejectsInvalidClientAndRetries(t *testing.T) {
	for _, client := range []*redisx.Client{nil, {}} {
		if _, err := New(client, Config{TTL: time.Minute}); err == nil {
			t.Fatal("应拒绝未初始化客户端")
		}
	}
	for _, retries := range []int{-1, 0, 2} {
		t.Run(strconv.Itoa(retries), func(t *testing.T) {
			client := newWireClient(t, retries, nil)
			before := client.Client().Options().MaxRetries
			locker, err := New(client, Config{TTL: time.Minute})
			if (err == nil) != (retries == -1) || (locker != nil) != (retries == -1) {
				t.Fatalf("未按实际重试次数校验: retries=%d locker=%v err=%v", retries, locker, err)
			}
			if client.Client().Options().MaxRetries != before {
				t.Fatal("构造锁不应改写共享客户端")
			}
		})
	}
}

func TestCommandsPreserveKeyTokenAndTTL(t *testing.T) {
	commands := make(chan []string, 8)
	client := newWireClient(t, -1, func(args []string) string {
		commands <- args
		if strings.EqualFold(args[0], "EVAL") {
			return ":1\r\n"
		}
		return "+OK\r\n"
	})
	ctx := context.Background()
	var previous string
	for _, tt := range []struct {
		ttl  time.Duration
		want string
	}{
		{1500*time.Millisecond + time.Nanosecond, "1501"},
		{time.Duration(math.MaxInt64), "9223372036855"},
	} {
		lock, acquired, err := mustNew(t, client, tt.ttl).TryLock(ctx, " key:原值 ")
		if err != nil || !acquired || lock == nil {
			t.Fatalf("获取失败: lock=%v acquired=%v err=%v", lock, acquired, err)
		}
		deadline, ok := lock.Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > tt.ttl {
			t.Fatal("句柄未提供保守租期")
		}
		args := <-commands
		if len(args) != 6 || !strings.EqualFold(args[0], "SET") || args[1] != " key:原值 " ||
			!strings.EqualFold(args[3], "NX") || !strings.EqualFold(args[4], "PX") || args[5] != tt.want {
			t.Fatalf("获取必须原子设置 key、NX 和向上取整的 TTL: %v", args)
		}
		token := args[2]
		if len(token) < 26 || token == previous {
			t.Fatal("每次获取应使用独立随机令牌")
		}
		previous = token
		if err := lock.Unlock(nil); err == nil {
			t.Fatal("释放应拒绝 nil context")
		}
		if err := lock.Unlock(ctx); err != nil {
			t.Fatal(err)
		}
		args = <-commands
		if len(args) != 5 || !strings.EqualFold(args[0], "EVAL") || args[2] != "1" ||
			args[3] != " key:原值 " || args[4] != token {
			t.Fatalf("释放没有携带本次身份: %v", args)
		}
	}
	if len(commands) != 0 {
		t.Fatal("单次调用不应产生多余命令")
	}
}

func TestTryLockSeparatesContentionAndErrors(t *testing.T) {
	for _, tt := range []struct {
		name     string
		response string
		wantErr  bool
	}{
		{"竞争失败", "$-1\r\n", false},
		{"服务端错误", "-ERR unavailable\r\n", true},
		{"响应丢失", "", true},
		{"异常状态", "+unexpected\r\n", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			client := newWireClient(t, -1, func([]string) string {
				calls.Add(1)
				return tt.response
			})
			lock, acquired, err := mustNew(t, client).TryLock(context.Background(), "key")
			if lock != nil || acquired || (err != nil) != tt.wantErr || calls.Load() != 1 {
				t.Fatalf("竞争与错误混淆或发生重试: lock=%v acquired=%v err=%v calls=%d", lock, acquired, err, calls.Load())
			}
			// net.Pipe 的关闭可能发生在设置读期限或真正读取时。
			if tt.response == "" && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("响应丢失的底层错误未保留: %v", err)
			}
			if strings.HasPrefix(tt.response, "-ERR") {
				var cause redis.Error
				if !errors.As(err, &cause) {
					t.Fatalf("Redis 错误链未保留: %v", err)
				}
			}
		})
	}
}

func TestTryLockDeadlineAndLateSuccess(t *testing.T) {
	client := newWireClient(t, -1, func([]string) string {
		time.Sleep(60 * time.Millisecond)
		return "+OK\r\n"
	})
	lock, acquired, err := mustNew(t, client, 20*time.Millisecond).TryLock(context.Background(), "key")
	if lock != nil || acquired || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("租期耗尽或响应超时不能返回锁: lock=%v acquired=%v err=%v", lock, acquired, err)
	}
}

func TestUnlockResults(t *testing.T) {
	for _, tt := range []struct {
		name     string
		response string
		wantErr  bool
	}{
		{"不再持有时幂等完成", ":0\r\n", false},
		{"实际删除", ":1\r\n", false},
		{"服务端失败", "-ERR unavailable\r\n", true},
		{"响应丢失", "", true},
		{"异常结果", ":2\r\n", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := newWireClient(t, -1, func(args []string) string {
				if strings.EqualFold(args[0], "SET") {
					return "+OK\r\n"
				}
				return tt.response
			})
			lock, _, err := mustNew(t, client).TryLock(context.Background(), "key")
			if err != nil {
				t.Fatal(err)
			}
			err = lock.Unlock(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("释放结果错误: %v", err)
			}
		})
	}
}

type expiredContext struct{ context.Context }

func (expiredContext) Deadline() (time.Time, bool) { return time.Now().Add(-time.Second), true }

func TestTryLockValidatesBeforeCommands(t *testing.T) {
	var calls atomic.Int32
	client := newWireClient(t, -1, func([]string) string {
		calls.Add(1)
		return "+OK\r\n"
	})
	locker := mustNew(t, client)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tt := range []struct {
		ctx context.Context
		key string
	}{
		{nil, "key"},
		{context.Background(), ""},
		{context.Background(), " \t "},
		{canceled, "key"},
		{expiredContext{context.Background()}, "key"},
	} {
		lock, acquired, err := locker.TryLock(tt.ctx, tt.key)
		if lock != nil || acquired || err == nil {
			t.Fatalf("无效参数未被拒绝: lock=%v acquired=%v err=%v", lock, acquired, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("无效请求发送了业务命令")
	}
	for _, locker := range []*Locker{nil, {}} {
		if _, _, err := locker.TryLock(context.Background(), "key"); err == nil {
			t.Fatal("未构造的实例不应可用")
		}
	}
}

func TestCommandsUseRedisxLoggingAndDuration(t *testing.T) {
	var output bytes.Buffer
	logger := logit.MustNew(
		logit.OptEncoder(logit.DefaultJSONEncoder),
		logit.OptWriter(logit.NewWriter(&output)),
		logit.OptMinLevel(logit.DebugLevel),
	)
	logit.SetNamed(t.Name(), logger)
	t.Cleanup(func() { _ = logit.Close(logger) })
	client := newWireClient(t, -1, func(args []string) string {
		if strings.EqualFold(args[0], "EVAL") {
			return ":1\r\n"
		}
		return "+OK\r\n"
	})
	ctx := logit.WithLoggerName(logit.WithStart(context.Background()), t.Name())
	executed, err := lockx.TryDo(ctx, mustNew(t, client), "key", func(context.Context) error {
		return nil
	})
	if !executed || err != nil {
		t.Fatalf("TryDo 失败: executed=%v err=%v", executed, err)
	}
	logit.InfoDuration(ctx, "done")
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
	if len(lines) != 3 {
		t.Fatalf("应仅有两条命令日志和一条汇总: %s", output.String())
	}
	for i, command := range []string{"set", "eval"} {
		var record map[string]any
		if err := json.Unmarshal(lines[i], &record); err != nil {
			t.Fatal(err)
		}
		details := record["downstream_details"].(map[string]any)
		if record["msg"] != "Redis" || details["command"] != command || details["args"] == nil {
			t.Fatalf("丢失 redisx 命令日志: %v", record)
		}
	}
	var summary map[string]any
	if err := json.Unmarshal(lines[2], &summary); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"Redis_" + t.Name() + "_1_duration_ms",
		"Redis_" + t.Name() + "_2_duration_ms",
	} {
		if _, ok := summary[key].(float64); !ok {
			t.Fatalf("丢失请求耗时 %s: %v", key, summary)
		}
	}
}

func mustNew(t *testing.T, client *redisx.Client, ttl ...time.Duration) *Locker {
	t.Helper()
	duration := time.Minute
	if len(ttl) > 0 {
		duration = ttl[0]
	}
	locker, err := New(client, Config{TTL: duration})
	if err != nil {
		t.Fatal(err)
	}
	return locker
}

// 只模拟协议响应，不在替身中重新实现 Lua 或锁语义；原子性由真实 Redis 验证。
func newWireClient(t *testing.T, retries int, handle func([]string) string) *redisx.Client {
	t.Helper()
	var workers sync.WaitGroup
	client, err := redisx.New(context.Background(), redisx.Config{
		Name: t.Name(),
		Options: redis.Options{
			Addr:            "test",
			Protocol:        2,
			DisableIdentity: true,
			MaxRetries:      retries,
			ReadTimeout:     time.Second,
			WriteTimeout:    time.Second,
			Dialer: func(context.Context, string, string) (net.Conn, error) {
				client, server := net.Pipe()
				workers.Go(func() {
					defer server.Close()
					reader := bufio.NewReader(server)
					for {
						args, err := readCommand(reader)
						if err != nil {
							return
						}
						var reply string
						switch strings.ToUpper(args[0]) {
						case "HELLO":
							reply = "-ERR unknown command 'HELLO'\r\n"
						case "PING":
							reply = "+PONG\r\n"
						case "CLIENT":
							reply = "+OK\r\n"
						default:
							if handle == nil {
								t.Errorf("意外的业务命令: %s", args[0])
								return
							}
							reply = handle(args)
						}
						if reply == "" {
							return
						}
						if _, err := io.WriteString(server, reply); err != nil {
							return
						}
					}
				})
				return client, nil
			},
		},
		LogCommands: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		workers.Wait()
	})
	return client
}

func readCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("invalid command: %q", line)
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || count < 1 {
		return nil, fmt.Errorf("invalid argument count: %q", line)
	}
	args := make([]string, count)
	for i := range args {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(line, "$") {
			return nil, fmt.Errorf("invalid argument: %q", line)
		}
		size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || size < 0 {
			return nil, fmt.Errorf("invalid argument size: %q", line)
		}
		data := make([]byte, size+2)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}
		args[i] = string(data[:size])
	}
	return args, nil
}

func TestNewRejectsInvalidTTL(t *testing.T) {
	client := newWireClient(t, -1, nil)
	for _, ttl := range []time.Duration{-time.Nanosecond, -time.Second} {
		if _, err := New(client, Config{TTL: ttl}); err == nil {
			t.Fatal("应拒绝无效租期")
		}
	}
}
