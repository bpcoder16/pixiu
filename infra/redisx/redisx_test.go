package redisx

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/redis/go-redis/v9"
)

func TestNewValidatesConfig(t *testing.T) {
	valid := Config{Name: "cache", Options: redis.Options{Addr: "127.0.0.1:6379"}}
	type testCase struct {
		name string
		ctx  context.Context
		cfg  Config
	}
	tests := []testCase{
		{"nil context", nil, valid},
		{"empty name", context.Background(), Config{Options: valid.Options}},
		{"empty address", context.Background(), Config{Name: "cache"}},
		{"negative threshold", context.Background(), Config{Name: "cache", Options: valid.Options, SlowThreshold: -1}},
		{"negative pool", context.Background(), Config{Name: "cache", Options: redis.Options{Addr: valid.Options.Addr, PoolSize: -1}}},
	}
	if strconv.IntSize > 32 {
		tooLarge := int64(1 << 32)
		tests = append(tests, testCase{"oversized pool", context.Background(), Config{Name: "cache", Options: redis.Options{Addr: valid.Options.Addr, PoolSize: int(tooLarge)}}})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.ctx, tt.cfg); err == nil {
				t.Fatal("New 应拒绝无效配置")
			}
		})
	}
}

func TestNewUsesContextAndClosesClient(t *testing.T) {
	var buf bytes.Buffer
	useLogger(t, &buf, logit.DebugLevel)
	dialer, closed := pingDialer("+PONG\r\n", false)
	options := redis.Options{Addr: "test", Dialer: dialer, Protocol: 2, DisableIdentity: true, MaxRetries: -1}
	client, err := New(context.Background(), Config{
		Name:        "cache",
		Options:     options,
		LogCommands: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("启动 Ping 不应产生日志: %q", buf.String())
	}
	if !client.Client().Options().ContextTimeoutEnabled {
		t.Fatal("命令必须遵守调用方 context")
	}
	if options.ContextTimeoutEnabled {
		t.Fatal("New 不应修改调用方配置")
	}
	if err := client.Client().Ping(context.Background()).Err(); err != nil {
		t.Fatalf("客户端不可用: %v", err)
	}
	if record := onlyRecord(t, &buf); record["downstream_details"].(map[string]any)["command"] != "ping" {
		t.Fatalf("真实命令未经过 Hook: %v", record)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close 未关闭 Redis 连接")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("重复关闭: %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = New(canceled, Config{Name: "cache", Options: options})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("初始化应传播 context 取消: %v", err)
	}
}

func TestNewFailureDoesNotLogStartupPing(t *testing.T) {
	var buf bytes.Buffer
	useLogger(t, &buf, logit.DebugLevel)
	dialer, closed := pingDialer("-ERR unavailable\r\n", false)
	_, err := New(context.Background(), Config{
		Name: "cache", Options: redis.Options{Addr: "test", Dialer: dialer, Protocol: 2, DisableIdentity: true, MaxRetries: -1},
	})
	if err == nil {
		t.Fatal("启动 Ping 应失败")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("启动失败后连接未关闭")
	}
	if buf.Len() != 0 {
		t.Fatalf("启动错误由调用方记录，模块不重复写日志: %q", buf.String())
	}
}

func TestCommandRespectsContextDeadline(t *testing.T) {
	dialer, _ := pingDialer("+PONG\r\n", true)
	client, err := New(context.Background(), Config{
		Name: "cache", Options: redis.Options{Addr: "test", Dialer: dialer, Protocol: 2, DisableIdentity: true, MaxRetries: -1, ReadTimeout: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	begin := time.Now()
	if err := client.Client().Ping(ctx).Err(); err == nil {
		t.Fatal("服务端不响应时应按 context 截止时间失败")
	}
	if elapsed := time.Since(begin); elapsed > 500*time.Millisecond {
		t.Fatalf("命令没有及时遵守 context 截止时间: %v", elapsed)
	}
}

func TestHookLogsCommandResultsAndArgs(t *testing.T) {
	var buf bytes.Buffer
	useLogger(t, &buf, logit.DebugLevel)
	ctx := context.Background()
	cmd := redis.NewStringCmd(ctx, "GET", "private-key")
	cmd.SetVal("private-value")
	hook := newLoggerHook("cache", time.Hour, true)
	if err := hook.ProcessHook(func(context.Context, redis.Cmder) error { return nil })(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	record := onlyRecord(t, &buf)
	if record["level"] != "DEBUG" ||
		record["msg"] != "Redis" ||
		record["downstream_type"] != "Redis" ||
		record["downstream_id"] != "cache" {
		t.Fatalf("结果日志不完整: %v", record)
	}
	details := record["downstream_details"].(map[string]any)
	if details["command"] != "get" || details["status"] != "ok" {
		t.Fatalf("命令详情错误: %v", details)
	}
	args, ok := details["args"].([]any)
	if !ok || len(args) != 1 || args[0] != "private-key" {
		t.Fatalf("正常命令日志未记录请求参数: %v", details)
	}
	if strings.Contains(buf.String(), "private-value") {
		t.Fatalf("日志不应记录命令返回值: %q", buf.String())
	}

	buf.Reset()
	hook = newLoggerHook("cache", time.Hour, false)
	_ = hook.ProcessHook(func(context.Context, redis.Cmder) error { return nil })(ctx, cmd)
	if buf.Len() != 0 {
		t.Fatalf("高频成功命令默认不产生日志: %q", buf.String())
	}

	buf.Reset()
	secretErr := errors.New("password-in-error")
	got := hook.ProcessHook(func(context.Context, redis.Cmder) error { return secretErr })(ctx, cmd)
	if !errors.Is(got, secretErr) {
		t.Fatalf("命令错误必须原样返回: %v", got)
	}
	record = onlyRecord(t, &buf)
	details = record["downstream_details"].(map[string]any)
	if record["level"] != "ERROR" || strings.Contains(buf.String(), "password-in-error") {
		t.Fatalf("错误分级或原始错误隔离异常: %q", buf.String())
	}
	args, ok = details["args"].([]any)
	if !ok || len(args) != 1 || args[0] != "private-key" {
		t.Fatalf("错误日志应记录请求参数，与 LogCommands 无关: %v", details)
	}

	buf.Reset()
	hook = newLoggerHook("cache", time.Hour, true)
	_ = hook.ProcessHook(func(context.Context, redis.Cmder) error { return secretErr })(ctx, cmd)
	record = onlyRecord(t, &buf)
	details = record["downstream_details"].(map[string]any)
	args, ok = details["args"].([]any)
	if record["level"] != "ERROR" || !ok || len(args) != 1 || args[0] != "private-key" || strings.Contains(buf.String(), "password-in-error") {
		t.Fatalf("错误日志应含请求参数但不含原始错误: %v", record)
	}
}

func TestHookMissSlowAndInternalCommands(t *testing.T) {
	var buf bytes.Buffer
	useLogger(t, &buf, logit.DebugLevel)
	ctx := context.Background()
	cmd := redis.NewStringCmd(ctx, "GET", "key")
	hook := newLoggerHook("cache", time.Hour, true)
	got := hook.ProcessHook(func(context.Context, redis.Cmder) error { return redis.Nil })(ctx, cmd)
	if !errors.Is(got, redis.Nil) {
		t.Fatalf("未命中语义改变: %v", got)
	}
	record := onlyRecord(t, &buf)
	if record["level"] != "DEBUG" || record["downstream_details"].(map[string]any)["status"] != "miss" {
		t.Fatalf("未命中应是正常结果: %v", record)
	}

	buf.Reset()
	hook = newLoggerHook("cache", time.Nanosecond, false)
	_ = hook.ProcessHook(func(context.Context, redis.Cmder) error {
		time.Sleep(time.Millisecond)
		return nil
	})(ctx, cmd)
	if record = onlyRecord(t, &buf); record["level"] != "WARN" {
		t.Fatalf("慢调用应告警: %v", record)
	}
	details := record["downstream_details"].(map[string]any)
	if args, ok := details["args"].([]any); !ok || len(args) != 1 || args[0] != "key" {
		t.Fatalf("慢调用日志应记录请求参数: %v", record)
	}

	buf.Reset()
	for _, name := range []string{"HELLO", "AUTH", "CLIENT"} {
		internal := redis.NewStatusCmd(ctx, name, "secret")
		_ = hook.ProcessHook(func(context.Context, redis.Cmder) error { return errors.New("secret") })(ctx, internal)
	}
	if buf.Len() != 0 {
		t.Fatalf("内部握手命令不应记录: %q", buf.String())
	}
}

func TestHookPipelineReportsLaterErrorAndTxCount(t *testing.T) {
	var buf bytes.Buffer
	useLogger(t, &buf, logit.DebugLevel)
	ctx := context.Background()
	cmds := []redis.Cmder{
		redis.NewStatusCmd(ctx, "MULTI"),
		redis.NewStringCmd(ctx, "GET", "private-key"),
		redis.NewStatusCmd(ctx, "SET", "private-key", "private-value"),
		redis.NewStatusCmd(ctx, "EXEC"),
	}
	hook := newLoggerHook("cache", time.Hour, false)
	got := hook.ProcessPipelineHook(func(_ context.Context, list []redis.Cmder) error {
		list[1].SetErr(redis.Nil)
		list[2].SetErr(errors.New("private-error"))
		return redis.Nil // go-redis 返回第一个错误，不能让它遮住后面的真实失败。
	})(ctx, cmds)
	if !errors.Is(got, redis.Nil) {
		t.Fatalf("Pipeline 错误必须原样返回: %v", got)
	}
	record := onlyRecord(t, &buf)
	if record["msg"] != "Redis" || record["downstream_type"] != "Redis" {
		t.Fatalf("批量日志消息或下游类型错误: %v", record)
	}
	details := record["downstream_details"].(map[string]any)
	if record["level"] != "ERROR" || details["status"] != "error" || details["count"] != float64(2) || details["misses"] != float64(1) || details["errors"] != float64(1) || details["transaction"] != true {
		t.Fatalf("批量执行日志异常: %v", record)
	}
	commands, ok := details["commands"].([]any)
	if !ok || len(commands) != 2 {
		t.Fatalf("批量错误日志应记录业务命令参数: %v", details)
	}
	first := commands[0].(map[string]any)
	second := commands[1].(map[string]any)
	if first["command"] != "get" || second["command"] != "set" {
		t.Fatalf("批量业务命令不正确: %v", commands)
	}
	if args := first["args"].([]any); len(args) != 1 || args[0] != "private-key" {
		t.Fatalf("GET 参数不正确: %v", args)
	}
	if args := second["args"].([]any); len(args) != 2 || args[0] != "private-key" || args[1] != "private-value" {
		t.Fatalf("SET 参数不正确: %v", args)
	}
	if strings.Contains(buf.String(), "private-error") {
		t.Fatalf("批量日志不应记录原始错误: %q", buf.String())
	}
}

func TestHookPipelineLogsBusinessCommands(t *testing.T) {
	var buf bytes.Buffer
	useLogger(t, &buf, logit.DebugLevel)
	ctx := context.Background()
	get := redis.NewStringCmd(ctx, "GET", "private-key")
	get.SetVal("private-result")
	cmds := []redis.Cmder{
		redis.NewStatusCmd(ctx, "MULTI"),
		get,
		redis.NewStatusCmd(ctx, "SET", "private-key", "private-value"),
		redis.NewStatusCmd(ctx, "EXEC"),
	}
	hook := newLoggerHook("cache", time.Hour, true)
	_ = hook.ProcessPipelineHook(func(context.Context, []redis.Cmder) error { return nil })(ctx, cmds)
	record := onlyRecord(t, &buf)
	details := record["downstream_details"].(map[string]any)
	commands, ok := details["commands"].([]any)
	if record["level"] != "DEBUG" || !ok || len(commands) != 2 {
		t.Fatalf("批量请求参数缺失: %v", details)
	}
	first := commands[0].(map[string]any)
	second := commands[1].(map[string]any)
	if first["command"] != "get" || second["command"] != "set" {
		t.Fatalf("批量命令不正确: %v", commands)
	}
	if got := first["args"].([]any); len(got) != 1 || got[0] != "private-key" {
		t.Fatalf("GET 参数不正确: %v", got)
	}
	if got := second["args"].([]any); len(got) != 2 || got[0] != "private-key" || got[1] != "private-value" {
		t.Fatalf("SET 参数不正确: %v", got)
	}
	if strings.Contains(buf.String(), "private-result") {
		t.Fatalf("批量日志不应记录返回值: %q", buf.String())
	}

	buf.Reset()
	cmds = []redis.Cmder{
		redis.NewStatusCmd(ctx, "AUTH", "private-password"),
		redis.NewStringCmd(ctx, "GET", "private-key"),
	}
	_ = hook.ProcessPipelineHook(func(context.Context, []redis.Cmder) error { return nil })(ctx, cmds)
	details = onlyRecord(t, &buf)["downstream_details"].(map[string]any)
	commands = details["commands"].([]any)
	if len(commands) != 1 || strings.Contains(buf.String(), "private-password") {
		t.Fatalf("内部握手命令参数不应记录: %v", details)
	}

	buf.Reset()
	hook = newLoggerHook("cache", time.Hour, false)
	commandErr := errors.New("private-error")
	if err := hook.ProcessPipelineHook(func(context.Context, []redis.Cmder) error {
		return commandErr
	})(ctx, cmds); !errors.Is(err, commandErr) {
		t.Fatalf("批量错误必须原样返回: %v", err)
	}
	details = onlyRecord(t, &buf)["downstream_details"].(map[string]any)
	commands, ok = details["commands"].([]any)
	if !ok || len(commands) != 1 ||
		strings.Contains(buf.String(), "private-password") ||
		strings.Contains(buf.String(), "private-error") {
		t.Fatalf("关闭 LogCommands 后的错误日志应包含业务参数且过滤握手参数和原始错误: %v", details)
	}
	business := commands[0].(map[string]any)
	args := business["args"].([]any)
	if business["command"] != "get" || len(args) != 1 || args[0] != "private-key" {
		t.Fatalf("关闭 LogCommands 后的错误日志缺少业务命令参数: %v", business)
	}
}

func TestHookRoutesByContextName(t *testing.T) {
	var defaultBuf, namedBuf bytes.Buffer
	useLogger(t, &defaultBuf, logit.DebugLevel)
	name := t.Name()
	logit.SetNamed(name, logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(&namedBuf))))
	ctx := logit.WithLoggerName(context.Background(), name)
	cmd := redis.NewStatusCmd(ctx, "SET", "key", "value")
	hook := newLoggerHook("cache", time.Hour, true)
	_ = hook.ProcessHook(func(context.Context, redis.Cmder) error { return nil })(ctx, cmd)
	if defaultBuf.Len() != 0 || namedBuf.Len() == 0 {
		t.Fatalf("日志未按 context 路由: default=%q named=%q", defaultBuf.String(), namedBuf.String())
	}
}

func TestHookRecordsDurationWithoutCommandLog(t *testing.T) {
	var buf bytes.Buffer
	useLogger(t, &buf, logit.InfoLevel)
	ctx := logit.WithStart(context.Background())
	cmd := redis.NewStringCmd(ctx, "GET", "key")

	for _, logCommands := range []bool{false, true} {
		hook := newLoggerHook("cache", time.Hour, logCommands)
		if err := hook.ProcessHook(func(context.Context, redis.Cmder) error {
			return nil
		})(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}
	if buf.Len() != 0 {
		t.Fatalf("正常命令不应输出日志: %q", buf.String())
	}

	logit.InfoDuration(ctx, "request done")
	record := onlyRecord(t, &buf)
	for _, key := range []string{
		"redis_1_duration_ms",
		"redis_2_duration_ms",
	} {
		if _, ok := record[key].(float64); !ok {
			t.Errorf("缺少下游耗时 %q: %v", key, record)
		}
	}
}

func TestHookRecordsDurationForMissErrorAndBatch(t *testing.T) {
	var buf bytes.Buffer
	useLogger(t, &buf, logit.InfoLevel)
	ctx := logit.WithStart(context.Background())
	hook := newLoggerHook("cache", time.Hour, false)
	cmd := redis.NewStringCmd(ctx, "GET", "key")

	if err := hook.ProcessHook(func(context.Context, redis.Cmder) error {
		return redis.Nil
	})(ctx, cmd); !errors.Is(err, redis.Nil) {
		t.Fatalf("未命中错误改变: %v", err)
	}
	commandErr := errors.New("redis unavailable")
	if err := hook.ProcessHook(func(context.Context, redis.Cmder) error {
		return commandErr
	})(ctx, cmd); !errors.Is(err, commandErr) {
		t.Fatalf("命令错误改变: %v", err)
	}

	batch := []redis.Cmder{redis.NewStringCmd(ctx, "GET", "key")}
	if err := hook.ProcessPipelineHook(func(context.Context, []redis.Cmder) error {
		return nil
	})(ctx, batch); err != nil {
		t.Fatal(err)
	}
	txBatch := []redis.Cmder{
		redis.NewStatusCmd(ctx, "MULTI"),
		redis.NewStringCmd(ctx, "GET", "key"),
		redis.NewStatusCmd(ctx, "EXEC"),
	}
	if err := hook.ProcessPipelineHook(func(context.Context, []redis.Cmder) error {
		return nil
	})(ctx, txBatch); err != nil {
		t.Fatal(err)
	}

	buf.Reset()
	logit.InfoDuration(ctx, "request done")
	record := onlyRecord(t, &buf)
	for _, key := range []string{
		"redis_1_duration_ms",
		"redis_2_duration_ms",
		"redis_3_duration_ms",
		"redis_4_duration_ms",
	} {
		if _, ok := record[key].(float64); !ok {
			t.Errorf("缺少下游耗时 %q: %v", key, record)
		}
	}
	if _, ok := record["redis_5_duration_ms"]; ok {
		t.Fatalf("批量执行被重复计数: %v", record)
	}
}

func TestHookSkipsDurationForStartupAndInternalCommands(t *testing.T) {
	var buf bytes.Buffer
	useLogger(t, &buf, logit.InfoLevel)
	ctx := logit.WithStart(context.Background())
	hook := newLoggerHook("cache", time.Hour, false)
	call := hook.ProcessHook(func(context.Context, redis.Cmder) error {
		return nil
	})
	if err := call(context.WithValue(ctx, startupPingKey{}, true), redis.NewStatusCmd(ctx, "PING")); err != nil {
		t.Fatal(err)
	}
	if err := call(ctx, redis.NewStatusCmd(ctx, "AUTH", "secret")); err != nil {
		t.Fatal(err)
	}
	internal := []redis.Cmder{
		redis.NewStatusCmd(ctx, "HELLO"),
		redis.NewStatusCmd(ctx, "CLIENT"),
	}
	if err := hook.ProcessPipelineHook(func(context.Context, []redis.Cmder) error {
		return nil
	})(ctx, internal); err != nil {
		t.Fatal(err)
	}
	if err := call(context.Background(), redis.NewStatusCmd(ctx, "PING")); err != nil {
		t.Fatal(err)
	}

	logit.InfoDuration(ctx, "request done")
	record := onlyRecord(t, &buf)
	if _, ok := record["redis_1_duration_ms"]; ok {
		t.Fatalf("跳过的命令被计入请求耗时: %v", record)
	}
}

func useLogger(t *testing.T, buf *bytes.Buffer, level logit.Level) {
	t.Helper()
	old := logit.Default()
	logit.SetDefault(logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(buf)), logit.OptMinLevel(level)))
	t.Cleanup(func() { logit.SetDefault(old) })
}

func onlyRecord(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte{'\n'})
	if len(lines) != 1 || len(lines[0]) == 0 {
		t.Fatalf("预期一条日志: %q", buf.String())
	}
	var record map[string]any
	if err := json.Unmarshal(lines[0], &record); err != nil {
		t.Fatal(err)
	}
	return record
}

// 内存连接只模拟握手和 Ping，避免依赖外部 Redis 或本地监听权限。
func pingDialer(pingReply string, blockAfterFirst bool) (func(context.Context, string, string) (net.Conn, error), <-chan struct{}) {
	closed := make(chan struct{}, 4)
	dial := func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() {
				_ = server.Close()
				select {
				case closed <- struct{}{}:
				default:
				}
			}()
			reader := bufio.NewReader(server)
			pings := 0
			for {
				name, err := readCommandName(reader)
				if err != nil {
					return
				}
				response := "+OK\r\n"
				switch name {
				case "HELLO":
					response = "-ERR unknown command 'HELLO'\r\n"
				case "PING":
					pings++
					if blockAfterFirst && pings > 1 {
						continue
					}
					response = pingReply
				}
				if _, err := server.Write([]byte(response)); err != nil {
					return
				}
			}
		}()
		return client, nil
	}
	return dial, closed
}

func readCommandName(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(line, "*") {
		return "", fmt.Errorf("invalid array: %q", line)
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || count < 1 {
		return "", fmt.Errorf("invalid array size: %q", line)
	}
	var name string
	for i := range count {
		line, err = r.ReadString('\n')
		if err != nil {
			return "", err
		}
		if !strings.HasPrefix(line, "$") {
			return "", fmt.Errorf("invalid bulk string: %q", line)
		}
		size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || size < 0 {
			return "", fmt.Errorf("invalid bulk size: %q", line)
		}
		arg := make([]byte, size+2)
		if _, err := io.ReadFull(r, arg); err != nil {
			return "", err
		}
		if i == 0 {
			name = strings.ToUpper(string(arg[:size]))
		}
	}
	return name, nil
}
