package bootstrap_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type bootstrapRedisServer struct {
	port      int
	listener  net.Listener
	connected atomic.Bool
	closed    chan struct{}
}

// 模拟多连接握手、Ping 和 Get，支持模板中的预建空闲连接池。
func newBootstrapRedisServer(t *testing.T, value, pingReply string) *bootstrapRedisServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &bootstrapRedisServer{
		port:     listener.Addr().(*net.TCPAddr).Port,
		listener: listener,
		closed:   make(chan struct{}),
	}
	stop := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		_ = listener.Close()
		server.requireClosed(t)
	})
	// bootstrap 会修改 time.Local；后台协议桩只使用预先计算的绝对期限。
	deadline := time.Now().Add(10 * time.Second)
	go func() {
		defer close(server.closed)
		var connections sync.WaitGroup
		defer connections.Wait()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			server.connected.Store(true)
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer conn.Close()
				_ = conn.SetDeadline(deadline)
				done := make(chan struct{})
				defer close(done)
				go func() {
					select {
					case <-stop:
						_ = conn.Close()
					case <-done:
					}
				}()
				serveBootstrapRedis(conn, value, pingReply)
			}()
		}
	}()
	return server
}

func (s *bootstrapRedisServer) requireClosed(t *testing.T) {
	t.Helper()
	// 停止接收新连接，但不主动关闭现有连接，以验证客户端完整释放连接池。
	_ = s.listener.Close()
	select {
	case <-s.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Redis 测试连接未关闭")
	}
}

func serveBootstrapRedis(conn net.Conn, value, pingReply string) {
	reader := bufio.NewReader(conn)
	for {
		args, err := readBootstrapRedisCommand(reader)
		if err != nil {
			return
		}
		reply := "+OK\r\n"
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			reply = "-ERR unknown command 'HELLO'\r\n"
		case "PING":
			if pingReply == "" {
				// 不返回响应，用真实 socket deadline 验证初始化失败时的回收。
				_, _ = io.Copy(io.Discard, conn)
				return
			}
			reply = pingReply
		case "GET":
			reply = fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
			if len(args) > 1 && args[1] == "missing" {
				reply = "$-1\r\n"
			}
		}
		if _, err := io.WriteString(conn, reply); err != nil {
			return
		}
	}
}

func readBootstrapRedisCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("invalid array: %q", line)
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || count < 1 {
		return nil, fmt.Errorf("invalid array size: %q", line)
	}
	args := make([]string, count)
	for i := range args {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(line, "$") {
			return nil, fmt.Errorf("invalid bulk string: %q", line)
		}
		size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || size < 0 {
			return nil, fmt.Errorf("invalid bulk size: %q", line)
		}
		data := make([]byte, size+2)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}
		args[i] = string(data[:size])
	}
	return args, nil
}
