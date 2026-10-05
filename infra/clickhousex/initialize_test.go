package clickhousex

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestNewHTTPInitializationTimeout(t *testing.T) {
	for _, stage := range []string{"hello", "ping"} {
		t.Run(stage, func(t *testing.T) {
			hello := encodeHTTPBlock(t,
				[]string{"displayName()", "version()", "revision()", "timezone()"},
				[]string{"String", "String", "UInt32", "String"},
				"test", "25.1.1", uint32(clickhouse.ClientTCPProtocolVersion), "UTC",
			)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				query := r.URL.Query().Get("query")
				if query == "" {
					query = string(body)
				}
				if stage == "ping" && strings.HasPrefix(query, "SELECT displayName()") {
					_, _ = w.Write(hello)
					return
				}
				// 已发送响应头，但不返回响应体；驱动的 ResponseHeaderTimeout 无法覆盖。
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
				case <-time.After(2 * time.Second):
				}
			}))
			defer server.Close()
			host, portText, _ := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
			port, _ := strconv.Atoi(portText)
			start := time.Now()
			client, err := New(Config{
				Name:        "timeout",
				InitTimeout: 100 * time.Millisecond,
				Master: Endpoint{
					Host:     host,
					Port:     port,
					Database: "default",
					Username: "default",
					Protocol: clickhouse.HTTP,
				},
			})
			if client != nil {
				_ = client.Close()
			}
			if client != nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("HTTP 初始化 %s 未返回超时: client=%v err=%v", stage, client, err)
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("初始化超时未及时返回: %v", elapsed)
			}
		})
	}
}

func TestNewNativeInitializationTimeout(t *testing.T) {
	for _, stage := range []string{"handshake", "tls", "ping"} {
		t.Run(stage, func(t *testing.T) {
			endpoint := testNativeEndpoint(t, stage)
			if stage == "tls" {
				endpoint.TLS = &tls.Config{ServerName: "localhost"}
			}
			start := time.Now()
			client, err := New(Config{
				Name:        "timeout",
				InitTimeout: 100 * time.Millisecond,
				Master:      endpoint,
			})
			if client != nil {
				_ = client.Close()
			}
			if client != nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("原生协议 %s 未返回超时: client=%v err=%v", stage, client, err)
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("原生协议初始化超时未及时返回: %v", elapsed)
			}
		})
	}
}

func TestNewNativeReleasesInitializationContext(t *testing.T) {
	client, err := New(Config{
		Name:   "startup",
		Master: testNativeEndpoint(t, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	pool, err := client.MasterDB(context.Background()).DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.PingContext(context.Background()); err != nil {
		t.Fatalf("初始化 context 取消后关闭了正常连接: %v", err)
	}
}

func testNativeEndpoint(t *testing.T, stallAt string) Endpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		if stallAt == "handshake" || stallAt == "tls" {
			_, _ = io.Copy(io.Discard, conn)
			return
		}
		reader := chproto.NewReader(conn)
		if packet, err := reader.ReadByte(); err != nil || packet != proto.ClientHello {
			return
		}
		if _, err := reader.Str(); err != nil {
			return
		}
		for range 3 {
			if _, err := reader.UVarInt(); err != nil {
				return
			}
		}
		for range 3 {
			if _, err := reader.Str(); err != nil {
				return
			}
		}
		// 使用不需要 addendum 的协议版本，仅模拟握手与 Ping。
		hello := &chproto.Buffer{}
		hello.PutByte(proto.ServerHello)
		hello.PutString("ClickHouse")
		hello.PutUVarInt(25)
		hello.PutUVarInt(1)
		hello.PutUVarInt(proto.DBMS_MIN_REVISION_WITH_VERSION_PATCH)
		hello.PutString("UTC")
		hello.PutString("test")
		hello.PutUVarInt(1)
		if _, err := conn.Write(hello.Buf); err != nil {
			return
		}
		for {
			packet, err := reader.ReadByte()
			if err != nil || packet != proto.ClientPing {
				return
			}
			if stallAt == "ping" {
				_, _ = io.Copy(io.Discard, conn)
				return
			}
			if _, err := conn.Write([]byte{proto.ServerPong}); err != nil {
				return
			}
		}
	}()
	host, portText, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	return Endpoint{
		Host:     host,
		Port:     port,
		Database: "default",
		Username: "default",
	}
}
