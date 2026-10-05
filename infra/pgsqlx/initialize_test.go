package pgsqlx

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestInitializationErrorKeepsConnectionCause(t *testing.T) {
	for _, reason := range []error{
		&net.DNSError{
			Err:        "no such host",
			Name:       "db.invalid",
			IsNotFound: true,
		},
		&net.OpError{
			Op:  "dial",
			Net: "tcp",
			Err: errors.New("connection refused"),
		},
		errors.New("tls: failed to verify certificate: x509: certificate signed by unknown authority"),
	} {
		cfg, err := pgconn.ParseConfig("postgres://reader:secret-marker@localhost/orders?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
			return nil, reason
		}
		_, err = pgconn.ConnectConfig(context.Background(), cfg)
		if err == nil {
			t.Fatal("模拟连接失败未生效")
		}
		got := initializationError("ping", "master", err)
		if !strings.Contains(got.Error(), reason.Error()) || !strings.Contains(got.Error(), `ping endpoint "master"`) {
			t.Fatalf("连接失败原因丢失: %v", got)
		}
		if strings.Contains(got.Error(), "secret-marker") || strings.Contains(got.Error(), "postgres://") {
			t.Fatalf("初始化错误泄露连接凭据: %v", got)
		}
		if _, ok := errors.AsType[*pgconn.ConnectError](got); ok {
			t.Fatal("返回错误链仍保留含凭据的连接配置")
		}
	}
}

func TestNewInitializationTimeout(t *testing.T) {
	for _, stage := range []string{"handshake", "ping"} {
		t.Run(stage, func(t *testing.T) {
			start := time.Now()
			client, err := New(Config{
				Name:        "timeout",
				Master:      testPostgresEndpoint(t, stage),
				InitTimeout: 100 * time.Millisecond,
			})
			if client != nil {
				_ = client.Close()
			}
			if client != nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("初始化 %s 未返回超时: client=%v err=%v", stage, client, err)
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("初始化超时未及时返回: %v", elapsed)
			}
		})
	}
}

func TestNewReleasesInitializationContext(t *testing.T) {
	client, err := New(Config{
		Name:   "startup",
		Master: testPostgresEndpoint(t, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.MasterDB(ctx).Exec("-- ping").Error; !errors.Is(err, context.Canceled) {
		t.Fatalf("业务查询未遵循 context: %v", err)
	}
	if err := client.MasterDB(context.Background()).Exec("-- ping").Error; err != nil {
		t.Fatalf("初始化 context 取消后影响业务查询: %v", err)
	}
}

func testPostgresEndpoint(t *testing.T, stallAt string) Endpoint {
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
		if stallAt == "handshake" {
			_, _ = io.Copy(io.Discard, conn)
			return
		}
		backend := pgproto3.NewBackend(conn, conn)
		if _, err := backend.ReceiveStartupMessage(); err != nil {
			return
		}
		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if backend.Flush() != nil {
			return
		}
		for {
			msg, err := backend.Receive()
			if err != nil {
				return
			}
			if _, ok := msg.(*pgproto3.Query); !ok {
				return
			}
			if stallAt == "ping" {
				_, _ = io.Copy(io.Discard, conn)
				return
			}
			backend.Send(&pgproto3.EmptyQueryResponse{})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if backend.Flush() != nil {
				return
			}
		}
	}()
	host, portText, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	return Endpoint{
		Host:     host,
		Port:     port,
		Database: "test",
		Username: "test",
		SSLMode:  "disable",
	}
}

func TestInitializationErrorKeepsContextAndSQLState(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		if err := initializationError("ping", "master", cause); !errors.Is(err, cause) {
			t.Fatalf("context 错误链丢失: %v", err)
		}
	}
	err := initializationError("ping", "master", &pgconn.PgError{
		Code:    "28P01",
		Message: "secret-marker",
	})
	if !strings.Contains(err.Error(), "SQLSTATE 28P01") || strings.Contains(err.Error(), "secret-marker") {
		t.Fatalf("服务端错误码或脱敏错误: %v", err)
	}
}

func TestPrepareReportsCertificateConfigurationCause(t *testing.T) {
	endpoint := validEndpoint()
	endpoint.SSLMode = "verify-full"
	endpoint.SSLRootCert = filepath.Join(t.TempDir(), "missing-ca.pem")
	_, err := prepare(endpoint, "master", "", false)
	if err == nil || !strings.Contains(err.Error(), "missing-ca.pem") || !strings.Contains(err.Error(), "no such file") {
		t.Fatalf("证书配置失败缺少原因: %v", err)
	}
	if strings.Contains(err.Error(), endpoint.Password) || strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("证书配置错误泄露凭据: %v", err)
	}
}
