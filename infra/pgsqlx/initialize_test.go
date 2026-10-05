package pgsqlx

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
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
