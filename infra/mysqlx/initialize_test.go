package mysqlx

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	gormmysql "gorm.io/driver/mysql"
)

func TestVersionProbeUsesDriverReadTimeout(t *testing.T) {
	endpoint := testVersionEndpoint(t, "8.0.36", "version")
	endpoint.driver.ReadTimeout = 100 * time.Millisecond
	db, pool, err := open(context.Background(), Config{Name: "startup"}, endpoint)
	if pool != nil {
		_ = pool.Close()
	}
	if db != nil || err == nil || !strings.Contains(err.Error(), "initialize endpoint") {
		t.Fatalf("版本探测未遵循驱动读取超时: db=%v err=%v", db, err)
	}
}

func TestVersionProbeKeepsDialectAndOperationContext(t *testing.T) {
	const version = "10.5.12-MariaDB"
	db, pool, err := open(context.Background(), Config{Name: "startup"}, testVersionEndpoint(t, version, ""))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	dialect := db.Dialector.(*gormmysql.Dialector)
	if dialect.ServerVersion != version || !dialect.DontSupportRenameColumn || !dialect.DontSupportForShareClause {
		t.Fatalf("版本识别或 MariaDB 兼容配置丢失: %+v", dialect.Config)
	}
	client := testClusterClient(t, db, pool)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var got string
	if err := client.MasterDB(ctx).Raw("SELECT VERSION()").Scan(&got).Error; !errors.Is(err, context.Canceled) {
		t.Fatalf("业务查询未遵循 context 取消: %v", err)
	}
	if err := client.MasterDB(context.Background()).Raw("SELECT VERSION()").Scan(&got).Error; err != nil || got != version {
		t.Fatalf("操作 context 取消后影响客户端: version=%q err=%v", got, err)
	}
}

func testVersionEndpoint(t *testing.T, version string, stallAt string) preparedEndpoint {
	t.Helper()
	endpoint, err := prepare(Endpoint{
		Host:        "localhost",
		Database:    "test",
		Username:    "test",
		ReadTimeout: time.Second,
	}, "master", "")
	if err != nil {
		t.Fatal(err)
	}
	endpoint.driver.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		t.Cleanup(func() { _ = client.Close() })
		t.Cleanup(func() { _ = server.Close() })
		go serveVersionProbe(server, version, stallAt)
		return client, nil
	}
	return endpoint
}

// 只模拟握手、建连设置、Ping 和版本查询；实际收发与取消仍由 MySQL 驱动完成。
func serveVersionProbe(conn net.Conn, version string, stallAt string) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	writePacket := func(sequence byte, payload []byte) error {
		header := []byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), sequence}
		_, err := conn.Write(append(header, payload...))
		return err
	}
	readPacket := func() ([]byte, error) {
		var header [4]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return nil, err
		}
		payload := make([]byte, int(header[0])|int(header[1])<<8|int(header[2])<<16)
		_, err := io.ReadFull(conn, payload)
		return payload, err
	}
	// 协议 4.1、数据库选择、secure connection 与 native password 认证。
	handshake := append([]byte{10}, []byte("8.0.36\x00")...)
	handshake = append(handshake, 1, 0, 0, 0)
	handshake = append(handshake, []byte("12345678\x00")...)
	handshake = append(handshake, 0x09, 0x82, 0x21, 2, 0, 8, 0, 21)
	handshake = append(handshake, make([]byte, 10)...)
	handshake = append(handshake, []byte("123456789012\x00mysql_native_password\x00")...)
	if writePacket(0, handshake) != nil {
		return
	}
	if _, err := readPacket(); err != nil {
		return
	}
	ok := []byte{0, 0, 0, 2, 0, 0, 0}
	if writePacket(2, ok) != nil {
		return
	}
	for {
		packet, err := readPacket()
		if err != nil || len(packet) == 0 || packet[0] == 1 {
			return
		}
		isVersion := packet[0] == 3 && string(packet[1:]) == "SELECT VERSION()"
		if stallAt == "ping" && packet[0] == 14 || stallAt == "version" && isVersion {
			_, _ = io.Copy(io.Discard, conn)
			return
		}
		if !isVersion {
			if writePacket(1, ok) != nil {
				return
			}
			continue
		}
		field := []byte{3, 'd', 'e', 'f', 0, 0, 0, 9, 'V', 'E', 'R', 'S', 'I', 'O', 'N', '(', ')', 0,
			12, 33, 0, 255, 0, 0, 0, 253, 0, 0, 0, 0, 0}
		eof := []byte{254, 0, 0, 2, 0}
		for i, payload := range [][]byte{{1}, field, eof, append([]byte{byte(len(version))}, version...), eof} {
			if writePacket(byte(i+1), payload) != nil {
				return
			}
		}
	}
}

func TestNewInitializationTimeout(t *testing.T) {
	for _, stage := range []string{"handshake", "ping", "version"} {
		t.Run(stage, func(t *testing.T) {
			endpoint := testMySQLEndpoint(t, stage)
			start := time.Now()
			client, err := New(Config{
				Name:        "timeout",
				Master:      endpoint,
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
		Master: testMySQLEndpoint(t, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var version string
	if err := client.MasterDB(context.Background()).Raw("SELECT VERSION()").Row().Scan(&version); err != nil || version != "10.5.12-MariaDB" {
		t.Fatalf("初始化 context 取消后影响业务查询: version=%q err=%v", version, err)
	}
}

func testMySQLEndpoint(t *testing.T, stallAt string) Endpoint {
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
		if stallAt == "handshake" {
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			_, _ = io.Copy(io.Discard, conn)
			return
		}
		serveVersionProbe(conn, "10.5.12-MariaDB", stallAt)
	}()
	host, portText, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	return Endpoint{
		Host:     host,
		Port:     port,
		Database: "test",
		Username: "test",
	}
}
