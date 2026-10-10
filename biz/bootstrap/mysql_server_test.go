package bootstrap_test

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type bootstrapMySQLServer struct {
	port      int
	connected atomic.Bool
	closed    chan struct{}
}

// 仅模拟单连接握手、会话设置、Ping 和版本查询，测试仍经过真实 MySQL 驱动。
func newBootstrapMySQLServer(t *testing.T, version string, stall bool) *bootstrapMySQLServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &bootstrapMySQLServer{
		port:   listener.Addr().(*net.TCPAddr).Port,
		closed: make(chan struct{}),
	}
	stop := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		_ = listener.Close()
		server.requireClosed(t)
	})
	go func() {
		defer close(server.closed)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		server.connected.Store(true)
		go func() {
			select {
			case <-stop:
				_ = conn.Close()
			case <-server.closed:
			}
		}()
		serveBootstrapMySQL(conn, version, stall)
	}()
	return server
}

func (s *bootstrapMySQLServer) requireClosed(t *testing.T) {
	t.Helper()
	select {
	case <-s.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("MySQL 测试连接未关闭")
	}
}

func serveBootstrapMySQL(conn net.Conn, version string, stall bool) {
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
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
	// MySQL 协议 4.1，启用数据库选择及 native password 认证。
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
		if stall && isVersion {
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
