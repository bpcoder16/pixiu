package websocketx

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func BenchmarkRoundTrip(b *testing.B) {
	for _, size := range []int{256, 1024, 16384} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			_, server := newTestEndpoint(b, Config{
				OnMessage: func(ctx context.Context, s *Session, m Message) error { return s.Send(ctx, m) },
			})
			c := dialTest(b, server)
			data := bytes.Repeat([]byte("x"), size)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for range b.N {
				if err := c.WriteMessage(websocket.BinaryMessage, data); err != nil {
					b.Fatal(err)
				}
				_, got, err := c.ReadMessage()
				if err != nil || !bytes.Equal(got, data) {
					b.Fatalf("往返失败: %v", err)
				}
			}
		})
	}
}

func BenchmarkBroadcast32(b *testing.B) {
	for _, prepared := range []bool{false, true} {
		b.Run(map[bool]string{
			false: "Send",
			true:  "SendPrepared",
		}[prepared], func(b *testing.B) {
			opened := make(chan *Session, 32)
			e, server := newTestEndpoint(b, Config{OnOpen: func(_ context.Context, s *Session) error {
				opened <- s
				return nil
			}})
			clients := make([]*websocket.Conn, 32)
			sessions := make([]*Session, 32)
			for i := range clients {
				clients[i] = dialTest(b, server)
				sessions[i] = await(b, opened)
			}
			data := bytes.Repeat([]byte("x"), 1024)
			message := Message{
				Type: Binary,
				Data: data,
			}
			frame, err := e.Prepare(message)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(32 * 1024)
			b.ResetTimer()
			for range b.N {
				for _, s := range sessions {
					if prepared {
						err = s.SendPrepared(context.Background(), frame)
					} else {
						err = s.Send(context.Background(), message)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				for _, client := range clients {
					_, got, err := client.ReadMessage()
					if err != nil || !bytes.Equal(got, data) {
						b.Fatalf("下发失败: %v", err)
					}
				}
			}
		})
	}
}

func BenchmarkPrepare(b *testing.B) {
	e, err := New(Config{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		e.Cancel()
		_ = e.Wait(context.Background())
	})
	m := Message{
		Type: Binary,
		Data: bytes.Repeat([]byte("x"), 1024),
	}
	b.ReportAllocs()
	b.SetBytes(1024)
	b.ResetTimer()
	for range b.N {
		if _, err := e.Prepare(m); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIdleConnections 建连并驻留一批回环连接；客户端与服务端在同一进程。
// 用 -benchtime=1x 运行，RSS 是本机 ps 的快照，不能当作仅服务端的单连接内存。
func BenchmarkIdleConnections(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			for range b.N {
				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)
				baseGoroutines := runtime.NumGoroutine()
				baseRSS := residentKiB()
				opened := make(chan struct{}, count)
				e, server := newTestEndpoint(b, Config{
					MaxConnections: count,
					OnOpen: func(context.Context, *Session) error {
						opened <- struct{}{}
						return nil
					},
				})
				clients := make([]*websocket.Conn, count)
				for i := range clients {
					clients[i] = dialTest(b, server)
					await(b, opened)
				}
				runtime.GC()
				var after runtime.MemStats
				runtime.ReadMemStats(&after)
				goroutines := runtime.NumGoroutine() - baseGoroutines
				rss := residentKiB()
				b.ReportMetric(float64(after.HeapInuse-before.HeapInuse)/float64(count), "heap-B/pair")
				b.ReportMetric(float64(goroutines)/float64(count), "goroutines/pair")
				if rss > 0 && baseRSS > 0 {
					b.ReportMetric(float64(rss), "rss-KiB")
					b.ReportMetric(float64(rss-baseRSS)*1024/float64(count), "rss-B/pair")
				}
				if e.Len() != count || goroutines > 4*count+64 || rss > 512*1024 {
					b.Fatalf("实验容量门槛未满足: connections=%d goroutines=%d rssKiB=%d", e.Len(), goroutines, rss)
				}
				_ = e.Stop()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := e.Wait(ctx)
				cancel()
				if err != nil || e.Budget().Usage() != (Usage{}) {
					b.Fatalf("驻留实验收尾失败: %v %+v", err, e.Budget().Usage())
				}
				for _, client := range clients {
					client.Close()
				}
				server.Close()
			}
		})
	}
}

func residentKiB() int64 {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		return 0
	}
	data, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return 0
	}
	value, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	return value
}
