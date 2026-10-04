package clickhousex

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestCreateSendsBatchAndReturnsCommitError(t *testing.T) {
	type event struct {
		ID uint64
	}
	for _, tc := range []struct {
		name   string
		ids    []uint64
		reject bool
	}{
		{
			name: "single",
			ids:  []uint64{1},
		},
		{
			name: "batch",
			ids:  []uint64{2, 3},
		},
		{
			name:   "commit error",
			ids:    []uint64{4},
			reject: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 使用真实驱动与方言，模拟服务端确认收到的 Native 数据块。
			writes := make(chan []uint64, 4)
			hello := encodeHTTPBlock(t,
				[]string{"displayName()", "version()", "revision()", "timezone()"},
				[]string{"String", "String", "UInt32", "String"},
				"test", "25.1.1", uint32(clickhouse.ClientTCPProtocolVersion), "UTC",
			)
			ping := encodeHTTPBlock(t, []string{"1"}, []string{"UInt8"}, uint8(1))
			describe := encodeHTTPBlock(t,
				[]string{"name", "type", "default_type", "default_expression", "comment", "codec_expression", "ttl_expression"},
				[]string{"String", "String", "String", "String", "String", "String", "String"},
				"id", "UInt64", "", "", "", "", "",
			)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				query := r.URL.Query().Get("query")
				if query == "" {
					query = string(body)
				}
				switch {
				case strings.HasPrefix(query, "SELECT displayName()"):
					_, _ = w.Write(hello)
				case query == "SELECT 1":
					_, _ = w.Write(ping)
				case strings.HasPrefix(query, "DESCRIBE TABLE"):
					_, _ = w.Write(describe)
				case strings.HasPrefix(query, "INSERT"):
					block := proto.NewBlock()
					if err := block.Decode(chproto.NewReader(bytes.NewReader(body)), 0); err != nil {
						t.Error(err)
						http.Error(w, "invalid block", http.StatusBadRequest)
						return
					}
					var ids []uint64
					for i := 0; i < block.Rows(); i++ {
						var id uint64
						if err := block.Columns[0].ScanRow(&id, i); err != nil {
							t.Error(err)
							return
						}
						ids = append(ids, id)
					}
					writes <- ids
					if tc.reject {
						http.Error(w, "insert rejected", http.StatusInternalServerError)
					}
				default:
					t.Errorf("非预期查询: %s", query)
					http.Error(w, "unexpected query", http.StatusBadRequest)
				}
			}))
			defer server.Close()
			host, portText, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
			if err != nil {
				t.Fatal(err)
			}
			port, err := strconv.Atoi(portText)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := New(ctx, Config{
				Name: "write-test",
				Master: Endpoint{
					Host:     host,
					Port:     port,
					Database: "default",
					Username: "default",
					Protocol: clickhouse.HTTP,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			var events []event
			for _, id := range tc.ids {
				events = append(events, event{ID: id})
			}
			if len(events) == 1 {
				err = client.MasterDB(ctx).Create(&events[0]).Error
			} else {
				err = client.MasterDB(ctx).Create(&events).Error
			}
			if tc.reject {
				if err == nil || !strings.Contains(err.Error(), "insert rejected") {
					t.Fatalf("提交失败未返回: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-writes:
				if !slices.Equal(got, tc.ids) {
					t.Fatalf("写入数据=%v, want %v", got, tc.ids)
				}
			default:
				t.Fatal("Create 返回后服务端仍未收到 INSERT")
			}
			if len(writes) != 0 {
				t.Fatal("出现额外写入或隐式重试")
			}
		})
	}
}

func encodeHTTPBlock(t *testing.T, names, types []string, values ...any) []byte {
	t.Helper()
	block := proto.NewBlock()
	for i, name := range names {
		if err := block.AddColumn(name, column.Type(types[i])); err != nil {
			t.Fatal(err)
		}
	}
	if err := block.Append(values...); err != nil {
		t.Fatal(err)
	}
	buf := &chproto.Buffer{}
	if err := block.Encode(buf, clickhouse.ClientTCPProtocolVersion); err != nil {
		t.Fatal(err)
	}
	return buf.Buf
}
