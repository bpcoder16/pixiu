package redislock

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

func TestManySingleKeyUsesSingleProtocolAndOwnsSnapshot(t *testing.T) {
	for _, keys := range [][]string{{" key "}, {" key ", " key "}} {
		t.Run(strings.Join(keys, ","), func(t *testing.T) {
			commands := make(chan []string, 4)
			client := newWireClient(t, -1, func(args []string) string {
				commands <- args
				if strings.EqualFold(args[0], "SET") {
					return "+OK\r\n"
				}
				return ":1\r\n"
			})
			held, acquired, err := mustNew(t, client).TryLockMany(context.Background(), keys)
			if err != nil || !acquired || held == nil {
				t.Fatalf("获取失败: %v %v", acquired, err)
			}
			acquire := <-commands
			if len(acquire) != 6 || !strings.EqualFold(acquire[0], "SET") || acquire[1] != " key " || acquire[3] != "NX" || acquire[4] != "PX" {
				t.Fatalf("一个唯一 key 必须使用 SET NX PX: %v", acquire)
			}
			keys[0] = "caller-mutated"
			if err := held.Unlock(context.Background()); err != nil {
				t.Fatal(err)
			}
			release := <-commands
			if len(release) != 5 || release[1] != unlockScript || release[2] != "1" || release[3] != acquire[1] || release[4] != acquire[2] {
				t.Fatalf("单 key 释放必须保留原始 key 与令牌: %v", release)
			}
			if len(commands) != 0 {
				t.Fatal("不能增加 Redis 命令")
			}
		})
	}
}

func TestManySingleKeyContentionAndCanceledSuccess(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		var calls atomic.Int32
		client := newWireClient(t, -1, func(args []string) string {
			calls.Add(1)
			if strings.EqualFold(args[0], "SET") {
				if canceled {
					cancel()
					return "+OK\r\n"
				}
				return "$-1\r\n"
			}
			return ":1\r\n"
		})
		held, acquired, err := mustNew(t, client).TryLockMany(ctx, []string{"key"})
		cancel()
		wantCalls := int32(1)
		if canceled {
			wantCalls = 2
		}
		if held != nil || acquired || calls.Load() != wantCalls || (err != nil) != canceled || canceled && !errors.Is(err, context.Canceled) {
			t.Fatalf("竞争或已确认成功后的取消处理错误: %v %v %v %d", held, acquired, err, calls.Load())
		}
	}
}
