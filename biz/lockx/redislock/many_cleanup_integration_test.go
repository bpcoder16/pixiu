package redislock

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/lockx"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/redis/go-redis/v9"
)

// 使用临时 ACL 用户制造真实命令错误，不修改默认用户；只在显式授权的测试实例运行。
func TestManyRedisCleanupIntegration(t *testing.T) {
	addr := os.Getenv("PIXIU_REDIS_TEST_ADDR")
	if addr == "" || os.Getenv("PIXIU_REDIS_TEST_ACL") != "1" {
		t.Skip("设置 PIXIU_REDIS_TEST_ADDR 和 PIXIU_REDIS_TEST_ACL=1 后运行真实 Redis ACL 清理测试")
	}
	admin := redis.NewClient(&redis.Options{
		Addr:         addr,
		MaxRetries:   -1,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	t.Cleanup(func() {
		_ = admin.Close()
	})
	ctx := context.Background()
	for _, tc := range []struct {
		name          string
		conflict      bool
		writeError    bool
		cleanupError  string
		withoutExists bool
	}{
		{name: "成功不需要EXISTS权限", withoutExists: true},
		{name: "竞争后清理全部已获取锁", conflict: true},
		{name: "竞争后GET失败仍清理其余锁", conflict: true, cleanupError: "get"},
		{name: "竞争后DEL失败仍清理其余锁", conflict: true, cleanupError: "del"},
		{name: "写入错误后清理全部已获取锁", writeError: true},
		{name: "同时保留写入与清理错误", writeError: true, cleanupError: "del"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := rand.Text()
			prefix := "pixiu:lockx:cleanup:" + id + ":"
			keys := []string{prefix + "a", prefix + "b", prefix + "c", prefix + "last"}
			username, password := "pixiu-lock-test-"+id, rand.Text()
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := admin.Del(cleanup, keys...).Err(); err != nil {
					t.Error(err)
				}
				if err := admin.Do(cleanup, "ACL", "DELUSER", username).Err(); err != nil {
					t.Error(err)
				}
			})
			args := []any{"ACL", "SETUSER", username, "on", ">" + password, "+ping", "+hello", "+eval", "~" + prefix + "*"}
			if !tc.withoutExists {
				args = append(args, "+exists")
			}
			for _, command := range []string{"set", "get", "del"} {
				allowed := keys
				if command == "set" && tc.writeError {
					allowed = keys[:3]
				} else if command == tc.cleanupError {
					allowed = keys[1:]
				}
				selector := "(+" + command
				for _, key := range allowed {
					selector += " ~" + key
				}
				args = append(args, selector+")")
			}
			if err := admin.Do(ctx, args...).Err(); err != nil {
				t.Fatal(err)
			}
			client, err := redisx.New(redisx.Config{
				Name: t.Name(),
				Options: redis.Options{
					Addr:            addr,
					Username:        username,
					Password:        password,
					Protocol:        2,
					DisableIdentity: true,
					MaxRetries:      -1,
					DialTimeout:     time.Second,
					ReadTimeout:     time.Second,
					WriteTimeout:    time.Second,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = client.Close()
			})
			if tc.conflict {
				if err := admin.Set(ctx, keys[3], "other-owner", time.Minute).Err(); err != nil {
					t.Fatal(err)
				}
			}
			const ttl = 10 * time.Second
			entered := false
			executed, err := lockx.TryDoMany(ctx, mustNew(t, client, ttl), keys, func(context.Context) error {
				entered = true
				return nil
			})
			wantSuccess := !tc.conflict && !tc.writeError
			wantError := tc.writeError || tc.cleanupError != ""
			if executed != wantSuccess || entered != wantSuccess || (err != nil) != wantError {
				t.Fatalf("执行或错误分类不正确: executed=%v entered=%v err=%v", executed, entered, err)
			}
			if wantError {
				var redisErr redis.Error
				if !errors.As(err, &redisErr) {
					t.Fatalf("应保留 Redis 错误链: %v", err)
				}
			}
			if tc.cleanupError != "" && !strings.Contains(err.Error(), "cleanup failed") {
				t.Fatalf("必须报告清理失败: %v", err)
			}
			if tc.writeError && tc.cleanupError != "" && !strings.Contains(err.Error(), "acquire failed") {
				t.Fatalf("不能丢失原始写入失败: %v", err)
			}
			for i, key := range keys {
				wantExists := int64(0)
				if i == 0 && tc.cleanupError != "" || i == 3 && tc.conflict {
					wantExists = 1
				}
				if count, err := admin.Exists(ctx, key).Result(); err != nil || count != wantExists {
					t.Fatalf("key %d 清理结果错误: count=%d want=%d err=%v", i, count, wantExists, err)
				}
			}
			if tc.conflict {
				if value, err := admin.Get(ctx, keys[3]).Result(); err != nil || value != "other-owner" {
					t.Fatalf("不能删除或修改竞争者的锁: %q %v", value, err)
				}
			}
			if tc.cleanupError != "" {
				if remaining, err := admin.PTTL(ctx, keys[0]).Result(); err != nil || remaining <= 0 || remaining > ttl {
					t.Fatalf("清理失败的占用必须仍有 TTL: %v %v", remaining, err)
				}
			}
		})
	}
}
