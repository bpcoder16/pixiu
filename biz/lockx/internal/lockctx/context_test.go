package lockctx

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

// 模拟已到期限、但取消计时器尚未调度的 context。
type deadlineContext struct {
	context.Context
	deadline time.Time
}

func (c deadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func TestErrCancellationAndDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		now := time.Now()
		for _, tt := range []struct {
			name string
			ctx  context.Context
			want error
		}{
			{
				name: "请求有效且无期限",
				ctx:  context.Background(),
			},
			{
				name: "请求取消",
				ctx:  canceled,
				want: context.Canceled,
			},
			{
				name: "期限未到",
				ctx: deadlineContext{
					Context:  context.Background(),
					deadline: now.Add(time.Second),
				},
			},
			{
				name: "恰好到期且取消尚未调度",
				ctx: deadlineContext{
					Context:  context.Background(),
					deadline: now,
				},
				want: context.DeadlineExceeded,
			},
			{
				name: "已过期且取消尚未调度",
				ctx: deadlineContext{
					Context:  context.Background(),
					deadline: now.Add(-time.Nanosecond),
				},
				want: context.DeadlineExceeded,
			},
			{
				name: "优先保留取消原因",
				ctx: deadlineContext{
					Context:  canceled,
					deadline: now.Add(-time.Second),
				},
				want: context.Canceled,
			},
		} {
			if got := Err(tt.ctx); got != tt.want {
				t.Fatalf("%s: got=%v want=%v", tt.name, got, tt.want)
			}
		}
	})
}
