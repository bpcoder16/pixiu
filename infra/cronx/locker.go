package cronx

import (
	"context"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/go-co-op/gocron/v2"
)

type trackedLocker struct {
	s      *Scheduler
	locker gocron.Locker
}

func (l *trackedLocker) Lock(ctx context.Context, name string) (gocron.Lock, error) {
	if !l.s.admit() {
		return nil, ErrClosed
	}
	lock, err := l.locker.Lock(ctx, name)
	if err != nil {
		defer l.s.work.Done()
		logit.Warn(l.s.ctx, "CronJobSkipped", logit.Str("job_name", name), logit.Str("reason", "lock_not_acquired"), logit.Err(err))
		return nil, err
	}
	// 成功获取的计数转移给锁对象；不能在 Handler 返回或关闭取消时提前结束计数。
	return &trackedLock{
		s:    l.s,
		lock: lock,
		name: name,
	}, nil
}

type trackedLock struct {
	s    *Scheduler
	lock gocron.Lock
	name string
}

func (l *trackedLock) Unlock(ctx context.Context) error {
	defer l.s.work.Done()
	// gocron 停机时会取消 ctx，独立短期限保证仍可发送解锁命令。
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	started := time.Now()
	err := l.lock.Unlock(releaseCtx)
	if err != nil {
		logit.Error(l.s.ctx, "CronUnlockFailed", logit.Str("job_name", l.name), logit.Dur("duration", time.Since(started)), logit.Err(err))
	}
	return err
}
