package cronx

import (
	"time"

	"github.com/go-co-op/gocron/v2"
)

// engineLifecycle 只将引擎停止事件接入关闭同步，其余回调为空以满足引擎接口。
type engineLifecycle struct{ stopped chan struct{} }

func (l engineLifecycle) SchedulerStopped() { close(l.stopped) }

func (engineLifecycle) SchedulerStarted()                                   {}
func (engineLifecycle) SchedulerShutdown()                                  {}
func (engineLifecycle) JobRegistered(gocron.Job)                            {}
func (engineLifecycle) JobUnregistered(gocron.Job)                          {}
func (engineLifecycle) JobStarted(gocron.Job)                               {}
func (engineLifecycle) JobRunning(gocron.Job)                               {}
func (engineLifecycle) JobFailed(gocron.Job, error)                         {}
func (engineLifecycle) JobCompleted(gocron.Job)                             {}
func (engineLifecycle) JobExecutionTime(gocron.Job, time.Duration)          {}
func (engineLifecycle) JobSchedulingDelay(gocron.Job, time.Time, time.Time) {}
func (engineLifecycle) ConcurrencyLimitReached(string, gocron.Job)          {}
