package websocketx

import (
	"context"
	"fmt"
	"time"

	"github.com/bpcoder16/pixiu/logit"
)

// EventKind 标识不包含业务正文的传输事件。
type EventKind string

const (
	EventHandshake      EventKind = "handshake"
	EventOpened         EventKind = "opened"
	EventReceived       EventKind = "received"
	EventHandled        EventKind = "handled"
	EventSent           EventKind = "sent"
	EventSendDropped    EventKind = "send_dropped"
	EventReceiveDropped EventKind = "receive_dropped"
	EventRevalidated    EventKind = "revalidated"
	EventClosed         EventKind = "closed"
)

// Event 仅包含元信息；Err 为脱敏的分类错误，不含用户错误文本或 panic 值。
// EventClosed 的 Code 可包含仅供本地观察的 1006，Err 可包含读取失败、回调超时及收尾钩子的故障分类。
type Event struct {
	Kind         EventKind
	ConnectionID string
	Type         MessageType
	Bytes        int64
	Duration     time.Duration
	Code         int
	Err          error
	Usage        Usage
}

// DrainError 汇总首次 StopInput 后的已知失败，不保留正文或无界错误列表。
// 计数可能覆盖同一消息的不同阶段，不能直接相加作为唯一丢失消息数。
type DrainError struct {
	ReceiveDropped uint64
	ReceiveExpired uint64
	SendDropped    uint64
	SendExpired    uint64
	WriteErrors    uint64
	CallbackErrors uint64
}

func (e *DrainError) Error() string {
	return fmt.Sprintf("%v (receive_dropped=%d receive_expired=%d send_dropped=%d send_expired=%d write_errors=%d callback_errors=%d)", ErrDrainFailed, e.ReceiveDropped, e.ReceiveExpired, e.SendDropped, e.SendExpired, e.WriteErrors, e.CallbackErrors)
}

func (e *DrainError) Unwrap() error { return ErrDrainFailed }

// 停收后的失败分类仅用于关闭结果统计，与 EventKind 的观察事件分类独立。
const (
	drainFailureReceiveDropped = "receive_dropped"
	drainFailureReceiveExpired = "receive_expired"
	drainFailureSendDropped    = "send_dropped"
	drainFailureSendExpired    = "send_expired"
	drainFailureWrite          = "write"
	drainFailureCallback       = "callback"
)

// recordDrainFailure 仅在检查到已停收时累计失败，最终由 Wait 返回 DrainError。
// 停收前的历史失败不计入本次关闭结果；停收前接纳、停收后报告失败的工作仍计入。
// 此处不负责输出日志或事件；未知 kind 保持忽略，各分类计数不等于唯一丢失消息数。
func (e *Endpoint) recordDrainFailure(kind string) {
	if !e.inputStopped.Load() {
		return
	}
	e.failureMu.Lock()
	defer e.failureMu.Unlock()
	switch kind {
	case drainFailureReceiveDropped:
		// 接收派发结束时，仍在接收队列中、尚未交给业务处理的消息被丢弃。
		e.failures.ReceiveDropped++
	case drainFailureReceiveExpired:
		// 入站消息出队时已超过从接纳开始计算的 MessageTimeout，未进入业务回调。
		e.failures.ReceiveExpired++
	case drainFailureSendDropped:
		// 写循环结束时，仍在发送队列中、尚未写入网络的消息被丢弃。
		e.failures.SendDropped++
	case drainFailureSendExpired:
		// 出站消息出队时已达到 ExpiresAt，跳过网络写入。
		e.failures.SendExpired++
	case drainFailureWrite:
		// 数据消息设置写期限或实际写入失败，不包含 Ping/Close 控制帧写入。
		e.failures.WriteErrors++
	case drainFailureCallback:
		// OnOpen/OnMessage/Revalidate 执行失败（含超时、取消或 panic），
		// 或握手、关闭钩子、观察流程中捕获到 panic；不汇总普通认证拒绝。
		e.failures.CallbackErrors++
	}
}

func scope(parent context.Context) context.Context {
	return logit.NewDurationScope(logit.NewContextScope(parent))
}

func eventLevel(event Event) logit.Level {
	if event.Err != nil {
		return logit.WarnLevel
	}
	if event.Kind == EventHandshake || event.Kind == EventClosed {
		return logit.InfoLevel
	}
	return logit.DebugLevel
}

// 没有观察器且日志级别关闭时，纯传输事件不分配额外字段/耗时作用域。
// 业务回调自身的独立作用域与失败计数不受此快速路径影响。
func (e *Endpoint) observeFresh(parent context.Context, event Event) {
	defer func() {
		if recover() != nil {
			e.recordDrainFailure(drainFailureCallback)
		}
	}()
	logger := e.config.Logger
	if logger == nil {
		logger = logit.Default()
	}
	if e.config.Observe == nil && !logger.Enabled(eventLevel(event)) {
		return
	}
	e.observe(scope(parent), event)
}

func (e *Endpoint) observe(ctx context.Context, event Event) {
	// 不递归报告观察器自身的 panic，避免故障放大；调用者的工作登记覆盖观察耗时。
	defer func() {
		if recover() != nil {
			e.recordDrainFailure(drainFailureCallback)
		}
	}()
	if e.config.Observe != nil {
		e.config.Observe(ctx, event)
	}
	logger := e.config.Logger
	if logger == nil {
		logger = logit.Default()
	}
	level := eventLevel(event)
	if logger.Enabled(level) {
		logger.Output(ctx, level, 1, "WebSocket", logit.Str("operation", string(event.Kind)), logit.Str("connectionId", event.ConnectionID), logit.Int("type", int(event.Type)), logit.Int64("bytes", event.Bytes), logit.Int("code", event.Code), logit.Dur("duration", event.Duration), logit.Err(event.Err))
	}
}
