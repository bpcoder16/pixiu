package realtime

import (
	"context"
	"errors"
	"net/http"

	"github.com/bpcoder16/pixiu/infra/websocketx"
)

const (
	SubprotocolJSON     = "pixiu.realtime.v1.json"
	SubprotocolProtobuf = "pixiu.realtime.v1.protobuf"
)

var (
	ErrStarted        = errors.New("realtime: already started")
	ErrInvalidMessage = errors.New("realtime: invalid message")
)

// ClientInfo 由 Authenticate 在握手时返回，身份在本次连接期间保持不变。
// 空标识不代表通配或已认证用户。
type ClientInfo struct {
	// UserID 标识业务用户，同时作为传输层的 Identity.Subject。
	UserID string
	// DeviceID 标识客户端设备。同一设备的多条连接各有独立的 Session.ID()。
	DeviceID string
	// Value 可保存业务定义的不可变信息，不得保存凭据、请求或可变共享状态。
	Value any
}

// Handler 处理同连接串行派发的请求；Message 及其正文只在回调期间有效。
// 普通错误只进入脱敏观察；业务显式 ReplyError，避免将内部错误直接下发。
type Handler func(context.Context, *Session, *Message) error

// Config 通过代码注入业务行为，运行后不得修改所引用的共享对象。
type Config struct {
	// Transport 提供容量、心跳、Observe、Logger 等参数。
	// 子协议、接纳门、认证及生命周期回调由 Server 持有，不能在此重复配置。
	// ReadLimit/MaxSendBytes 零值各为 64 KiB，其他零值沿用 websocketx。
	Transport websocketx.Config
	// Authenticate 必填；业务允许匿名时需显式返回空身份。
	Authenticate func(context.Context, *http.Request) (ClientInfo, error)
	// NewState 仅做快速内存初始化，每个连接返回独立的业务状态。
	// State 中的并发访问和资源释放由业务负责，库不做深拷贝。
	NewState func(ClientInfo) any
	OnOpen   func(context.Context, *Session) error
	// OnMessage 必填，避免接入后静默丢弃客户端请求。
	OnMessage Handler
	// OnClose 等待该连接在途回调结束后执行，可释放业务状态。
	// ctx 独立于 Session.Context 的断连取消，受 Transport.OnCloseTimeout（默认 5 秒）及 Server.Cancel 约束。
	// 回调须响应取消；实际返回前 Session.Done/Server.Done 不关闭。
	OnClose func(context.Context, *Session)
	// Revalidate 复核不可变身份；可与 OnOpen/OnMessage 并发。
	Revalidate func(context.Context, ClientInfo) error
}

func validateConfig(cfg Config) error {
	if cfg.Authenticate == nil || cfg.OnMessage == nil {
		return errors.New("realtime: Authenticate and OnMessage are required")
	}
	t := cfg.Transport
	if len(t.Subprotocols) != 0 || t.RequireSubprotocol || t.CheckReady != nil ||
		t.Authenticate != nil || t.OnOpen != nil || t.OnMessage != nil ||
		t.OnDetach != nil || t.OnClose != nil || t.Revalidate != nil {
		return errors.New("realtime: transport protocol and callbacks are managed by Server")
	}
	return nil
}
