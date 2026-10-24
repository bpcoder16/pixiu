package websocketx

import (
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/logit"
)

var (
	// ErrClosed 表示连接或 Endpoint 已关闭或当前不接纳该操作。
	ErrClosed = errors.New("websocketx: closed")
	// ErrNilContext 表示调用方传入了 nil context。
	ErrNilContext = errors.New("websocketx: nil context")
	// ErrQueueFull 表示单连接队列额度或 Endpoint 接纳名额不足。
	ErrQueueFull = errors.New("websocketx: connection queue full")
	// ErrBudgetExceeded 表示共享收发预算不足。
	ErrBudgetExceeded = errors.New("websocketx: shared budget exceeded")
	// ErrMessageTooLarge 表示单条消息超过配置的字节上限。
	ErrMessageTooLarge = errors.New("websocketx: message too large")
	// ErrInvalidMessage 表示消息或握手校验失败。
	ErrInvalidMessage = errors.New("websocketx: invalid message")
	// ErrWrongEndpoint 表示预制消息未初始化或属于其他 Endpoint。
	ErrWrongEndpoint = errors.New("websocketx: prepared message belongs to another endpoint")
	// ErrExpired 表示消息已过业务有效期或排队处理期限。
	ErrExpired = errors.New("websocketx: expired")
	// ErrCallback 表示回调失败，用于隐藏原始错误详情。
	ErrCallback = errors.New("websocketx: callback failed")
	// ErrPanic 表示组件捕获到回调 panic。
	ErrPanic = errors.New("websocketx: callback panic")
	// ErrDrainFailed 表示停收后的排空或收尾存在失败，详情见 DrainError。
	ErrDrainFailed = errors.New("websocketx: drain lost work or cleanup failed")
)

// MessageType 只支持完整的 Text 或 Binary 数据消息，控制帧由组件管理。
type MessageType int

const (
	Text   MessageType = 1
	Binary MessageType = 2
)

// Message 的 Data 在 OnMessage 期间只读有效；Send 返回后调用方可复用输入。
// ExpiresAt 为零表示不过期，入站消息不附加业务有效期。
type Message struct {
	Type      MessageType
	Data      []byte
	ExpiresAt time.Time
}

// Identity 是认证产生的不可变快照，Value 不得包含请求对象、原始凭据或可变共享对象。
type Identity struct {
	Subject string
	Value   any
}

// Config 配置原始传输，数值零值采用默认值。
// 回调不能同步等待自身或所属 Endpoint 完成；业务自行等待派生的后台工作。
type Config struct {
	// HandshakeTimeout 是认证与升级共用的期限，默认 5 秒。
	HandshakeTimeout time.Duration
	// ReadLimit 限制客户端 → 服务端的单条消息字节数（含解压后），默认 1 MiB。
	ReadLimit int64
	// MaxSendBytes 限制服务端 → 客户端的单条消息正文字节数（按压缩前计算），默认 1 MiB。
	MaxSendBytes int64
	// ReceiveQueueSize 是单连接客户端 → 服务端的累计消息条数上限，默认 16。
	// 包含正在读取组装、排队等待处理和正在派发处理的消息。
	ReceiveQueueSize int
	// ReceiveQueueBytes 是单连接客户端 → 服务端的累计正文字节数上限，默认 4 MiB。
	// 包含读取组装中、排队等待处理和正在派发处理的正文，按解压后字节数计算。
	ReceiveQueueBytes int64
	// SendQueueSize 是单连接服务端 → 客户端的累计消息条数上限，默认 64。
	// 包含排队等待发送和正在写入网络的消息。
	SendQueueSize int
	// SendQueueBytes 是单连接服务端 → 客户端的累计正文字节数上限，默认 4 MiB。
	// 包含排队等待发送和正在写入网络的正文，按压缩前字节数计算。
	SendQueueBytes int64
	// Budget 可供多个 Endpoint 共享；nil 时创建独立的默认预算。
	Budget *Budget
	// MaxPendingHandshakes 是在途握手上限，默认 256。
	MaxPendingHandshakes int
	// MaxConnections 是连接总数上限（含握手和收尾），默认 10000。
	MaxConnections int
	// PingInterval 是主动 Ping 间隔，默认 20 秒，启用心跳时须小于 PongTimeout。
	PingInterval time.Duration
	// PongTimeout 是接收 Pong 的期限，默认 60 秒，每次收到 Pong 后重置。
	PongTimeout time.Duration
	// DisableHeartbeat 默认为 false，即开启心跳。
	// 设为 true 后不主动发送 Ping、不检查 Pong 超时，但仍会回复客户端的 Ping。
	DisableHeartbeat bool
	// WriteTimeout 是出站网络写入期限，默认 10 秒，适用于单条数据消息和主动 Ping。
	// 从实际开始写入时计时，不包含发送队列等待时间；超时或写入失败时发起关闭连接。
	WriteTimeout time.Duration
	// CloseTimeout 是发起连接关闭后发送 Close 控制帧（关闭通知）的期限，默认 1 秒。
	// 通知发送完成、超时或失败后均关闭底层连接；仅控制通知发送，不是整个关闭流程的超时。
	CloseTimeout time.Duration
	// MessageTimeout 是 OnOpen 初始化和入站消息业务处理的期限，默认 30 秒，不是网络读取超时。
	// OnOpen 从开始执行计时；消息从完整接纳至 OnMessage 返回计时，包含接收队列等待时间。
	// 消息过期或回调超时会发起关闭连接；正在执行的回调须响应 ctx 取消，不能被强制终止。
	MessageTimeout time.Duration
	// RevalidateInterval 是资格复核的基准间隔，默认 60 秒。
	RevalidateInterval time.Duration
	// RevalidateTimeout 是单次资格复核期限，默认 5 秒。
	RevalidateTimeout time.Duration
	// AllowedOrigins 是允许的完整 Origin 列表，空列表默认按 scheme/host/port 校验同源。
	AllowedOrigins []string
	// AllowAnyOrigin 允许任意合法 Origin，与非空 AllowedOrigins 互斥。
	AllowAnyOrigin bool
	// RejectMissingOrigin 为 true 时拒绝缺失 Origin 的请求，默认允许。
	RejectMissingOrigin bool
	// Subprotocols 是服务端支持的子协议候选列表，按优先级从高到低排列；客户端也可提供多个候选。
	// 按服务端列表顺序选择第一个双方都支持的子协议，匹配成功时仅选定一个，由 Session.Subprotocol() 返回。
	// 无匹配时的服务端行为由 RequireSubprotocol 决定。
	Subprotocols []string
	// RequireSubprotocol 默认为 false，控制服务端是否强制要求协商出子协议。
	// 为 true 时 Subprotocols 不得为空；客户端未提供候选或无匹配时，在升级前返回 HTTP 400。
	// 为 false 时，无匹配不阻止服务端升级，响应不携带 Sec-WebSocket-Protocol，Session.Subprotocol() 返回空字符串。
	RequireSubprotocol bool
	// EnableCompression 为 true 时协商消息压缩，默认关闭。
	EnableCompression bool
	// CompressionLevel 为零时默认 flate.BestSpeed；仅影响启用压缩的连接。
	CompressionLevel int
	// CheckReady 是可选的快速只读接纳门，在锁外调用；false 返回 HTTP 503。
	CheckReady func() bool
	// Authenticate 在握手时认证并生成身份；nil 时允许匿名连接。
	Authenticate func(context.Context, *http.Request) (Identity, error)
	// OnOpen 在连接建立后、消息处理前执行，返回错误时关闭连接。
	// 单连接回调时序（省略未配置的回调）：
	//
	//	连接建立 -> OnOpen -> OnMessage -> OnMessage -> ...（同连接串行）
	//
	//	任意阶段触发关闭
	//	  +-> OnDetach --------------------------------------------+
	//	  +-> 关闭网络，等待在途业务、读写和复核结束 --------------+-> OnClose -> Done
	//
	// OnDetach 可与 OnOpen/OnMessage 并发；提前关闭时 OnOpen/OnMessage 可能不执行。
	// OnClose 等待两条关闭分支实际结束后执行，返回后才会关闭 Session.Done()。
	OnOpen func(context.Context, *Session) error
	// OnMessage 按连接串行处理完整消息；nil 时丢弃消息。
	OnMessage func(context.Context, *Session, Message) error
	// OnDetach 只做快速内存索引清理，可与 OnOpen 并发，不执行 I/O。
	OnDetach func(*Session)
	// OnClose 在网络和在途工作收尾后执行一次。
	OnClose func(context.Context, *Session)
	// Revalidate 可与消息处理并发，返回错误时关闭连接；nil 时不启用复核。
	Revalidate func(context.Context, *Session) error
	// Observe 同步、并发安全且应快速返回；事件不携带正文或凭据。
	Observe func(context.Context, Event)
	// Logger 为 nil 时使用 logit.Default，组件不拥有或关闭 Logger。
	Logger logit.Logger
}

func normalize(cfg Config) (Config, error) {
	for _, d := range []time.Duration{cfg.HandshakeTimeout, cfg.PingInterval, cfg.PongTimeout, cfg.WriteTimeout, cfg.CloseTimeout, cfg.MessageTimeout, cfg.RevalidateInterval, cfg.RevalidateTimeout} {
		if d < 0 {
			return cfg, errors.New("websocketx: negative timeout")
		}
	}
	for _, n := range []int64{cfg.ReadLimit, cfg.MaxSendBytes, int64(cfg.ReceiveQueueSize), cfg.ReceiveQueueBytes, int64(cfg.SendQueueSize), cfg.SendQueueBytes, int64(cfg.MaxPendingHandshakes), int64(cfg.MaxConnections)} {
		if n < 0 {
			return cfg, errors.New("websocketx: negative limit")
		}
	}
	defaultDuration := func(value *time.Duration, fallback time.Duration) {
		if *value == 0 {
			*value = fallback
		}
	}
	defaultDuration(&cfg.HandshakeTimeout, 5*time.Second)
	defaultDuration(&cfg.PingInterval, 20*time.Second)
	defaultDuration(&cfg.PongTimeout, time.Minute)
	defaultDuration(&cfg.WriteTimeout, 10*time.Second)
	defaultDuration(&cfg.CloseTimeout, time.Second)
	defaultDuration(&cfg.MessageTimeout, 30*time.Second)
	defaultDuration(&cfg.RevalidateInterval, time.Minute)
	defaultDuration(&cfg.RevalidateTimeout, 5*time.Second)
	if !cfg.DisableHeartbeat && cfg.PingInterval >= cfg.PongTimeout {
		return cfg, errors.New("websocketx: PingInterval must be less than PongTimeout")
	}
	if cfg.ReadLimit == 0 {
		cfg.ReadLimit = 1 << 20
	}
	if cfg.MaxSendBytes == 0 {
		cfg.MaxSendBytes = 1 << 20
	}
	if cfg.ReceiveQueueSize == 0 {
		cfg.ReceiveQueueSize = 16
	}
	if cfg.ReceiveQueueBytes == 0 {
		cfg.ReceiveQueueBytes = 4 << 20
	}
	if cfg.SendQueueSize == 0 {
		cfg.SendQueueSize = 64
	}
	if cfg.SendQueueBytes == 0 {
		cfg.SendQueueBytes = 4 << 20
	}
	if cfg.MaxPendingHandshakes == 0 {
		cfg.MaxPendingHandshakes = 256
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 10_000
	}
	if cfg.CompressionLevel == 0 {
		cfg.CompressionLevel = flate.BestSpeed
	}
	if cfg.CompressionLevel < flate.BestSpeed || cfg.CompressionLevel > flate.BestCompression {
		return cfg, errors.New("websocketx: invalid compression level")
	}
	cfg.AllowedOrigins = slices.Clone(cfg.AllowedOrigins)
	for i, origin := range cfg.AllowedOrigins {
		normalized, err := originKey(origin)
		if err != nil {
			return cfg, fmt.Errorf("websocketx: invalid allowed origin at index %d", i)
		}
		cfg.AllowedOrigins[i] = normalized
	}
	if cfg.AllowAnyOrigin && len(cfg.AllowedOrigins) > 0 {
		return cfg, errors.New("websocketx: AllowAnyOrigin conflicts with AllowedOrigins")
	}
	cfg.Subprotocols = slices.Clone(cfg.Subprotocols)
	seen := make(map[string]bool)
	for _, protocol := range cfg.Subprotocols {
		if protocol == "" || seen[protocol] || !validToken(protocol) {
			return cfg, errors.New("websocketx: invalid or duplicate subprotocol")
		}
		seen[protocol] = true
	}
	if cfg.RequireSubprotocol && len(cfg.Subprotocols) == 0 {
		return cfg, errors.New("websocketx: required subprotocol list is empty")
	}
	if cfg.Budget == nil {
		cfg.Budget, _ = NewBudget(BudgetConfig{})
	}
	return cfg, nil
}

func validToken(value string) bool {
	for _, c := range value {
		if c <= 32 || c >= 127 || strings.ContainsRune("()<>@,;:\\\"/[]?={}", c) {
			return false
		}
	}
	return true
}

func originKey(value string) (string, error) {
	u, err := url.Parse(value)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return "", errors.New("invalid origin")
	}
	return u.Scheme + "://" + strings.ToLower(u.Host), nil
}
