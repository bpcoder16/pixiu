package natsx

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrClosed       = errors.New("natsx: client closing")
	ErrCloseTimeout = errors.New("natsx: close timeout")
	ErrConnect      = errors.New("natsx: connect failed")
)

// Config 是实例式 NATS 连接配置；零超时使用模块默认值。
type Config struct {
	// Name 是连接名称，同时用于日志标识；不能为空或仅含空白。
	Name string
	// URLs 是服务器地址列表；至少提供一个地址，且每项不能为空或仅含空白。
	URLs []string
	// Username 与 Password 配对，允许空密码；只有 Password 时无效。
	// 两者均为空时可通过官方选项使用动态用户名/密码或其他认证；显式配置时不可与认证选项或 URL 凭据混用。
	Username string
	// Password 是 Username 对应的密码；凭据原样传递，不裁剪空白。
	Password string
	// ConnectTimeout 是建立连接的超时；零值默认 5 秒。
	ConnectTimeout time.Duration
	// DrainTimeout 仅配置底层 NATS 连接的 drain 超时；零值默认 10 秒。
	// Client 的消费排空及其他关闭等待统一由 CloseTimeout 限制。
	DrainTimeout time.Duration
	// CloseTimeout 是整体关闭流程的超时；零值默认 20 秒，超时后强制关闭连接。
	CloseTimeout time.Duration
	// JetStream 配置基于当前连接创建的默认 JetStream 上下文。
	JetStream JetStreamConfig
}

// JetStreamConfig 是默认 JetStream 上下文的配置；不开放完整的 jetstream.JetStreamOpt。
type JetStreamConfig struct {
	// PublishAsyncTimeout 是异步发布等待服务端 PubAck 的超时；零值默认 5 秒，负值无效。
	// 仅影响默认上下文的异步发布，不影响同步发布或 Consumer 的 AckWait。
	PublishAsyncTimeout time.Duration
}

// Client 持有一个 NATS 连接、默认 JetStream 上下文及启动阶段登记的消费句柄。
type Client struct {
	// name 是连接名称，用于状态与操作日志标识。
	name string
	// durationPrefix 是出站调用登记请求级耗时的前缀，初始化后固定。
	durationPrefix string
	// nc 是底层 NATS 连接，供 Core 操作与 JetStream 共用。
	nc *nats.Conn
	// js 是基于 nc 创建的默认 JetStream 上下文。
	js jetstream.JetStream
	// closeTimeout 限制整体关闭流程的等待时间。
	closeTimeout time.Duration
	// handlerCtx 是消息回调的生命周期上下文；临时断连和重连不取消它。
	handlerCtx context.Context
	// cancelHandlers 在初始化失败、连接永久关闭或关闭流程结束时取消消息回调上下文。
	cancelHandlers context.CancelFunc
	// closedEvent 由 Pixiu 的连接关闭回调关闭，供 Close 等待。
	closedEvent chan struct{}
	// closing 标记关闭流程已开始，阻止新增订阅与消费句柄。
	closing atomic.Bool
	// publishClosed 在消费排空后或关闭流程结束时置位，阻止新增发布与请求。
	publishClosed atomic.Bool
	// consumerMu 串行化启动阶段的句柄登记与关闭时的摘取，避免交错时遗漏句柄。
	consumerMu sync.Mutex
	// subscriptions 保留启动阶段登记的 Core 订阅，供 Close 统一排空并等待。
	subscriptions []*nats.Subscription
	// consumers 保留启动阶段登记的 worker 消费句柄，Closed 包含全部 handler 的完成。
	consumers []jetstream.ConsumeContext
	// closeOnce 保证并发或重复调用 Close 时只执行一次关闭流程。
	closeOnce sync.Once
	// closeErr 保存首次关闭流程的结果，由 closeOnce 保证后续调用安全读取。
	closeErr error
}

// New 在应用启动阶段建立连接并创建默认 JetStream 上下文；TLS 和其他认证方式通过官方选项配置。
// 默认在意外断连后持续重连，心跳间隔为 20 秒，重连发送缓冲为 16 MiB。
// 可通过 nats.MaxReconnects、nats.PingInterval、nats.ReconnectBufSize 等选项覆盖。
// 服务器地址只能通过 Config.URLs 设置；Config 的 Name 和超时优先于同名 nats.Option 设置。
// 固定用户名/密码需使用 Config；未配置时可通过 nats.UserInfoHandler 动态获取。
// Config 认证字段非空时，拒绝混用官方认证选项或 URL 凭据；凭据原样传递。
// DisconnectedErrCB、DisconnectedCB、ReconnectedCB、ClosedCB、AsyncErrorCB 由 Pixiu 管理；
// 调用方选项设置上述任一非 nil 回调时，在拨号前返回错误。
func New(cfg Config, options ...nats.Option) (*Client, error) {
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, errors.New("natsx: empty connection name")
	}
	if len(cfg.URLs) == 0 {
		return nil, errors.New("natsx: no server URLs")
	}
	for _, serverURL := range cfg.URLs {
		if strings.TrimSpace(serverURL) == "" {
			return nil, errors.New("natsx: empty server URL")
		}
	}
	if cfg.ConnectTimeout < 0 || cfg.DrainTimeout < 0 || cfg.CloseTimeout < 0 {
		return nil, errors.New("natsx: negative timeout")
	}
	if cfg.JetStream.PublishAsyncTimeout < 0 {
		return nil, errors.New("natsx: negative JetStream.PublishAsyncTimeout")
	}
	if cfg.Password != "" && cfg.Username == "" {
		return nil, errors.New("natsx: password requires username")
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = 10 * time.Second
	}
	if cfg.CloseTimeout == 0 {
		cfg.CloseTimeout = 20 * time.Second
	}
	if cfg.JetStream.PublishAsyncTimeout == 0 {
		cfg.JetStream.PublishAsyncTimeout = 5 * time.Second
	}
	c := &Client{
		name:           cfg.Name,
		durationPrefix: downstreamType + "_" + cfg.Name,
		closeTimeout:   cfg.CloseTimeout,
		closedEvent:    make(chan struct{}),
	}
	opts := nats.GetDefaultOptions()
	// 常驻服务意外断连后持续重连；主动关闭及官方认证失败终止规则仍然有效。
	opts.MaxReconnect = -1
	// 缩短依赖心跳发现失活连接的等待时间，后续调用方选项可覆盖这些默认值。
	opts.PingInterval = 20 * time.Second
	// 每条连接重连时按需缓冲待发送消息，增加短暂断连的缓冲余量。
	opts.ReconnectBufSize = 16 * 1024 * 1024
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&opts); err != nil {
			return nil, fmt.Errorf("natsx: invalid connection option: %w", err)
		}
	}
	// 地址统一由 Config.URLs 提供，拒绝选项中的地址，避免调用方配置被静默覆盖。
	if opts.Url != "" || len(opts.Servers) > 0 {
		return nil, errors.New("natsx: server URL options are unsupported; use Config.URLs")
	}
	// RetryOnFailedConnect 默认 false；禁止首次连接失败后转入后台重试，
	// 保证 New 成功返回前已建立连接；建立连接后的自动重连不受此限制。
	if opts.RetryOnFailedConnect {
		return nil, errors.New("natsx: RetryOnFailedConnect is unsupported")
	}
	// NoCallbacksAfterClientClose 默认 false；启用后主动关闭连接不再触发连接关闭回调。
	// Close 依赖 ClosedCB 取消消息上下文并关闭 closedEvent；禁用会使关闭完成通知缺失，
	// 导致连接已关闭仍可能等待至超时，因此禁止启用。
	if opts.NoCallbacksAfterClientClose {
		return nil, errors.New("natsx: NoCallbacksAfterClientClose is unsupported")
	}
	// 连接日志及关闭通知由 Pixiu 统一管理，不接受或串联用户连接回调。
	if opts.DisconnectedErrCB != nil || opts.DisconnectedCB != nil ||
		opts.ReconnectedCB != nil || opts.ClosedCB != nil || opts.AsyncErrorCB != nil {
		return nil, errors.New("natsx: connection lifecycle and async error callbacks are managed by pixiu")
	}
	if opts.User != "" || opts.Password != "" {
		return nil, errors.New("natsx: username/password options are unsupported; use Config")
	}
	if cfg.Username != "" {
		// 动态用户名/密码回调与 Config 的固定凭据互斥，避免认证来源冲突。
		if opts.UserInfo != nil || opts.Token != "" || opts.TokenHandler != nil ||
			opts.UserJWT != nil || opts.Nkey != "" || opts.SignatureCB != nil {
			return nil, errors.New("natsx: config authentication conflicts with connection options")
		}
		// 官方客户端优先使用 URL 内嵌凭据；拒绝混用，避免显式配置被静默覆盖。
		for _, serverURL := range cfg.URLs {
			if !strings.Contains(serverURL, "://") {
				serverURL = "nats://" + serverURL
			}
			parsed, err := url.Parse(serverURL)
			if err != nil {
				return nil, fmt.Errorf("natsx: invalid server URL with config authentication: %w", err)
			}
			if parsed.User != nil {
				return nil, errors.New("natsx: config authentication conflicts with URL credentials")
			}
		}
		opts.User = cfg.Username
		opts.Password = cfg.Password
	}
	opts.Servers = append([]string(nil), cfg.URLs...)
	opts.Name = cfg.Name
	opts.Timeout = cfg.ConnectTimeout
	opts.DrainTimeout = cfg.DrainTimeout
	// 消息处理生命周期由 Client 管理，不绑定初始化调用方的请求或超时。
	c.handlerCtx, c.cancelHandlers = context.WithCancel(context.Background())
	// 五个连接回调字段均由 Pixiu 管理；用户自定义已在上方校验中拒绝。
	c.installCallbacks(&opts)
	nc, err := opts.Connect()
	if err != nil {
		c.cancelHandlers()
		// 第一版原样记录并保留底层错误链，错误文本可能包含 URL 或认证凭据。
		c.logState(logit.ErrorLevel, "connect_failed", "", err)
		return nil, fmt.Errorf("%w: %q: %w", ErrConnect, cfg.Name, err)
	}
	c.nc = nc
	js, err := jetstream.New(nc,
		jetstream.WithPublishAsyncTimeout(cfg.JetStream.PublishAsyncTimeout),
		jetstream.WithPublishAsyncErrHandler(func(_ jetstream.JetStream, msg *nats.Msg, err error) {
			subject := ""
			if msg != nil {
				subject = msg.Subject
			}
			c.logState(logit.ErrorLevel, "async_publish_error", subject, err)
		}),
	)
	if err != nil {
		// 初始化失败后的主动清理不应记录意外断连告警。
		c.closing.Store(true)
		c.cancelHandlers()
		nc.Close()
		return nil, fmt.Errorf("natsx: create JetStream context: %w", err)
	}
	c.js = js
	c.logState(logit.InfoLevel, "connected", "", nil)
	return c, nil
}

func (c *Client) installCallbacks(opts *nats.Options) {
	// DisconnectedErrCB：当前连接断开时触发，如网络中断、服务端关闭或心跳超时；后续可能重连。
	// 主动关闭也可能触发且 err 为 nil；Client.Close 期间通过 closing 标记抑制断连告警。
	// 使用此回调后，旧版 DisconnectedCB 保持 nil。
	opts.DisconnectedErrCB = func(_ *nats.Conn, err error) {
		if !c.closing.Load() {
			c.logState(logit.WarnLevel, "disconnected", "", err)
		}
	}
	// ReconnectedCB：断连后成功重新连接服务器时触发；首次连接成功不走此回调。
	opts.ReconnectedCB = func(_ *nats.Conn) {
		c.logState(logit.InfoLevel, "reconnected", "", nil)
	}
	// ClosedCB：连接永久关闭、不再重连时触发，如主动 Close、Drain 完成或重连终止。
	// 在此取消消息处理上下文，并通过 closedEvent 通知关闭流程已完成。
	opts.ClosedCB = func(nc *nats.Conn) {
		// 连接永久关闭也应通知仍在执行的回调；临时断连和重连不取消。
		c.cancelHandlers()
		c.logState(logit.InfoLevel, "closed", "", nc.LastError())
		// 官方客户端对同一连接最多触发一次 ClosedCB；通知通道仅在此关闭。
		close(c.closedEvent)
	}
	// AsyncErrorCB：NATS 运行时异步错误通知，如慢消费者、发布/订阅权限不足或后台写入失败。
	// sub 可能为 nil；此回调不代表连接必然断开，JetStream 异步发布失败另由上方专用回调处理。
	opts.AsyncErrorCB = func(_ *nats.Conn, sub *nats.Subscription, err error) {
		subject := ""
		if sub != nil {
			subject = sub.Subject
		}
		c.logState(logit.ErrorLevel, "async_error", subject, err)
	}
}

// Conn 返回原生连接；直接操作它不会获得封装层的追踪和结果日志。
// 不得通过原生 setter 或修改 Opts 覆盖 Pixiu 管理的连接回调；可通过 StatusChanged 观察连接状态。
func (c *Client) Conn() *nats.Conn { return c.nc }

// JetStream 返回默认上下文；直接操作它不会获得封装层的结果日志。
func (c *Client) JetStream() jetstream.JetStream { return c.js }

// Close 统一排空启动阶段登记的订阅与消费，期间拒绝新增注册，允许回调继续发布与请求。
// 消费排空后停止发布，等待异步 PubAck，最后 drain 连接。
// 所有阶段共用 CloseTimeout 整体期限；DrainTimeout 仅用于底层连接 drain。
// 关闭完成或超时后取消消息处理 ctx；不强制终止未响应取消的业务函数。
// 调用方应先停生产者，最后关闭日志；不得在消费回调中调用 Close。
func (c *Client) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.close() })
	return c.closeErr
}

func (c *Client) close() error {
	// 各关闭阶段共用同一计时器，避免阶段切换时重置整体期限。
	timer := time.NewTimer(c.closeTimeout)
	defer timer.Stop()
	defer c.cancelHandlers()
	defer c.publishClosed.Store(true)
	c.consumerMu.Lock()
	c.closing.Store(true)
	subscriptions := c.subscriptions
	c.subscriptions = nil
	consumers := c.consumers
	c.consumers = nil
	c.consumerMu.Unlock()
	// 单独排空业务 Core 订阅，保留请求响应与异步 PubAck 所需的内部订阅。
	// 连接仍处于正常状态，回调首次发起 Request 也可以创建响应订阅。
	for _, sub := range subscriptions {
		// 已停止的订阅或已关闭的连接无需再排空；完成状态由后续等待统一判断。
		_ = sub.Drain()
	}
	for _, consumer := range consumers {
		consumer.Drain()
	}
	for _, sub := range subscriptions {
		// 原生通知会补发当前关闭状态，兼容提前停止的订阅，且不占用用户关闭回调。
		select {
		case <-sub.StatusChanged(nats.SubscriptionClosed):
		case <-timer.C:
			return c.forceClose()
		}
	}
	for _, consumer := range consumers {
		select {
		case <-consumer.Closed():
		case <-timer.C:
			return c.forceClose()
		}
	}
	c.publishClosed.Store(true)
	select {
	case <-c.js.PublishAsyncComplete():
	case <-timer.C:
		return c.forceClose()
	}
	if !c.nc.IsClosed() {
		if err := c.nc.Drain(); err != nil {
			c.nc.Close()
			return fmt.Errorf("natsx: drain: %w", err)
		}
	}
	select {
	case <-c.closedEvent:
	case <-timer.C:
		return c.forceClose()
	}
	// 仅记录排空超时及通用超时；其他运行时错误不在此重复记录，均不作为关闭结果返回。
	if err := c.nc.LastError(); errors.Is(err, nats.ErrDrainTimeout) || errors.Is(err, nats.ErrTimeout) {
		c.logState(logit.ErrorLevel, "close_error", "", err)
	}
	return nil
}

func (c *Client) forceClose() error {
	c.nc.Close()
	c.logState(logit.ErrorLevel, "close_timeout", "", ErrCloseTimeout)
	return ErrCloseTimeout
}

func (c *Client) logState(level logit.Level, event, subject string, err error) {
	ctx := context.Background()
	if !logit.LoggerFromContext(ctx).Enabled(level) {
		return
	}
	fields := []logit.Field{
		logit.Str("nats_name", c.name),
		logit.Str("nats_event", event),
	}
	if subject != "" {
		fields = append(fields, logit.Str("subject", subject))
	}
	if err != nil {
		fields = append(fields, logit.Err(err))
	}
	logit.Output(ctx, level, 0, "nats connection", fields...)
}
