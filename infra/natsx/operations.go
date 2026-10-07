package natsx

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const downstreamType = "NATS"

func (c *Client) checkOperation(ctx context.Context, subject string) error {
	if ctx == nil {
		return errors.New("natsx: nil context")
	}
	if strings.TrimSpace(subject) == "" {
		return errors.New("natsx: empty subject")
	}
	if c.publishClosed.Load() {
		return ErrClosed
	}
	return ctx.Err()
}

// Publish 向 Core NATS 发送消息；成功只表示客户端已接受写入。
// headers 可省略或传入一个 Header（可为 nil），多传返回参数错误；data 允许 nil。
// data 和提供的 Header 在调用期间须独占；Header 非 nil 时原地写入追踪 ID，已写入的字段不回滚。
func (c *Client) Publish(ctx context.Context, subject string, data []byte, headers ...nats.Header) error {
	if err := c.checkOperation(ctx, subject); err != nil {
		return err
	}
	if len(headers) > 1 {
		return errors.New("natsx: at most one publish header is allowed")
	}
	begin := time.Now()
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
	}
	if len(headers) == 1 {
		msg.Header = headers[0]
	}
	setMessageLogID(ctx, msg)
	err := c.nc.PublishMsg(msg)
	c.logResult(ctx, "corePublish", subject, msg, nil, begin, err, nil)
	return err
}

// Request 向 Core NATS 发送请求并等待响应，由 ctx 控制等待期限。
// headers 可省略或传入一个 Header（可为 nil），多传返回参数错误；data 允许 nil。
// data 和提供的 Header 在调用期间须独占；Header 非 nil 时原地写入追踪 ID，已写入的字段不回滚。
func (c *Client) Request(ctx context.Context, subject string, data []byte, headers ...nats.Header) (*nats.Msg, error) {
	if err := c.checkOperation(ctx, subject); err != nil {
		return nil, err
	}
	if len(headers) > 1 {
		return nil, errors.New("natsx: at most one request header is allowed")
	}
	begin := time.Now()
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
	}
	if len(headers) == 1 {
		msg.Header = headers[0]
	}
	setMessageLogID(ctx, msg)
	reply, err := c.nc.RequestMsgWithContext(ctx, msg)
	c.logResult(ctx, "coreRequest", subject, msg, reply, begin, err, nil)
	return reply, err
}

// PublishJetStream 同步发布并等待服务端 PubAck；messageID 必须非空，不自动生成。
// 同一业务事件重试时复用 messageID，不同事件使用不同 ID；去重仅在同一 Stream 的去重窗口内生效。
// headers 可省略或传入一个 Header（可为 nil），多传返回参数错误；data 允许 nil。
// data 和提供的 Header 在调用期间须独占；Header 非 nil 时原地写入追踪 ID 并覆盖 Nats-Msg-Id，已写入的字段不回滚。
func (c *Client) PublishJetStream(ctx context.Context, subject string, data []byte, messageID string, headers ...nats.Header) (*jetstream.PubAck, error) {
	if err := c.checkOperation(ctx, subject); err != nil {
		return nil, err
	}
	if messageID == "" {
		return nil, errors.New("natsx: empty message ID")
	}
	if len(headers) > 1 {
		return nil, errors.New("natsx: at most one JetStream publish header is allowed")
	}
	begin := time.Now()
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
	}
	if len(headers) == 1 {
		msg.Header = headers[0]
	}
	setMessageLogID(ctx, msg)
	ack, err := c.js.PublishMsg(ctx, msg, jetstream.WithMsgID(messageID))
	c.logResult(ctx, "jetStreamPublish", subject, msg, nil, begin, err, ack)
	return ack, err
}

// Subscribe 仅在应用启动阶段创建 Core 普通订阅，每个匹配的订阅者都会收到消息。
// 运行期间保持订阅关系固定，由 Client.Close 统一排空；自行 Drain/Unsubscribe 后不保证排空及在途回调等待。
// handler 的 ctx 由 Client 管理，Close 排空期间保持有效，关闭完成或超时后取消。
func (c *Client) Subscribe(subject string, handler func(context.Context, *nats.Msg)) (*nats.Subscription, error) {
	return c.subscribe(subject, "", handler)
}

// QueueSubscribe 仅在应用启动阶段创建 Core 队列订阅，queue 必须非空。
// 同一队列组中每条消息只交给一个订阅者；运行期间保持订阅关系固定，由 Client.Close 统一排空。
// 自行 Drain/Unsubscribe 后不保证排空及在途回调等待；handler 的 ctx 生命周期与 Subscribe 相同。
func (c *Client) QueueSubscribe(subject, queue string, handler func(context.Context, *nats.Msg)) (*nats.Subscription, error) {
	if strings.TrimSpace(queue) == "" {
		return nil, errors.New("natsx: empty queue")
	}
	return c.subscribe(subject, queue, handler)
}

func (c *Client) subscribe(subject, queue string, handler func(context.Context, *nats.Msg)) (*nats.Subscription, error) {
	if strings.TrimSpace(subject) == "" || handler == nil {
		return nil, errors.New("natsx: empty subject or nil handler")
	}
	ctx := c.handlerCtx
	wrapped := func(msg *nats.Msg) {
		msgCtx := MessageContext(ctx, msg.Header)
		begin := time.Now()
		handler(msgCtx, msg)
		c.logResult(msgCtx, "coreCallback", msg.Subject, msg, nil, begin, nil, nil)
	}
	c.consumerMu.Lock()
	defer c.consumerMu.Unlock()
	if c.closing.Load() {
		return nil, ErrClosed
	}
	begin := time.Now()
	var (
		sub *nats.Subscription
		err error
	)
	if queue == "" {
		sub, err = c.nc.Subscribe(subject, wrapped)
	} else {
		sub, err = c.nc.QueueSubscribe(subject, queue, wrapped)
	}
	if err != nil {
		c.logResult(ctx, "coreSubscribe", subject, nil, nil, begin, err, nil)
		return nil, err
	}
	c.subscriptions = append(c.subscriptions, sub)
	c.logResult(ctx, "coreSubscribe", subject, nil, nil, begin, nil, nil)
	return sub, nil
}

func (c *Client) logResult(ctx context.Context, action, subject string, msg, reply *nats.Msg, begin time.Time, err error, ack *jetstream.PubAck) {
	elapsed := time.Since(begin)
	// 出站耗时独立于日志级别；消费回调是自身处理，不计入下游调用。
	switch action {
	case "corePublish", "coreRequest", "jetStreamPublish":
		logit.AddDownstreamDurationAuto(ctx, c.durationPrefix, elapsed)
	}
	level := logit.InfoLevel
	if err != nil {
		level = logit.ErrorLevel
	}
	if !logit.LoggerFromContext(ctx).Enabled(level) {
		return
	}
	details := map[string]any{
		"operation": action,
		"subject":   subject,
	}
	appendMessageDetails(details, "request", msg)
	appendMessageDetails(details, "response", reply)
	if err != nil {
		details["err"] = err.Error()
	}
	if ack != nil {
		details["ack"] = ack
	}
	fields := logit.DownstreamFields(downstreamType, c.name, elapsed, details)
	logit.Output(ctx, level, 1, downstreamType, fields...)
}

func appendMessageDetails(details map[string]any, prefix string, msg *nats.Msg) {
	if msg == nil {
		return
	}
	headers := msg.Header
	if headers == nil {
		headers = nats.Header{}
	}
	details[prefix+"_headers"] = headers
	// 非 UTF-8 正文不能直接作为 JSON 字符串保存，否则无效字节会被替换。
	if utf8.Valid(msg.Data) {
		details[prefix+"_body"] = string(msg.Data)
		details[prefix+"_body_encoding"] = "utf-8"
	} else {
		details[prefix+"_body"] = base64.StdEncoding.EncodeToString(msg.Data)
		details[prefix+"_body_encoding"] = "base64"
	}
}
