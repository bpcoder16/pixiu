package natsx

import (
	"context"
	"encoding/base64"
	"errors"
	"slices"
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
// data 在调用期间须独占；Header 深拷贝后写入追踪 ID 并覆盖 Nats-Msg-Id，不修改调用方 Header。
// 调用期间 Header 不得并发修改，可被其他发布调用只读共享。
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
		msg.Header = cloneHeader(headers[0])
	}
	setMessageLogID(ctx, msg)
	ack, err := c.js.PublishMsg(ctx, msg, jetstream.WithMsgID(messageID))
	c.logResult(ctx, "jetStreamPublish", subject, msg, nil, begin, err, ack)
	return ack, err
}

// PublishJetStreamAsync 提交消息并返回原生 future；提交成功不表示服务端已持久化。
// messageID 必须非空，Header 至多一个；复制 data 和 Header，不修改调用方输入。
// 调用期间输入不得并发修改，返回后可复用；future.Msg() 是 SDK 持有的消息，不得修改。
// ctx 仅用于提交前取消检查、追踪和耗时，不撤销已提交消息或控制后续 PubAck 等待。
// 最终结果通过 future.Ok/Err 获取，异步错误同时记录日志；Close 等待已提交的确认。
func (c *Client) PublishJetStreamAsync(ctx context.Context, subject string, data []byte, messageID string, headers ...nats.Header) (jetstream.PubAckFuture, error) {
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
		Data:    slices.Clone(data),
	}
	if len(headers) == 1 {
		msg.Header = cloneHeader(headers[0])
	}
	setMessageLogID(ctx, msg)
	future, err := c.js.PublishMsgAsync(msg, jetstream.WithMsgID(messageID))
	c.logResult(ctx, "jetStreamPublishAsync", subject, msg, nil, begin, err, nil)
	return future, err
}

func cloneHeader(header nats.Header) nats.Header {
	cloned := make(nats.Header, len(header))
	// map 和多值切片都需独立，避免异步确认期间与调用方复用 Header 相互影响。
	for key, values := range header {
		cloned[key] = slices.Clone(values)
	}
	return cloned
}

func (c *Client) subscribeHandler(subject, queue string, handler CoreHandler) (*nats.Subscription, error) {
	if strings.TrimSpace(subject) == "" || handler == nil {
		return nil, errors.New("natsx: empty subject or nil handler")
	}
	ctx := c.handlerCtx
	wrapped := func(msg *nats.Msg) {
		c.consumerMu.Lock()
		if ctx.Err() != nil {
			c.consumerMu.Unlock()
			return
		}
		c.coreHandlers.Add(1)
		c.consumerMu.Unlock()
		defer c.coreHandlers.Done()
		msgCtx := MessageContext(ctx, msg.Header)
		begin := time.Now()
		err := handler(msgCtx, msg)
		c.logResult(msgCtx, "coreCallback", msg.Subject, msg, nil, begin, err, nil)
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
	c.coreSubscriptions = append(c.coreSubscriptions, sub)
	c.logResult(ctx, "coreSubscribe", subject, nil, nil, begin, nil, nil)
	return sub, nil
}

func (c *Client) logResult(ctx context.Context, action, subject string, msg, reply *nats.Msg, begin time.Time, err error, ack *jetstream.PubAck) {
	elapsed := time.Since(begin)
	// 出站耗时独立于日志级别；消费回调是自身处理，不计入下游调用。
	switch action {
	case "corePublish", "coreRequest", "jetStreamPublish", "jetStreamPublishAsync":
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
