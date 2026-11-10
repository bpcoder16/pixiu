package natsoutbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/bpcoder16/pixiu/biz/outbox"
	"github.com/bpcoder16/pixiu/infra/natsx"
	"github.com/nats-io/nats.go"
	"gorm.io/gorm"
)

const JetStreamKind = "nats.jetstream.publish"
const JetStreamVersion = 1

// JetStreamMessage 使用原始 subject，由 natsx 添加 namespace；消息标识由登记生成的任务 ID 提供。
type JetStreamMessage struct {
	Client    string
	Subject   string
	Data      []byte
	Header    nats.Header
	ExpiresAt time.Time
}

type payload struct {
	Client  string      `json:"client"`
	Subject string      `json:"subject"`
	Data    []byte      `json:"data"`
	Header  nats.Header `json:"header"`
}

func validate(p payload) error {
	if p.Client == "" || strings.TrimSpace(p.Client) != p.Client || !utf8.ValidString(p.Client) {
		return fmt.Errorf("%w: NATS client name", outbox.ErrInvalidArgument)
	}
	if p.Subject == "" || !utf8.ValidString(p.Subject) || strings.IndexFunc(p.Subject, unicode.IsSpace) >= 0 || strings.ContainsAny(p.Subject, "*>\x00") {
		return fmt.Errorf("%w: concrete NATS subject required", outbox.ErrInvalidArgument)
	}
	for token := range strings.SplitSeq(p.Subject, ".") {
		if token == "" {
			return fmt.Errorf("%w: empty NATS subject token", outbox.ErrInvalidArgument)
		}
	}
	return nil
}

// EnqueueJetStreamTx 序列化并登记事务任务，返回自增 ID；不建立连接、不发布也不提交事务。
func EnqueueJetStreamTx(store *outbox.Store, tx *gorm.DB, msg JetStreamMessage) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("%w: nil store", outbox.ErrInvalidArgument)
	}
	p := payload{
		Client:  msg.Client,
		Subject: msg.Subject,
		Data:    msg.Data,
		Header:  msg.Header,
	}
	if err := validate(p); err != nil {
		return 0, err
	}
	// Marshal 同步生成独立快照；调用期间输入不得并发修改，返回后即可复用。
	encoded, err := json.Marshal(p)
	if err != nil {
		return 0, err
	}
	return store.EnqueueTx(tx, outbox.Task{
		Kind:      JetStreamKind,
		Version:   JetStreamVersion,
		Payload:   encoded,
		ExpiresAt: msg.ExpiresAt,
	})
}

// NewJetStreamHandler 通过 natsx.Named 获取并借用客户端；发布等待有效 PubAck 后才返回 nil。
// 名称列表为空、名称非法、未登记或注册表已关闭时，在构造阶段直接 panic。
func NewJetStreamHandler(names ...string) outbox.Handler {
	if len(names) == 0 {
		panic(fmt.Errorf("%w: empty NATS clients", outbox.ErrInvalidArgument))
	}
	snapshot := make(map[string]*natsx.Client, len(names))
	for _, name := range names {
		if name == "" || strings.TrimSpace(name) != name || !utf8.ValidString(name) {
			panic(fmt.Errorf("%w: NATS client name", outbox.ErrInvalidArgument))
		}
		snapshot[name] = natsx.Named(name)
	}
	return func(ctx context.Context, delivery outbox.Delivery) error {
		if ctx == nil || delivery.ID <= 0 || delivery.Kind != JetStreamKind || delivery.Version != JetStreamVersion || len(delivery.Payload) > 1<<20 {
			return fmt.Errorf("%w: JetStream delivery", outbox.ErrInvalidArgument)
		}
		var p payload
		if err := json.Unmarshal(delivery.Payload, &p); err != nil {
			return err
		}
		if err := validate(p); err != nil {
			return err
		}
		client := snapshot[p.Client]
		if client == nil {
			return fmt.Errorf("natsoutbox: missing client %q", p.Client)
		}
		ack, err := client.PublishJetStream(ctx, p.Subject, p.Data, strconv.FormatInt(delivery.ID, 10), p.Header)
		if err != nil {
			return err
		}
		if ack == nil || ack.Stream == "" || ack.Sequence == 0 {
			return errors.New("natsoutbox: missing or invalid PubAck")
		}
		return nil
	}
}
