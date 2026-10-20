package natsx_test

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/bpcoder16/pixiu/infra/natsx"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func ExampleNew_userPassword() {
	client, err := natsx.New(natsx.Config{
		Name:     "orders",
		URLs:     []string{"nats://127.0.0.1:4222"},
		Username: os.Getenv("NATS_USERNAME"),
		Password: os.Getenv("NATS_PASSWORD"),
	})
	if err != nil {
		logit.Error(context.Background(), "NATS 初始化失败")
		return
	}
	defer client.Close()
}

func ExampleNew_credentials() {
	// 未配置用户名/密码时，可透传官方选项使用其他认证方式。
	client, err := natsx.New(natsx.Config{
		Name: "orders",
		URLs: []string{"nats://127.0.0.1:4222"},
	}, nats.UserCredentials(os.Getenv("NATS_CREDS_FILE")))
	if err != nil {
		logit.Error(context.Background(), "NATS 初始化失败")
		return
	}
	defer client.Close()
}

func ExampleClient_RegisterJetStreamConsumers() {
	client, err := natsx.New(natsx.Config{
		Name: "tasks",
		URLs: []string{"nats://127.0.0.1:4222"},
	})
	if err != nil {
		return
	}
	defer client.Close()
	setup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.RegisterStreams(setup, jetstream.StreamConfig{
		Name:      "TASKS",
		Subjects:  []string{"tasks.sms"},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
	}); err != nil {
		return
	}
	handler := func(ctx context.Context, msg jetstream.Msg) error {
		// 执行业务并返回错误，由组件确认或等待服务端超时重投；不要自行 ACK。
		logit.Info(ctx, "处理短信任务", logit.Str("subject", msg.Subject()))
		return nil
	}
	consumer := natsx.DefaultJetStreamConsumer("TASKS", "send-sms", handler, "tasks.sms")
	consumer.Consume.Workers = 4
	if err := client.RegisterJetStreamConsumers(setup, consumer); err != nil {
		return
	}
	if err := client.RegisterCoreConsumers(setup, natsx.CoreConsumer{
		Name:    "invalidate-cache",
		Subject: "cache.invalidate",
		Handler: func(ctx context.Context, msg *nats.Msg) error {
			logit.Info(ctx, "清理缓存", logit.Str("key", string(msg.Data)))
			return nil
		},
	}); err != nil {
		return
	}
	cancel() // 结束准备阶段，不会取消长期 handler。
	stop, cancelStop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancelStop()
	<-stop.Done()
	// 若有 HTTP/Cron 等生产者，先停止并等待，再通过 defer 排空 Client。
}
