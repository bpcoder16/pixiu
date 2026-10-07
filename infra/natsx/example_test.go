package natsx_test

import (
	"context"
	"errors"
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

func ExampleClient_ConsumeWithWorkers() {
	stop, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stopSignals()
	// 停机信号只通知主协程调用 Close，消息处理 ctx 由 Client 自己管理。
	ctx := context.Background()
	// 启动阶段：创建连接，准备 Stream/Consumer 并完成全部消费注册。
	client, err := natsx.New(natsx.Config{
		Name: "orders",
		URLs: []string{"nats://127.0.0.1:4222"},
	})
	if err != nil {
		logit.Error(ctx, "NATS 初始化失败")
		return
	}
	defer func() {
		if err := client.Close(); err != nil {
			logit.Error(ctx, "NATS 关闭失败")
		}
	}()

	// Stream/Consumer 管理是实际网络操作，仍单独使用 ctx 设置本次 RPC 的期限。
	setupCtx, cancelSetup := context.WithTimeout(ctx, 5*time.Second)
	defer cancelSetup()
	_, err = client.JetStream().CreateOrUpdateStream(setupCtx, jetstream.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.created"},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		logit.Error(ctx, "创建 Stream 失败")
		return
	}
	consumer, err := client.JetStream().CreateOrUpdateConsumer(setupCtx, "ORDERS", jetstream.ConsumerConfig{
		Durable:   "orders-worker",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		logit.Error(ctx, "创建 Consumer 失败")
		return
	}
	cancelSetup()
	// 启动固定数量的 worker；成功后即可执行回调，依赖资源需预先就绪。
	handle, err := client.ConsumeWithWorkers(consumer, natsx.ConsumeConfig{
		Workers:     5,
		MaxMessages: 5,
		ErrorHandler: func(_ jetstream.ConsumeContext, err error) {
			// 模块已记录错误；应用可按需告警或通知主协程退出，不在这里调用 Close。
			if errors.Is(err, jetstream.ErrConsumerDeleted) {
				stopSignals()
			}
		},
	}, func(msgCtx context.Context, msg jetstream.Msg) {
		// 此处执行实际业务；只有处理成功后才 ACK。业务失败时自行决定 NAK/Term 等策略。
		logit.Info(msgCtx, "处理订单消息", logit.Str("subject", msg.Subject()))
		if err := msg.Ack(); err != nil {
			logit.Error(msgCtx, "消息 ACK 失败")
		}
	})
	if err != nil {
		logit.Error(ctx, "启动消费失败")
		return
	}
	// 运行阶段：保持消费关系固定，处理消息并等待停机信号。
	select {
	case <-stop.Done():
	case <-handle.Closed():
		// 消费提前终止时结束服务，交给应用的重启策略处理。
	}
	// 关闭阶段：若还运行着外部生产者，先停止并等待，再返回执行 client.Close。
	// Client.Close 排空消费并取消消息处理 ctx；日志由应用在最后关闭。
}
