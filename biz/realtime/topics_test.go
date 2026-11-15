package realtime

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/realtime/protocol"
	"github.com/bpcoder16/pixiu/infra/websocketx"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestTopicSubscriptionsAndPush(t *testing.T) {
	opened := make(chan *Session, 3)
	cfg := baseConfig()
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	server, h := startTestServer(t, cfg)
	jsonClient := dial(t, h, SubprotocolJSON)
	first := await(t, opened)
	pbClient := dial(t, h, SubprotocolProtobuf)
	second := await(t, opened)
	chatClient := dial(t, h, SubprotocolJSON)
	third := await(t, opened)
	awaitTargets(t, server, 3)
	for _, subscription := range []struct {
		session   *Session
		namespace string
		topics    []string
	}{
		{first, "auction", []string{"same", "same", "other"}},
		{first, "chat", []string{"same"}},
		{second, "auction", []string{"same"}},
		{third, "chat", []string{"same"}},
	} {
		if err := server.SubscribeTopics(subscription.session, subscription.namespace, subscription.topics...); err != nil {
			t.Fatal(err)
		}
	}
	assertTopicPush(t, server, "auction", "same", 2)
	responseBody(t, jsonClient, protocol.Kind_EVENT)
	responseBody(t, pbClient, protocol.Kind_EVENT)
	assertTopicPush(t, server, "chat", "same", 2)
	responseBody(t, jsonClient, protocol.Kind_EVENT)
	responseBody(t, chatClient, protocol.Kind_EVENT)

	if err := server.UnsubscribeTopics(first, "auction", "same", "same", "missing"); err != nil {
		t.Fatal(err)
	}
	assertTopicPush(t, server, "auction", "same", 1)
	responseBody(t, pbClient, protocol.Kind_EVENT)
	if err := server.ReplaceTopics(first, "auction", "new", "new"); err != nil {
		t.Fatal(err)
	}
	assertTopicPush(t, server, "auction", "other", 0)
	assertTopicPush(t, server, "auction", "new", 1)
	responseBody(t, jsonClient, protocol.Kind_EVENT)
	if err := server.ReplaceTopics(first, "auction"); err != nil {
		t.Fatal(err)
	}
	assertTopicPush(t, server, "auction", "new", 0)
	assertTopicPush(t, server, "chat", "same", 2)
	responseBody(t, jsonClient, protocol.Kind_EVENT)
	responseBody(t, chatClient, protocol.Kind_EVENT)

	if count, err := server.ClearTopic("chat", "same"); err != nil || count != 2 {
		t.Fatalf("清除 namespace/topic = %d, %v", count, err)
	}
	assertTopicPush(t, server, "chat", "same", 0)
	assertTopicPush(t, server, "auction", "same", 1)
	responseBody(t, pbClient, protocol.Kind_EVENT)
	if count, err := server.ClearTopic("chat", "same"); err != nil || count != 0 {
		t.Fatalf("重复清除应幂等: %d, %v", count, err)
	}
	if err := server.SubscribeTopics(first, "chat", "same"); err != nil {
		t.Fatal(err)
	}
	assertTopicPush(t, server, "chat", "same", 1)
	responseBody(t, jsonClient, protocol.Kind_EVENT)
}

func assertTopicPush(t *testing.T, server *Server, namespace, topic string, count int) {
	t.Helper()
	result, err := server.PushToTopic(t.Context(), namespace, topic, PushMessage{
		Type: "home.refreshed",
		Data: &emptypb.Empty{},
	})
	if err != nil || result != (PushResult{Matched: count, Enqueued: count}) {
		t.Fatalf("推送 %s/%s = %+v, %v，期望 %d 个连接", namespace, topic, result, err, count)
	}
}

func TestTopicLimitIsAtomicAcrossNamespaces(t *testing.T) {
	opened := make(chan *Session, 2)
	cfg := baseConfig()
	cfg.MaxTopicsPerSession = 3
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	server, h := startTestServer(t, cfg)
	client := dial(t, h, SubprotocolJSON)
	session := await(t, opened)
	dial(t, h, SubprotocolJSON)
	other := await(t, opened)
	awaitTargets(t, server, 2)
	if err := server.SubscribeTopics(session, "auction", "a", "b", "a"); err != nil {
		t.Fatal(err)
	}
	if err := server.SubscribeTopics(session, "chat", "a"); err != nil {
		t.Fatal(err)
	}
	if err := server.SubscribeTopics(session, "auction", "a", "b"); err != nil {
		t.Fatalf("已满时重复订阅仍应成功: %v", err)
	}
	for _, update := range []func() error{
		func() error { return server.SubscribeTopics(session, "new", "c") },
		func() error { return server.SubscribeTopics(session, "auction", "a", "c") },
		func() error { return server.ReplaceTopics(session, "auction", "c", "d", "e") },
	} {
		if err := update(); !errors.Is(err, ErrTopicLimitExceeded) {
			t.Fatalf("跨 namespace 总数超限未拒绝: %v", err)
		}
	}
	assertTopicPush(t, server, "auction", "a", 1)
	responseBody(t, client, protocol.Kind_EVENT)
	assertTopicPush(t, server, "auction", "b", 1)
	responseBody(t, client, protocol.Kind_EVENT)
	assertTopicPush(t, server, "auction", "c", 0)
	if err := server.SubscribeTopics(other, "auction", "x", "y", "z"); err != nil {
		t.Fatalf("上限应按连接独立计算: %v", err)
	}
	if err := server.ReplaceTopics(session, "auction", "c", "d", "c"); err != nil {
		t.Fatalf("等量替换不能按新旧集合之和判超限: %v", err)
	}
	if err := server.UnsubscribeTopics(session, "auction", "c"); err != nil {
		t.Fatal(err)
	}
	if err := server.SubscribeTopics(session, "new", "e"); err != nil {
		t.Fatalf("退订后应释放配额: %v", err)
	}
	if _, err := server.ClearTopic("auction", "d"); err != nil {
		t.Fatal(err)
	}
	if err := server.SubscribeTopics(session, "new", "f"); err != nil {
		t.Fatalf("清除 topic 后应释放配额: %v", err)
	}
}

func TestTopicValidationAndDefaultLimit(t *testing.T) {
	cfg := baseConfig()
	cfg.MaxTopicsPerSession = -1
	if _, err := NewServer(cfg); err == nil {
		t.Fatal("负数上限应拒绝")
	}
	cfg.MaxTopicsPerSession = 0
	opened := make(chan *Session, 1)
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	server, h := startTestServer(t, cfg)
	dial(t, h, SubprotocolJSON)
	session := await(t, opened)
	awaitTargets(t, server, 1)
	for _, invalid := range []string{"", " ", "bad*", "a/b", strings.Repeat("x", 129)} {
		for _, change := range []func(*Session, string, ...string) error{
			server.SubscribeTopics, server.UnsubscribeTopics, server.ReplaceTopics,
		} {
			if err := change(session, invalid, "valid"); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("非法 namespace %q: %v", invalid, err)
			}
			if err := change(session, "auction", "valid", invalid); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("非法 topic %q: %v", invalid, err)
			}
		}
		if _, err := server.PushToTopic(t.Context(), invalid, "valid", PushMessage{Type: "x", Data: &emptypb.Empty{}}); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("非法推送 namespace: %v", err)
		}
		if _, err := server.ClearTopic("auction", invalid); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("非法清除 topic: %v", err)
		}
	}
	assertTopicPush(t, server, "auction", "valid", 0)
	topics := make([]string, 501)
	for n := range topics {
		topics[n] = "topic-" + strconv.Itoa(n)
	}
	if err := server.SubscribeTopics(session, "auction", topics...); !errors.Is(err, ErrTopicLimitExceeded) {
		t.Fatalf("默认上限应为 500: %v", err)
	}
	if err := server.SubscribeTopics(session, "auction", topics[:500]...); err != nil {
		t.Fatal(err)
	}
	if err := server.ReplaceTopics(session, "auction"); err != nil {
		t.Fatal(err)
	}
	assertNoTopicSubscriptions(t, server, session)
}

func assertNoTopicSubscriptions(t *testing.T, server *Server, sessions ...*Session) {
	t.Helper()
	server.targets.mu.RLock()
	defer server.targets.mu.RUnlock()
	if len(server.targets.namespaces) != 0 {
		t.Fatal("空 topic/namespace 仍留在索引中")
	}
	for _, session := range sessions {
		if len(session.topics) != 0 {
			t.Fatal("连接仍持有 topic 引用")
		}
	}
}

func TestTopicNamesSharedAndReleased(t *testing.T) {
	opened := make(chan *Session, 2)
	cfg := baseConfig()
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	server, h := startTestServer(t, cfg)
	dial(t, h, SubprotocolJSON)
	first := await(t, opened)
	dial(t, h, SubprotocolJSON)
	second := await(t, opened)
	awaitTargets(t, server, 2)
	for _, session := range []*Session{first, second} {
		if err := server.SubscribeTopics(session, strings.Clone("auction"), strings.Clone("same")); err != nil {
			t.Fatal(err)
		}
	}
	server.targets.mu.RLock()
	shared := server.targets.namespaces["auction"].topics["same"]
	_, firstShares := first.topics[shared]
	_, secondShares := second.topics[shared]
	server.targets.mu.RUnlock()
	if !firstShares || !secondShares {
		t.Fatal("同名订阅应共享 topic 对象及字符串")
	}
	if err := server.ReplaceTopics(first, "auction"); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Close(1000, "done"); err != nil {
		t.Fatal(err)
	}
	assertNoTopicSubscriptions(t, server, first, second)
}

func TestTopicHandlerSubscriptionWaitsForOpen(t *testing.T) {
	for _, protocolName := range []string{SubprotocolJSON, SubprotocolProtobuf} {
		t.Run(protocolName, func(t *testing.T) {
			var server *Server
			opened := make(chan *Session, 1)
			attempts := make(chan error, 3)
			received := make(chan struct{}, 1)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			cfg := baseConfig()
			cfg.OnOpen = func(_ context.Context, s *Session) error {
				for _, change := range []func(*Session, string, ...string) error{
					server.SubscribeTopics, server.UnsubscribeTopics, server.ReplaceTopics,
				} {
					attempts <- change(s, "auction", "sale")
				}
				opened <- s
				<-release
				return nil
			}
			cfg.Transport.Observe = func(_ context.Context, event websocketx.Event) {
				if event.Kind == websocketx.EventReceived {
					received <- struct{}{}
				}
			}
			cfg.RegisterHandlers = testRoute("home", "refresh", func(ctx context.Context, s *Session, m *Message) error {
				if err := server.SubscribeTopics(s, "auction", "sale"); err != nil {
					return err
				}
				if err := server.ReplaceTopics(s, "auction", "sale", "other"); err != nil {
					return err
				}
				if err := server.UnsubscribeTopics(s, "auction", "other"); err != nil {
					return err
				}
				return s.Reply(ctx, m, "home.refreshed", &emptypb.Empty{})
			})
			server, h := startTestServer(t, cfg)
			defer unblock()
			client := dial(t, h, protocolName)
			session := await(t, opened)
			for range 3 {
				if err := await(t, attempts); !errors.Is(err, websocketx.ErrClosed) {
					t.Errorf("OnOpen 中应拒绝订阅变更: %v", err)
				}
			}
			// 确保请求在 OnOpen 尚未返回时已经入队，验证客户端无需另等 ready 通知。
			typ, wire := requestWire(t, protocolName, &emptypb.Empty{})
			if err := client.WriteMessage(typ, wire); err != nil {
				t.Fatal(err)
			}
			await(t, received)
			assertNoTopicSubscriptions(t, server, session)
			assertTopicPush(t, server, "auction", "sale", 0)
			unblock()
			responseBody(t, client, protocol.Kind_RESPONSE)
			assertTopicPush(t, server, "auction", "sale", 1)
			responseBody(t, client, protocol.Kind_EVENT)
			assertTopicPush(t, server, "auction", "other", 0)
		})
	}
}

func TestTopicPendingOpenAndDetach(t *testing.T) {
	var server *Server
	opened := make(chan *Session, 1)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	cfg := baseConfig()
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		if err := server.SubscribeTopics(s, "auction", "sale"); !errors.Is(err, websocketx.ErrClosed) {
			t.Errorf("OnOpen 中应拒绝订阅: %v", err)
		}
		opened <- s
		<-release
		return nil
	}
	server, h := startTestServer(t, cfg)
	defer unblock()
	dial(t, h, SubprotocolJSON)
	session := await(t, opened)
	assertTopicPush(t, server, "auction", "sale", 0)
	if _, err := session.Close(1000, "done"); err != nil {
		t.Fatal(err)
	}
	if err := server.SubscribeTopics(session, "auction", "late"); !errors.Is(err, websocketx.ErrClosed) {
		t.Fatalf("断开后不得重新订阅: %v", err)
	}
	assertNoTopicSubscriptions(t, server, session)
	unblock()
	await(t, session.Done())
	assertTopicPush(t, server, "auction", "sale", 0)
}

func TestTopicDetachDoesNotWaitForOnClose(t *testing.T) {
	opened := make(chan *Session, 1)
	closing := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	cfg := baseConfig()
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	cfg.OnClose = func(context.Context, *Session) {
		close(closing)
		<-release
	}
	server, h := startTestServer(t, cfg)
	defer unblock()
	dial(t, h, SubprotocolJSON)
	session := await(t, opened)
	awaitTargets(t, server, 1)
	if err := server.SubscribeTopics(session, "auction", "sale"); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Close(1000, "done"); err != nil {
		t.Fatal(err)
	}
	await(t, closing)
	assertNoTopicSubscriptions(t, server, session)
	select {
	case <-session.Done():
		t.Fatal("测试应保持 OnClose 尚未结束")
	default:
	}
}

func TestTopicConcurrentChangesAndDetach(t *testing.T) {
	opened := make(chan *Session, 1)
	cfg := baseConfig()
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	server, h := startTestServer(t, cfg)
	dial(t, h, SubprotocolJSON)
	session := await(t, opened)
	awaitTargets(t, server, 1)
	begin := make(chan struct{})
	liveDone := make(chan struct{}, 3)
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	var workers sync.WaitGroup
	for _, namespace := range []string{"auction", "chat", "feed"} {
		workers.Go(func() {
			<-begin
			for step := range 100 {
				// 前半段必须在活跃连接上完成，避免只验证断线后的 ErrClosed 分支。
				if step == 50 {
					liveDone <- struct{}{}
					<-resume
				}
				for _, change := range []func(*Session, string, ...string) error{
					server.SubscribeTopics, server.ReplaceTopics, server.UnsubscribeTopics,
				} {
					if err := change(session, namespace, "a", "b", "a"); err != nil && (step < 50 || !errors.Is(err, websocketx.ErrClosed)) {
						t.Errorf("并发变更: %v", err)
					}
				}
				if _, err := server.ClearTopic(namespace, "a"); err != nil {
					t.Errorf("并发清除: %v", err)
				}
			}
		})
	}
	close(begin)
	for range 3 {
		await(t, liveDone)
	}
	unblock()
	if _, err := session.Close(1000, "done"); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	assertNoTopicSubscriptions(t, server, session)
}

func TestTopicConcurrentLimit(t *testing.T) {
	opened := make(chan *Session, 1)
	cfg := baseConfig()
	cfg.MaxTopicsPerSession = 1
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	server, h := startTestServer(t, cfg)
	dial(t, h, SubprotocolJSON)
	session := await(t, opened)
	awaitTargets(t, server, 1)
	begin := make(chan struct{})
	results := make(chan error, 2)
	for _, namespace := range []string{"auction", "chat"} {
		go func() {
			<-begin
			results <- server.SubscribeTopics(session, namespace, "same")
		}()
	}
	close(begin)
	successes := 0
	for range 2 {
		if err := await(t, results); err == nil {
			successes++
		} else if !errors.Is(err, ErrTopicLimitExceeded) {
			t.Fatal(err)
		}
	}
	if successes != 1 {
		t.Fatalf("并发跨 namespace 订阅绕过上限: 成功 %d 次", successes)
	}
}

func TestTopicOpenFailureLeavesNoSubscriptions(t *testing.T) {
	var server *Server
	opened := make(chan *Session, 1)
	cfg := baseConfig()
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		if err := server.SubscribeTopics(s, "auction", "sale"); !errors.Is(err, websocketx.ErrClosed) {
			t.Errorf("OnOpen 中应拒绝订阅: %v", err)
		}
		opened <- s
		return errors.New("初始化失败")
	}
	server, h := startTestServer(t, cfg)
	dial(t, h, SubprotocolJSON)
	session := await(t, opened)
	await(t, session.Done())
	assertNoTopicSubscriptions(t, server, session)
	assertTopicPush(t, server, "auction", "sale", 0)
}

func TestTopicStopDrainsAcceptedPush(t *testing.T) {
	opened := make(chan *Session, 1)
	cfg := baseConfig()
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	server, h := startTestServer(t, cfg)
	client := dial(t, h, SubprotocolProtobuf)
	session := await(t, opened)
	awaitTargets(t, server, 1)
	if err := server.SubscribeTopics(session, "auction", "sale", "cleanup"); err != nil {
		t.Fatal(err)
	}
	body := &blockedPushBody{
		Empty:   &emptypb.Empty{},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	unblock := sync.OnceFunc(func() { close(body.release) })
	defer unblock()
	done := make(chan error, 1)
	go func() {
		_, err := server.PushToTopic(t.Context(), "auction", "sale", PushMessage{
			Type: "home.refreshed",
			Data: body,
		})
		done <- err
	}()
	await(t, body.entered)
	if err := server.Stop(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Session, string, ...string) error{
		server.SubscribeTopics, server.UnsubscribeTopics, server.ReplaceTopics,
	} {
		if err := change(session, "auction", "sale"); !errors.Is(err, websocketx.ErrClosed) {
			t.Fatalf("停收后接纳订阅变更: %v", err)
		}
	}
	if _, err := server.PushToTopic(t.Context(), "auction", "sale", PushMessage{}); !errors.Is(err, websocketx.ErrClosed) {
		t.Fatalf("停收后接纳新推送: %v", err)
	}
	if count, err := server.ClearTopic("auction", "cleanup"); err != nil || count != 1 {
		t.Fatalf("停收期间应允许清理: %d, %v", count, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := server.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("未等待已接纳推送: %v", err)
	}
	unblock()
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
	// 已接纳推送仍随停机排空完成投递。
	responseBody(t, client, protocol.Kind_EVENT)
	await(t, server.Done())
	assertNoTopicSubscriptions(t, server, session)
}

func TestTopicServerOwnership(t *testing.T) {
	opened := make(chan *Session, 2)
	cfg := baseConfig()
	cfg.OnOpen = func(_ context.Context, session *Session) error {
		opened <- session
		return nil
	}
	firstServer, firstHTTP := startTestServer(t, cfg)
	firstClient := dial(t, firstHTTP, SubprotocolJSON)
	first := await(t, opened)
	secondServer, secondHTTP := startTestServer(t, cfg)
	secondClient := dial(t, secondHTTP, SubprotocolJSON)
	second := await(t, opened)
	awaitTargets(t, firstServer, 1)
	awaitTargets(t, secondServer, 1)
	if err := firstServer.SubscribeTopics(first, "auction", "sale"); err != nil {
		t.Fatal(err)
	}
	if err := secondServer.SubscribeTopics(second, "auction", "sale"); err != nil {
		t.Fatal(err)
	}
	// 只有本 Server 就绪索引中的原始 Session 可变更订阅，复制 raw 字段不能绕过检查。
	copied := *first
	for _, change := range []func(*Session, string, ...string) error{
		firstServer.SubscribeTopics, firstServer.UnsubscribeTopics, firstServer.ReplaceTopics,
	} {
		for _, invalid := range []*Session{nil, {}} {
			if err := change(invalid, "auction", "sale"); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("nil 或零值 Session 应返回 ErrInvalidMessage: %v", err)
			}
		}
		for _, unavailable := range []*Session{second, &copied} {
			if err := change(unavailable, "auction", "sale"); !errors.Is(err, websocketx.ErrClosed) {
				t.Fatalf("未在本 Server 就绪索引中的 Session 应返回 ErrClosed: %v", err)
			}
		}
	}
	assertTopicPush(t, firstServer, "auction", "sale", 1)
	responseBody(t, firstClient, protocol.Kind_EVENT)
	assertTopicPush(t, secondServer, "auction", "sale", 1)
	responseBody(t, secondClient, protocol.Kind_EVENT)
	if _, err := first.Close(1000, "done"); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Session, string, ...string) error{
		firstServer.SubscribeTopics, firstServer.UnsubscribeTopics, firstServer.ReplaceTopics,
	} {
		if err := change(first, "auction", "sale"); !errors.Is(err, websocketx.ErrClosed) {
			t.Fatalf("已断开连接应返回 ErrClosed: %v", err)
		}
	}
	assertNoTopicSubscriptions(t, firstServer, first)
}
