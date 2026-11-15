package realtime

import (
	"strings"

	"github.com/bpcoder16/pixiu/infra/websocketx"
)

// topicNamespace 与 topicSubscription 只在有订阅者时存在，均由 pushIndex.mu 保护。
type topicNamespace struct {
	name   string
	topics map[string]*topicSubscription
}

type topicSubscription struct {
	namespace *topicNamespace
	name      string
	sessions  sessionSet
}

type topicUpdate uint8

const (
	topicSubscribe topicUpdate = iota
	topicUnsubscribe
	topicReplace
)

// SubscribeTopics 原子增加指定连接在 namespace 下的订阅，重复项幂等。
// namespace/topic 均为 1–128 字节，只允许 ASCII 字母、数字、点、下划线和短横线。
// 业务先校验权限及业务状态；框架不注册客户端订阅路由，不解释 topic 的业务含义。
// nil、零值 Session 返回 ErrInvalidMessage；外部连接或复制品不在本 Server 就绪索引中，返回 websocketx.ErrClosed。
// 仅接纳初始化成功且仍在线的连接；OnOpen 内、Stop 或断线后调用返回 websocketx.ErrClosed。
// 客户端消息 Handler 在连接初始化成功后执行，可直接订阅；断线自动释放所有订阅。
func (s *Server) SubscribeTopics(session *Session, namespace string, topics ...string) error {
	return s.updateTopics(session, namespace, topics, topicSubscribe)
}

// UnsubscribeTopics 原子移除指定连接在 namespace 下的订阅，不存在的项忽略。
// Session 校验与 SubscribeTopics 相同。
// 退订不撤回已取得目标快照或已入队的推送，客户端应忽略已不关注的数据。
func (s *Server) UnsubscribeTopics(session *Session, namespace string, topics ...string) error {
	return s.updateTopics(session, namespace, topics, topicUnsubscribe)
}

// ReplaceTopics 原子替换指定连接在 namespace 下的全部订阅，空列表清空该 namespace。
// Session 校验与 SubscribeTopics 相同。
// 其他 namespace 保持不变；非法参数或超限均保留原订阅，不进行部分变更。
func (s *Server) ReplaceTopics(session *Session, namespace string, topics ...string) error {
	return s.updateTopics(session, namespace, topics, topicReplace)
}

func (s *Server) updateTopics(session *Session, namespace string, topics []string, mode topicUpdate) error {
	if session == nil || session.raw == nil || !validID(namespace) {
		return ErrInvalidMessage
	}
	requested := make(map[string]struct{})
	for _, topic := range topics {
		if !validID(topic) {
			return ErrInvalidMessage
		}
		requested[topic] = struct{}{}
		if mode != topicUnsubscribe && len(requested) > s.config.MaxTopicsPerSession {
			return ErrTopicLimitExceeded
		}
	}
	// 和 Stop 共用接纳锁，封门后不再变更订阅；断线清理由内部索引直接完成。
	s.pushMu.Lock()
	defer s.pushMu.Unlock()
	if !s.Ready() {
		return websocketx.ErrClosed
	}
	index := &s.targets
	index.mu.Lock()
	defer index.mu.Unlock()
	// 与 OnDetach 共用索引锁，防止并发清理后重新挂入已断开的连接。
	select {
	case <-session.raw.Detached():
		return websocketx.ErrClosed
	default:
	}
	// 以本 Server 的就绪索引统一校验原始 Session；和登记、断线清理共用索引锁，避免重新挂入已断开的连接。
	if _, ready := index.all[session]; !ready {
		return websocketx.ErrClosed
	}
	// 按最终集合预检配额，替换时不把新旧订阅重复计数；检查通过前不改索引。
	count := len(session.topics)
	for topic := range session.topics {
		if topic.namespace.name != namespace {
			continue
		}
		_, requestedTopic := requested[topic.name]
		if mode == topicReplace || (mode == topicSubscribe && requestedTopic) {
			count--
		}
	}
	if mode != topicUnsubscribe && len(requested) > s.config.MaxTopicsPerSession-count {
		return ErrTopicLimitExceeded
	}
	for topic := range session.topics {
		if topic.namespace.name != namespace {
			continue
		}
		_, requestedTopic := requested[topic.name]
		if (mode == topicUnsubscribe && requestedTopic) || (mode == topicReplace && !requestedTopic) {
			index.unsubscribe(session, topic)
		}
	}
	if mode != topicUnsubscribe {
		for topic := range requested {
			index.subscribe(session, namespace, topic)
		}
	}
	return nil
}

// ClearTopic 删除本 Server 上 namespace/topic 的全部订阅，返回移除的连接数。
// 不存在时返回零；允许停机期间清理，不阻止后续重新订阅，不跨节点广播。
func (s *Server) ClearTopic(namespace, topic string) (int, error) {
	if !validID(namespace) || !validID(topic) {
		return 0, ErrInvalidMessage
	}
	index := &s.targets
	index.mu.Lock()
	defer index.mu.Unlock()
	group := index.namespaces[namespace]
	if group == nil {
		return 0, nil
	}
	subscription := group.topics[topic]
	if subscription == nil {
		return 0, nil
	}
	count := len(subscription.sessions)
	for session := range subscription.sessions {
		index.unsubscribe(session, subscription)
	}
	return count, nil
}

// 以下方法由调用方持有 pushIndex.mu，双向引用在同一临界区内一起变更。
func (i *pushIndex) subscribe(s *Session, namespace, name string) {
	group := i.namespaces[namespace]
	if group == nil {
		if i.namespaces == nil {
			i.namespaces = make(map[string]*topicNamespace)
		}
		group = &topicNamespace{
			name:   strings.Clone(namespace),
			topics: make(map[string]*topicSubscription),
		}
		i.namespaces[group.name] = group
	}
	topic := group.topics[name]
	if topic == nil {
		// 每个活跃 topic 只持有一份名称；Clone 避免保留调用方的大字符串底层存储。
		topic = &topicSubscription{
			namespace: group,
			name:      strings.Clone(name),
			sessions:  make(sessionSet),
		}
		group.topics[topic.name] = topic
	}
	if s.topics == nil {
		s.topics = make(map[*topicSubscription]struct{})
	}
	s.topics[topic] = struct{}{}
	topic.sessions[s] = struct{}{}
}

func (i *pushIndex) unsubscribe(s *Session, topic *topicSubscription) {
	delete(s.topics, topic)
	if len(s.topics) == 0 {
		s.topics = nil
	}
	delete(topic.sessions, s)
	if len(topic.sessions) == 0 {
		group := topic.namespace
		delete(group.topics, topic.name)
		if len(group.topics) == 0 {
			delete(i.namespaces, group.name)
			if len(i.namespaces) == 0 {
				i.namespaces = nil
			}
		}
	}
}
