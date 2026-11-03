package realtime

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/bpcoder16/pixiu/biz/realtime/protocol"
	"github.com/bpcoder16/pixiu/infra/websocketx"
	"google.golang.org/protobuf/proto"
)

// PushMessage 描述主动事件。Type 必填，Data 与 Error 二选一；ExpiresAt 为零表示不过期。
type PushMessage struct {
	Type      string
	Data      proto.Message
	Error     *Error
	ExpiresAt time.Time
}

func (msg PushMessage) outgoing() Outgoing {
	return Outgoing{
		Kind:      protocol.Kind_EVENT,
		Type:      msg.Type,
		Data:      msg.Data,
		Error:     msg.Error,
		ExpiresAt: msg.ExpiresAt,
	}
}

// PushResult 统计本节点匹配、已入队和失败的连接数。成功入队不等于终端已收到。
type PushResult struct {
	Matched  int
	Enqueued int
	Failed   int
}

type pushTarget struct {
	kind uint8
	id   string
}

const (
	allTargets uint8 = iota
	userTarget
	deviceTarget
)

type sessionSet map[*Session]struct{}

type pushIndex struct {
	mu             sync.RWMutex
	all            sessionSet
	users, devices map[string]sessionSet
}

func addMember(index map[string]sessionSet, id string, s *Session) {
	if id == "" {
		return
	}
	if index[id] == nil {
		index[id] = make(sessionSet)
	}
	index[id][s] = struct{}{}
}

func removeMember(index map[string]sessionSet, id string, s *Session) {
	set := index[id]
	delete(set, s)
	if len(set) == 0 {
		delete(index, id)
	}
}

func (i *pushIndex) add(ctx context.Context, s *Session) {
	i.mu.Lock()
	defer i.mu.Unlock()
	deadline, bounded := ctx.Deadline()
	if ctx.Err() != nil || (bounded && !time.Now().Before(deadline)) {
		return
	}
	// 和 detach 共用一把索引锁，阻止慢 OnOpen 返回后重新挂上已断开的连接。
	select {
	case <-s.raw.Detached():
		return
	default:
	}
	if i.all == nil {
		i.all = make(sessionSet)
		i.users = make(map[string]sessionSet)
		i.devices = make(map[string]sessionSet)
	}
	i.all[s] = struct{}{}
	addMember(i.users, s.info.UserID, s)
	addMember(i.devices, s.info.DeviceID, s)
}

func (i *pushIndex) remove(s *Session) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.all, s)
	removeMember(i.users, s.info.UserID, s)
	removeMember(i.devices, s.info.DeviceID, s)
}

func (i *pushIndex) snapshot(target pushTarget) []*Session {
	i.mu.RLock()
	defer i.mu.RUnlock()
	var set sessionSet
	switch target.kind {
	case allTargets:
		set = i.all
	case userTarget:
		set = i.users[target.id]
	case deviceTarget:
		set = i.devices[target.id]
	}
	matches := make([]*Session, 0, len(set))
	for s := range set {
		matches = append(matches, s)
	}
	return matches
}

// Broadcast 向当前 Server 上所有已就绪的连接投递事件。零命中是正常结果。
func (s *Server) Broadcast(ctx context.Context, msg PushMessage) (PushResult, error) {
	return s.deliver(ctx, pushTarget{kind: allTargets}, msg)
}

// PushToUser 向当前 Server 上该用户的全部已就绪连接投递。UserID 使用握手认证值。
func (s *Server) PushToUser(ctx context.Context, userID string, msg PushMessage) (PushResult, error) {
	return s.deliver(ctx, pushTarget{kind: userTarget, id: userID}, msg)
}

// PushToDevice 向当前 Server 上该设备的全部已就绪连接投递。DeviceID 使用握手认证值。
func (s *Server) PushToDevice(ctx context.Context, deviceID string, msg PushMessage) (PushResult, error) {
	return s.deliver(ctx, pushTarget{kind: deviceTarget, id: deviceID}, msg)
}

func (s *Server) deliver(ctx context.Context, target pushTarget, msg PushMessage) (result PushResult, err error) {
	if ctx == nil {
		return result, websocketx.ErrNilContext
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return result, ctxErr
	}
	if target.kind != allTargets && strings.TrimSpace(target.id) == "" {
		return result, ErrInvalidMessage
	}
	s.pushMu.Lock()
	if !s.Ready() {
		s.pushMu.Unlock()
		return result, websocketx.ErrClosed
	}
	s.pushes.Add(1)
	s.pushMu.Unlock()
	defer s.pushes.Done()
	out := msg.outgoing()
	if validationErr := validateOutgoing(out); validationErr != nil {
		return result, validationErr
	}
	if !msg.ExpiresAt.IsZero() && !time.Now().Before(msg.ExpiresAt) {
		return result, websocketx.ErrExpired
	}
	matches := s.targets.snapshot(target)
	result.Matched = len(matches)
	// 仅为实际命中的子协议各编码、准备一次，慢连接或单种编码失败时继续投递。
	type encoded struct {
		frame *websocketx.PreparedMessage
		err   error
	}
	frames := make(map[string]encoded, 2)
	for i, session := range matches {
		halted := ctx.Err()
		if halted == nil && s.canceled.Load() {
			halted = websocketx.ErrClosed
		}
		if halted != nil {
			result.Failed += len(matches) - i
			if err == nil {
				err = halted
			}
			break
		}
		key := session.Subprotocol()
		entry, ok := frames[key]
		if !ok {
			wire, encodeErr := encodeEnvelope(key, out, s.config.Transport.MaxSendBytes)
			if encodeErr == nil {
				entry.frame, encodeErr = s.endpoint.Prepare(wire)
			}
			entry.err = encodeErr
			frames[key] = entry
		}
		sendErr := entry.err
		if sendErr == nil {
			sendErr = session.raw.SendPrepared(ctx, entry.frame)
		}
		if sendErr != nil {
			result.Failed++
			if err == nil {
				err = sendErr
			}
		} else {
			result.Enqueued++
		}
	}
	return result, err
}
