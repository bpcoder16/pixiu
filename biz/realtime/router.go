package realtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var errRegistrationClosed = errors.New("realtime: handler registration is closed")

// Router 由 NewServer 提供给 Config.RegisterHandlers，按 scene 和 action 登记业务处理器。
// 每个服务拥有自己的路由表，构造成功后只读。首次使用后不可复制。
type Router struct {
	mu     sync.Mutex
	routes map[[2]string]Handler
	sealed bool
}

// Handle 注册一对区分大小写的协议标识，重复注册返回错误并保留原处理器。
func (r *Router) Handle(scene, action string, handler Handler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return errRegistrationClosed
	}
	if !validID(scene) || !validID(action) {
		return fmt.Errorf("realtime: invalid route scene %q or action %q", scene, action)
	}
	if handler == nil {
		return fmt.Errorf("realtime: nil handler for route %q/%q", scene, action)
	}
	key := [2]string{scene, action}
	if r.routes[key] != nil {
		return fmt.Errorf("realtime: duplicate route %q/%q", scene, action)
	}
	if r.routes == nil {
		r.routes = make(map[[2]string]Handler)
	}
	r.routes[key] = handler
	return nil
}

// 注册完成后，各连接在原消息回调中同步分发，共享只读表。
func (r *Router) seal(onNotFound Handler) Handler {
	r.mu.Lock()
	defer r.mu.Unlock()
	if onNotFound == nil {
		onNotFound = func(ctx context.Context, s *Session, m *Message) error {
			return s.ReplyError(ctx, m, "request.error", "NOT_FOUND", "不支持的消息")
		}
	}
	r.sealed = true
	routes := r.routes
	return func(ctx context.Context, session *Session, message *Message) error {
		if handler := routes[[2]string{message.Scene(), message.Action()}]; handler != nil {
			return handler(ctx, session, message)
		}
		return onNotFound(ctx, session, message)
	}
}
