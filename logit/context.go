package logit

import (
	"context"
	"fmt"
	"maps"
	"sync"
)

// ctxKey 是本包专用的 context key 类型,根除裸 string key 的冲突风险。
type ctxKey int

const (
	ctxKeyFields ctxKey = iota
	ctxKeyMeta
)

// ctxField 是存进 context 的字段:Field 本体 + 可见性掩码。
// vis 决定该字段在哪些级别的日志行输出(AddDebugField 等设置),默认全级别可见。
type ctxField struct {
	field Field
	vis   Level
}

// fieldStore 按添加顺序存储字段,支持按 key 覆盖/查找/删除。
// 挂在 context 上的是 *fieldStore,对其的修改对共享同一 store 的所有 ctx 立即可见。
type fieldStore struct {
	mu    sync.RWMutex
	order []string // 按首次添加顺序排列的 key
	idx   map[string]ctxField
}

func newFieldStore() *fieldStore {
	return &fieldStore{idx: make(map[string]ctxField)}
}

func (s *fieldStore) add(f Field, vis Level) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.idx[f.Key]; !ok {
		s.order = append(s.order, f.Key)
	}
	s.idx[f.Key] = ctxField{field: f, vis: vis}
}

func (s *fieldStore) get(key string) (ctxField, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, ok := s.idx[key]
	return f, ok
}

func (s *fieldStore) del(keys ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		if _, ok := s.idx[key]; !ok {
			continue
		}
		delete(s.idx, key)
		for i, k := range s.order {
			if k == key {
				s.order = append(s.order[:i], s.order[i+1:]...)
				break
			}
		}
	}
}

func (s *fieldStore) clone() *fieldStore {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copied := newFieldStore()
	copied.order = append(copied.order, s.order...)
	maps.Copy(copied.idx, s.idx)
	return copied
}

// snapshot 按添加顺序复制字段,供可能回调到 context 写 API 的冷路径使用。
// 回调发生在锁外,避免 CopyAllFields(ctx, ctx) 或 RangeFields 内修改时自锁。
func (s *fieldStore) snapshot() []ctxField {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fields := make([]ctxField, 0, len(s.order))
	for _, key := range s.order {
		fields = append(fields, s.idx[key])
	}
	return fields
}

// rangeFields 按添加顺序遍历,fn 返回非 nil 时停止。
func (s *fieldStore) rangeFields(fn func(f ctxField) error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, key := range s.order {
		if err := fn(s.idx[key]); err != nil {
			return
		}
	}
}

func findStore(ctx context.Context, key ctxKey) *fieldStore {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(key).(*fieldStore); ok {
		return v
	}
	return nil
}

// WithContext 初始化 ctx 的字段存储(普通 + meta 双作用域),已初始化则原样返回。
// 之后才可调用 AddField/AddMeta 等可变 API;服务入口处调用一次即可。
func WithContext(ctx context.Context) context.Context {
	if findStore(ctx, ctxKeyMeta) == nil {
		ctx = context.WithValue(ctx, ctxKeyMeta, newFieldStore())
	}
	if findStore(ctx, ctxKeyFields) == nil {
		ctx = context.WithValue(ctx, ctxKeyFields, newFieldStore())
	}
	return ctx
}

// ForkContext 基于当前 ctx 分支:继承普通字段副本(分支上的修改不影响父 ctx),
// meta 字段继续共享(全链路串联)。
func ForkContext(ctx context.Context) context.Context {
	ctx = WithContext(ctx)
	if s := findStore(ctx, ctxKeyFields); s != nil {
		ctx = context.WithValue(ctx, ctxKeyFields, s.clone())
	}
	return ctx
}

// CopyAllFields 把 src 的普通字段与 meta 字段复制到 dest(如后台任务用
// context.Background() 起协程,但希望继承日志上下文)。
func CopyAllFields(dest, src context.Context) context.Context {
	dest = WithContext(dest)
	if s := findStore(src, ctxKeyFields); s != nil {
		for _, f := range s.snapshot() {
			findStore(dest, ctxKeyFields).add(f.field, f.vis)
		}
	}
	if s := findStore(src, ctxKeyMeta); s != nil {
		for _, f := range s.snapshot() {
			findStore(dest, ctxKeyMeta).add(f.field, f.vis)
		}
	}
	return dest
}

// AddField 向普通作用域添加字段(所有级别可见)。ctx 必须已经 WithContext,
// 否则 panic——这是编程错误,应在首次测试时暴露。保留字段名也会 panic。
func AddField(ctx context.Context, fields ...Field) {
	mustStore(ctx, ctxKeyFields).addFields(AllLevels, fields)
}

// AddDebugField 添加仅 Debug 级别日志行可见的字段(敏感调试信息不漏进 Info 日志)。
// 保留字段名会 panic。
func AddDebugField(ctx context.Context, fields ...Field) {
	mustStore(ctx, ctxKeyFields).addFields(DebugLevel, fields)
}

// AddMeta 向 meta 作用域添加字段(全级别可见、不受 ForkContext 影响)。
// 保留字段名会 panic；logId 须通过 SetLogID 设置。
func AddMeta(ctx context.Context, fields ...Field) {
	mustStore(ctx, ctxKeyMeta).addFields(AllLevels, fields)
}

func (s *fieldStore) addFields(vis Level, fields []Field) {
	// 先验证整批字段，避免遇到无效键时已写入前面的字段。
	for _, f := range fields {
		rejectReservedField(f.Key)
	}
	for _, f := range fields {
		s.add(f, vis)
	}
}

func mustStore(ctx context.Context, key ctxKey) *fieldStore {
	if s := findStore(ctx, key); s != nil {
		return s
	}
	panic(fmt.Sprintf("logit: context not initialized, call logit.WithContext first (missing %v store)", key))
}

// FindField 查找普通作用域字段,不存在返回零值与 false。
func FindField(ctx context.Context, key string) (Field, bool) {
	if s := findStore(ctx, ctxKeyFields); s != nil {
		if cf, ok := s.get(key); ok {
			return cf.field, true
		}
	}
	return Field{}, false
}

// FindMeta 查找 meta 作用域字段。
func FindMeta(ctx context.Context, key string) (Field, bool) {
	if s := findStore(ctx, ctxKeyMeta); s != nil {
		if cf, ok := s.get(key); ok {
			return cf.field, true
		}
	}
	return Field{}, false
}

// DeleteField 删除普通作用域字段,不存在则忽略。
func DeleteField(ctx context.Context, keys ...string) {
	if s := findStore(ctx, ctxKeyFields); s != nil {
		s.del(keys...)
	}
}

// DeleteMeta 删除 meta 作用域字段。
func DeleteMeta(ctx context.Context, keys ...string) {
	if s := findStore(ctx, ctxKeyMeta); s != nil {
		s.del(keys...)
	}
}

// RangeFields 按添加顺序遍历普通作用域字段,fn 返回非 nil 时停止。
func RangeFields(ctx context.Context, fn func(f Field) error) {
	if s := findStore(ctx, ctxKeyFields); s != nil {
		for _, cf := range s.snapshot() {
			if err := fn(cf.field); err != nil {
				return
			}
		}
	}
}

// RangeMeta 按添加顺序遍历 meta 作用域字段。
func RangeMeta(ctx context.Context, fn func(f Field) error) {
	if s := findStore(ctx, ctxKeyMeta); s != nil {
		for _, cf := range s.snapshot() {
			if err := fn(cf.field); err != nil {
				return
			}
		}
	}
}

// eachVisible 是编码热路径:按"普通作用域在前、meta 作用域在后"的顺序,
// 把在 lineLevel 级别可见的字段依次交给 fn,fn 返回非 nil 时停止该作用域遍历。
func eachVisible(ctx context.Context, lineLevel Level, fn func(f Field) error) {
	walk := func(s *fieldStore) {
		s.rangeFields(func(cf ctxField) error {
			if cf.vis.Is(lineLevel) {
				return fn(cf.field)
			}
			return nil
		})
	}
	if s := findStore(ctx, ctxKeyFields); s != nil {
		walk(s)
	}
	if s := findStore(ctx, ctxKeyMeta); s != nil {
		walk(s)
	}
}
