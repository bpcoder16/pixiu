package logit

import (
	"context"
	"fmt"
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

// fieldStore 按添加顺序存储字段,同名 key 覆盖旧值。
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

// rangeFields 按添加顺序遍历字段。
func (s *fieldStore) rangeFields(fn func(f ctxField)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, key := range s.order {
		fn(s.idx[key])
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

// AddMeta 向 meta 作用域添加字段(全级别可见,派生 context 共享)。
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

// eachVisible 是编码热路径:按"meta 作用域在前、普通作用域在后"的顺序,
// 把在 lineLevel 级别可见的字段依次交给 fn。
func eachVisible(ctx context.Context, lineLevel Level, fn func(f Field)) {
	walk := func(s *fieldStore) {
		s.rangeFields(func(cf ctxField) {
			if cf.vis.Is(lineLevel) {
				fn(cf.field)
			}
		})
	}
	if s := findStore(ctx, ctxKeyMeta); s != nil {
		walk(s)
	}
	if s := findStore(ctx, ctxKeyFields); s != nil {
		walk(s)
	}
}
