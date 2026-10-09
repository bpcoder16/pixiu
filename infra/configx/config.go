package configx

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

var configs sync.Map // name → *T，解析完成后才发布。

// Load 读取 YAML、TOML 或 JSON 文件，按 mapstructure 标签解析为 T 并注册到 name。
// T 必须是结构体；重复名称返回错误，读取或解析失败不占用名称。
// 相对路径基于工作目录，后缀忽略大小写。未知字段通过 UnmarshalExact 报错，禁用弱类型转换。
func Load[T any](name, filePath string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("configx: empty config name")
	}
	if typ := reflect.TypeFor[T](); typ.Kind() != reflect.Struct {
		return fmt.Errorf("configx: config %q requires a struct type, got %v", name, typ)
	}
	if _, exists := configs.Load(name); exists {
		return fmt.Errorf("configx: config %q is already loaded", name)
	}
	if filePath == "" {
		return fmt.Errorf("configx: config %q has an empty file path", name)
	}
	format := strings.ToLower(strings.TrimPrefix(filepath.Ext(filePath), "."))
	switch format {
	case "yaml", "yml", "toml", "json":
	default:
		return fmt.Errorf("configx: config %q file %q has unsupported format %q", name, filePath, format)
	}

	decoders := &fileDecoders{}
	v := viper.NewWithOptions(viper.WithDecoderRegistry(decoders))
	v.SetConfigFile(filePath)
	v.SetConfigType(format)
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("configx: read config %q from %q: %w", name, filePath, err)
	}
	cfg := new(T)
	if err := v.UnmarshalExact(cfg, func(dc *mapstructure.DecoderConfig) {
		dc.WeaklyTypedInput = false
		// Viper 路径展开会丢弃 null 和空对象；根映射使用完整源数据，
		// 让 UnmarshalExact 也能检查这些未知字段，后续保留默认解码 hook。
		root := true
		dc.DecodeHook = mapstructure.ComposeDecodeHookFunc(
			func(_, _ reflect.Type, data any) (any, error) {
				if root {
					root = false
					return decoders.values, nil
				}
				return data, nil
			},
			dc.DecodeHook,
		)
	}); err != nil {
		return fmt.Errorf("configx: decode config %q from %q: %w", name, filePath, err)
	}

	// 预检不能防止并发重名；仅将完整对象原子发布，失败者不得覆盖已有值。
	if _, exists := configs.LoadOrStore(name, cfg); exists {
		return fmt.Errorf("configx: config %q is already loaded", name)
	}
	return nil
}

// Get 返回已注册且类型匹配的共享结构体指针，不重新读文件或拷贝内容。
// 名称不存在或 T 与加载时不同则返回 nil 和错误；共享对象的并发读写由调用方协调。
func Get[T any](name string) (*T, error) {
	value, exists := configs.Load(name)
	if !exists {
		return nil, fmt.Errorf("configx: config %q is not loaded", name)
	}
	cfg, ok := value.(*T)
	if !ok {
		return nil, fmt.Errorf("configx: config %q has type %T, requested *%v", name, value, reflect.TypeFor[T]())
	}
	return cfg, nil
}

// GetOrZero 返回共享配置，获取失败时返回新分配的 T 零值指针。
// 兜底对象不注册、不共享，也不填充默认值或执行业务校验。
func GetOrZero[T any](name string) *T {
	cfg, err := Get[T](name)
	if err != nil {
		return new(T)
	}
	return cfg
}
