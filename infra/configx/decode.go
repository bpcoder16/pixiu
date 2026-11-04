package configx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

func jsonNumberHook(next mapstructure.DecodeHookFunc) mapstructure.DecodeHookFunc {
	return func(from, to reflect.Value) (any, error) {
		if number, ok := from.Interface().(json.Number); ok {
			// json.Number 底层是 string，但不能隐式映射为字符串字段。
			if to.Kind() == reflect.String && to.Type() != from.Type() {
				return nil, fmt.Errorf("cannot decode JSON number as %v", to.Type())
			}
			// 默认字符串 hook 会断言 string；数字绕过它们，由 mapstructure 解析。
			return number, nil
		}
		return mapstructure.DecodeHookExec(next, from, to)
	}
}

// fileDecoders 保存完整源数据用于严格映射，JSON 额外保留数字精度。
type fileDecoders struct {
	values map[string]any
}

func (d *fileDecoders) Decoder(format string) (viper.Decoder, error) {
	var decoder viper.Decoder
	if format == "json" {
		decoder = jsonDecoder{}
	} else {
		var err error
		decoder, err = viper.NewCodecRegistry().Decoder(format)
		if err != nil {
			return nil, err
		}
	}
	return capturedDecoder{decoder: decoder, values: &d.values}, nil
}

type capturedDecoder struct {
	decoder viper.Decoder
	values  *map[string]any
}

func (d capturedDecoder) Decode(data []byte, dst map[string]any) error {
	if err := d.decoder.Decode(data, dst); err != nil {
		return err
	}
	// Viper 会原地把键转成小写，此处仍能区分冲突的原始拼写。
	if err := checkMapKeys(dst); err != nil {
		return fmt.Errorf("config root: %w", err)
	}
	*d.values = dst
	return nil
}

func checkMapKeys[K comparable](object map[K]any) error {
	seen := make(map[string]string, len(object))
	for key, value := range object {
		if name, ok := any(key).(string); ok {
			lower := strings.ToLower(name)
			if previous, exists := seen[lower]; exists {
				return fmt.Errorf("case-insensitive key conflict between %q and %q", min(previous, name), max(previous, name))
			}
			seen[lower] = name
		}
		if err := checkNestedKeys(value); err != nil {
			return fmt.Errorf("key %q: %w", fmt.Sprint(key), err)
		}
	}
	return nil
}

func checkNestedKeys(value any) error {
	switch v := value.(type) {
	case map[string]any:
		return checkMapKeys(v)
	case map[any]any:
		// 含非字符串键的 YAML 对象也可能包含需要检查的字符串键。
		return checkMapKeys(v)
	case []any:
		for i, item := range v {
			if err := checkNestedKeys(item); err != nil {
				return fmt.Errorf("index %d: %w", i, err)
			}
		}
	}
	return nil
}

type jsonDecoder struct{}

func (jsonDecoder) Decode(data []byte, dst map[string]any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	// Viper 默认先转 float64，会在映射到 int64/uint64 前丢失大整数精度。
	decoder.UseNumber()
	if err := decoder.Decode(&dst); err != nil {
		return err
	}
	// Decoder 只读取首值；配置文件必须完整，拒绝第二个值及非法尾部。
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("multiple JSON values in config file")
	}
	return nil
}
