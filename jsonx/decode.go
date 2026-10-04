package jsonx

import (
	"encoding/json"
	"errors"
	"io"
)

// DecodeOne 解码首个 JSON 值到 target，不检查尾部或额外确认 EOF。
// reader 必须非 nil；target 的类型与未知字段处理沿用 encoding/json 的默认规则。
// 解码期间观察到的读取错误不会因已解析出完整值而被忽略，可通过错误链检查。
// 返回错误时 target 可能已被赋值，不应作为成功结果使用。
// 本函数不关闭 reader，也不设置大小限制或超时；解码器可能预读后续内容，
// 需要连续解码多个值时应由调用方持有同一个 json.Decoder。
func DecodeOne(reader io.Reader, target any) error {
	input := &errorReader{Reader: reader}
	decodeErr := json.NewDecoder(input).Decode(target)

	// 没有读取错误时，直接返回解码结果，成功即为 nil。
	if input.err == nil {
		return decodeErr
	}
	if decodeErr == nil {
		return input.err
	}

	// 解码错误可能已经包含读取错误，避免在错误链中重复加入。
	if errors.Is(decodeErr, input.err) {
		return decodeErr
	}
	return errors.Join(decodeErr, input.err)
}

// errorReader 保留首次非 EOF 读取错误，防止解码器先解析完整值而忽略同次读取的错误。
// 每次 DecodeOne 独占一个包装，只用于同步读取，不接管输入的生命周期。
type errorReader struct {
	io.Reader
	err error
}

func (r *errorReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err != nil && err != io.EOF && r.err == nil {
		r.err = err
	}
	return n, err
}
