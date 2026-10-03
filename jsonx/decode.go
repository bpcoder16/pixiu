package jsonx

import (
	"encoding/json"
	"errors"
	"io"
)

// DecodeOne 解码一个 JSON 值到 target，并确认其后只有空白直到 EOF。
// reader 必须非 nil；target 的类型与未知字段处理沿用 encoding/json 的默认规则。
// 读取错误不会因已解析出完整值而被忽略，解析错误与读取错误可通过错误链检查。
// 返回错误时 target 可能已被赋值，不应作为成功结果使用。
// 本函数不关闭 reader，也不设置大小限制或超时；不适用于持续输入的多值 JSON 流。
func DecodeOne(reader io.Reader, target any) (resultErr error) {
	input := &errorReader{Reader: reader}
	defer func() {
		if input.err != nil && !errors.Is(resultErr, input.err) {
			resultErr = errors.Join(resultErr, input.err)
		}
	}()
	decoder := json.NewDecoder(input)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if input.err != nil {
		return input.err
	}
	// 继续使用原解码器确认结束，不能丢弃已缓冲的额外 JSON 内容。
	var extra struct{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return err
		}
		return errors.New("jsonx: unexpected trailing JSON")
	}
	return nil
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
