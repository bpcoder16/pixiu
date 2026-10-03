// Package jsonx 提供独立于传输协议的 JSON 单值解码，拒绝尾部额外内容并保留底层读取错误。
//
// 以下示例需导入 fmt、strings 和 github.com/bpcoder16/pixiu/jsonx：
//
//	var value struct {
//		Count int `json:"count"`
//	}
//	reader := strings.NewReader(`{"count":42}`)
//	if err := jsonx.DecodeOne(reader, &value); err != nil {
//		return err
//	}
//	fmt.Println(value.Count)
//
// DecodeOne 不关闭输入，不设置大小限制或超时；文件、HTTP 响应体等资源由调用方管理。
// 确认 EOF 可能继续读取或等待，不适用于持续连接中的多个 JSON 消息。
// 使用 io.LimitReader 时，达到限制产生的 EOF 不代表原始输入已结束。
package jsonx
