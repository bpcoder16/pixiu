// Package jsonx 提供独立于传输协议的 JSON 首值解码，保留解码期间观察到的底层读取错误。
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
// 首值解析成功后不检查尾部或额外确认 EOF，接受后续存在其他 JSON 或非法内容。
// 解码器可能预读后续字节；连续解码多个值时应持有同一个 json.Decoder。
package jsonx
