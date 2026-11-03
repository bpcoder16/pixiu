// Package protocol 保存 realtime.v1 的公共 protobuf 信封，不包含业务消息类型。
// JSON 使用同语义的外层字段及内嵌 ProtoJSON；不能直接 protojson.Marshal 本信封代替 JSON 编码。
//
// 客户端构造 PB 请求示例，需导入 protocol、proto 和 wrapperspb：
//
//	body, err := proto.Marshal(wrapperspb.String("hello"))
//	if err != nil {
//		return err
//	}
//	wire, err := proto.Marshal(&protocol.Envelope{
//		Kind:      protocol.Kind_REQUEST,
//		RequestId: "r1",
//		Scene:     "home",
//		Action:    "hello",
//		Body:      &protocol.Envelope_Data{Data: body},
//	})
//	if err != nil {
//		return err
//	}
//	// 协商 pixiu.realtime.v1.protobuf 后将 wire 作为一条 Binary 消息发送。
//
// 请求通过 scene/action 路由；下行不设置这两个字段，响应、错误响应和事件均必填 Type。
// Type 是业务约定的消息类型字符串，客户端据此选择处理器和正文解码类型。
// 响应回显 RequestId，事件不携带；data 即使为空 bytes，也需保留 oneof 存在性。
// 生成入口为 go generate ./biz/realtime，工具版本见该包的 generate.sh。
package protocol
