// Package aliyun 提供阿里云 SDK 适配包内部共用的配置校验、凭据解析和 HTTP 客户端构造。
// 不保存全局账号、客户端或注册表；每次构造的凭据和连接池独立。
// 服务包保留自己的公开 Config、默认值、SDK 适配、请求执行及关闭逻辑。
// 例如服务包的构造函数可依次校验参数、解析凭据并创建连接池：
//
//	if err := aliyun.ValidateConfig(name, region, endpoint, requestTimeout, slowThreshold); err != nil {
//		return err
//	}
//	credential, err := aliyun.ResolveCredential(accessKeyID, accessKeySecret, providedCredential)
//	if err != nil {
//		return err
//	}
//	httpClient, transport, err := aliyun.NewHTTPClient()
//	if err != nil {
//		return err
//	}
//	defer transport.CloseIdleConnections()
//	_ = credential
//	_ = httpClient
//
// 示例变量来自服务配置；实际服务构造成功后持有连接池，由自己的 Close 负责关闭。
// 注入凭据与任意 AK/SK 字段互斥，动态凭据不提前读取，所有构造函数均不联网。
// 错误由服务包补充前缀；服务特有参数和 SDK 错误仍由对应服务处理。
package aliyun
