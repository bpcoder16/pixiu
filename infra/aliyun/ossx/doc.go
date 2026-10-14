// Package ossx 封装官方 OSS SDK V2 的普通对象上传与本地文件上传。
// 导入路径为 github.com/bpcoder16/pixiu/infra/aliyun/ossx。
// 配置、凭据、连接池和命名注册表独立于 esx，可分别使用不同账号。
// Name、Region、Bucket 必填；每个实例固定一个 Bucket，多 Bucket 创建多个实例。
// 固定 AK/SK 通过 AccessKeyID、AccessKeySecret 提供，New 内部构造凭据；
// 也可手动注入 Credential，但与 AK/SK 字段互斥。RequestTimeout 默认 60 秒，SlowThreshold 默认 500 毫秒。
// New 只构造，不联网验活；Endpoint 可选，须为不含凭据、查询或非根路径的 HTTP(S) URL。
// 不自动重试或跟随重定向，保留 SDK 的 CRC64 校验；PutObject 不关闭调用方的 Reader。
// 普通 PUT 遵循 OSS 单对象上传限制，不提供分片上传；Key 原样传递，不改变对象 ACL。
// SDK 错误可用 errors.As 识别，超时不代表远端没有写入。
//
// OptLogRequests(true) 开启脱敏结果日志，默认关闭；不提供正文日志。
// logit.WithStart(ctx) 启用请求级耗时，登记与结果日志开关无关。
// 官方 Credential.GetCredential 不接收 ctx，已开始的刷新不能保证立即取消；
// 自定义 Reader 也须及时响应读取，调用方不能依赖超时强制中断任意阻塞的 Read。
// 本包禁止 OSS SDK 调试输出；凭据库的进程级 DEBUG 开关须由部署方关闭。
//
// 以下示例中的 accessKeyID、accessKeySecret 来自部署配置，ctx 为请求 context：
//
//	client, err := ossx.New(ossx.Config{
//		Name:            "synonyms-files",
//		Region:          "cn-hangzhou",
//		Bucket:          "synonyms-bucket",
//		AccessKeyID:     accessKeyID,
//		AccessKeySecret: accessKeySecret,
//	}, ossx.OptLogRequests(true))
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//	result, err := client.PutFile(ctx, ossx.PutFileRequest{
//		Key:         "releases/v1/synonyms.txt",
//		FilePath:    "/data/synonyms.txt",
//		ContentType: "text/plain; charset=utf-8",
//	})
//	if err != nil {
//		return err
//	}
//	_ = result.RequestID
//
// Bucket 在构造时校验，上传请求仅指定 Key 和数据；旧请求中的 Bucket 须移到 Config。
// biz/bootstrap 可通过 MustRegisterAliyunOSS(name, isDefault) 声明实例，
// MustBaseInit 读取 aliyunossx.<name>.yaml 并创建固定 AK/SK 客户端。
//
// NewNamed/NewDefault 仅在启动阶段串行初始化，Named/Default 在未初始化或
// CloseAll 开始后 panic。共享实例统一登记 CloseAll 至 lifecycle.Stack；
// 独立 New 登记自己的 Close。先停止并等待使用方，再关闭连接，日志最后关闭。
// Close 不排空在途请求，不关闭借用凭据；CloseAll 不影响独立 New 和其他模块。
package ossx
