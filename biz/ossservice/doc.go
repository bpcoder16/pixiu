// Package ossservice 组合 ossx 和 stsx，为指定 OSS 对象签发受限上传凭据。
// 本包只依赖现有客户端，不创建或关闭连接，不读全局配置，不管理上传记录。
// 场景选择、身份检查、文件校验、ObjectKey 生成和 HTTP 响应由应用负责。
// New(ossClient) 仅要求 OSS 客户端；需要签发凭据时通过 WithSTSClient 注入 STS 客户端。
// 未配置 STS 不影响服务构造，调用 CreateUploadCredentials 时返回 ErrSTSNotConfigured。
//
// 示例中的 ossClient、stsClient 为应用已创建的客户端，ctx 为请求 Context；
// 需导入 time 和 github.com/bpcoder16/pixiu/biz/ossservice：
//
//	service, err := ossservice.New(ossClient, ossservice.WithSTSClient(stsClient))
//	if err != nil {
//	    return err
//	}
//	result, err := service.CreateUploadCredentials(ctx, []string{
//	    "test/files/20261027/550e8400e29b41d4a716446655440000.json",
//	}, ossservice.UploadCredentialsOptions{
//	    RoleARN:     "acs:ram::123456789012:role/upload",
//	    SessionName: "file-upload",
//	    Duration:    40 * time.Minute,
//	})
//	if err != nil {
//	    return err
//	}
//	_ = result.Info
//	_ = result.Credentials
//
// 授权固定使用传入 OSS 客户端的 Bucket，仅包含指定 Key 的 oss:PutObject，
// 不授予读取、删除、列举权限；空列表、空 Key 及含 *、? 的 Key 会在签发前被拒绝。
// Policy 不对 Key 做路径清理或 URL 转义，授权范围与调用方提供的对象保持一致。
// STS 仍限制完整 Policy 最多 2048 字节，超限直接失败，不改用通配符或拆分签发。
// 凭据有效期内允许再次写入同名对象；该流程不提供一次性上传或实际文件大小限制。
//
// RoleARN、SessionName、Duration 透传 stsx 的校验和默认值；
// Credentials.Expiration 保留 time.Time，应用按接口约定转换时区及格式。
// 不缓存或重试签发，错误保留 SDK 错误链与 Context 取消、超时语义；
// SDK 错误可能包含敏感原文，勿直接记录错误、临时凭据或完整结果。
// 应用应先完成业务校验，签发成功并保存上传记录后再向客户端返回凭据；
// 保存失败时不返回凭据，已经签发的远端会话不会由本包回滚。
package ossservice
