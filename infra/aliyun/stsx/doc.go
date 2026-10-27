// Package stsx 封装阿里云 STS AssumeRole，与 ossx、esx 平行。
// 导入路径为 github.com/bpcoder16/pixiu/infra/aliyun/stsx。
// 本包只签发临时凭据，不感知 Bucket、上传场景或对象 Key，不缓存或自动续期。
//
// # 配置与调用
//
// Config.Name、Region 必填；AccessKeyID、AccessKeySecret 成对提供，
// 或注入与其互斥的 Credential。动态提供者须并发安全，由调用方持有和关闭。
// New 只构造，不联网验活；Endpoint 留空使用 https://sts.<region>.aliyuncs.com，
// 自定义地址须为不含凭据、查询或非根路径的 HTTP(S) URL。
// RequestTimeout 默认 60 秒，SlowThreshold 默认 500 毫秒；不自动重试或跟随重定向。
//
// AssumeRoleRequest.RoleARN 必填；SessionName 留空默认为 sts-session。
// Duration 必须为 15 分钟至 12 小时的整秒时长，实际允许的上限还受角色配置限制。
// Policy 为可选 JSON，最多 2048 字节；留空继承角色权限，非空时取与角色权限的交集。
// 调用者须有目标角色的 sts:AssumeRole 权限，目标角色的信任策略须允许调用者扮演。
//
// OSS 上传可使用 biz/ossservice，或自行构造只含指定对象 oss:PutObject 的策略调用 AssumeRole；
// 返回的 Credentials 与 ossx.Info 由应用组合成响应。资源 Key 不应包含 RAM 通配符，
// 策略不能限制实际文件大小，业务须在上传确认时核验对象元数据。
// 角色 ARN、会话名和上传策略由业务管理，不放入 OSS 或 STS 的连接配置。
//
// # 错误、日志与生命周期
//
// 错误保留 SDK 错误链和 Context 取消或超时语义；SDK 错误可能包含敏感原文，勿直接记录。
// OptLogRequests(true) 开启结果日志，仅记录 operation、status、request_id、error_code。
// 不记录凭据、Policy 或原始错误；logit.WithStart(ctx) 的下游耗时独立于日志开关。
// 官方 Credential.GetCredential 不接收 ctx，已开始的刷新不能保证立即取消，
// 调用方应为动态凭据提供者配置自己的网络超时。
//
// NewNamed/NewDefault 仅在启动阶段串行初始化，Named/Default 在未初始化或 CloseAll 后 panic。
// 共享实例统一关闭 CloseAll；独立 New 的实例自行 Close，不受 CloseAll 影响。
// Close 幂等且不排空在途请求；先停止并等待使用方，最后关闭日志。
// biz/bootstrap.MustRegisterAliyunSTS(name, isDefault) 声明实例，
// MustBaseInit 从 aliyunstsx.<name>.yaml 加载连接配置，并统一登记 CloseAll。
package stsx
