// Package esx 封装阿里云 Elasticsearch 管理 SDK 的词典清单、词典更新和实例查询。
// 导入路径为 github.com/bpcoder16/pixiu/infra/aliyun/esx。
// SDK 主版本与 Elasticsearch 服务端 7/8/9 无对应关系，不需要按服务端版本创建适配包。
// 本包独立配置账号，与 ossx 不共享 Config、凭据实例、连接池或注册表。
// Name、Region、InstanceID 必填；每个客户端固定一个目标实例，多实例分别创建。
// InstanceID 不得为空或含首尾空白；固定 AK/SK 通过 AccessKeyID、AccessKeySecret 成对提供，New 内部构造凭据。
// 也可手动注入 Credential，但与任意 AK/SK 字段互斥；New 只构造，不联网验活。
// RequestTimeout 默认 60 秒，SlowThreshold 默认 200 毫秒；更短的 ctx 期限优先。
// Endpoint 可选，须为不含凭据、查询或非根路径的 HTTP(S) URL。
// 禁用自动重试和重定向；官方凭据获取接口无 ctx，不能保证立即取消正在刷新的凭据。
//
// UpdateSynonymsDicts 接收完整目标清单，遗漏已有文件会删除它；本包拒绝空清单。
// KeepSynonymsDict 保留已有文件，SynonymsDictFromOSS 新增或替换同名文件。
// 文件名仅允许字母、数字、下划线和 .txt 后缀，总长最多 30；构造器只组装，调用时校验。
// ClientToken 必填且最多 64 个 ASCII 字符，同次重试复用、新更新更换。
// 同实例更新须由调用方串行化；查询清单后更新没有 CAS 保证。
// OSS 对象须满足服务端访问和地域限制；上传使用不同账号不等于管理服务可以读取对象。
// 本包不调整 ACL，不编排上传、等待分发或官方 reload。
// 更新成功不证明词库已分发、索引已加载或实例不会重启；DescribeInstance 的 active 也不证明词库就绪。
//
// 结果日志通过 OptLogRequests(true) 开启，默认关闭；不输出正文、下载 URL 或原始错误文本。
// SDK 错误通过 %w 保留，可用 errors.As 识别；取消和超时原因可用 errors.Is 判断。
// 返回的错误仍可能含敏感详情，不宜直接记录。
// 日志消息和下游类型统一为 AliyunES，耗时字段为 AliyunES_<Name>_<序号>_duration_ms。
// logit.WithStart(ctx) 启用请求级耗时，与结果日志开关无关。
// SDK 自身 dara/credential 的进程级 DEBUG 调试开关须由部署方关闭。
//
// 以下示例中的 accessKeyID、accessKeySecret 来自 ES 管理账号的部署配置，
// ctx 为请求 context；应用已经确认目标清单只有下列两个文件：
//
//	client, err := esx.New(esx.Config{
//		Name:            "synonyms-control",
//		Region:          "cn-hangzhou",
//		InstanceID:      "es-instance-id",
//		AccessKeyID:     accessKeyID,
//		AccessKeySecret: accessKeySecret,
//	}, esx.OptLogRequests(true))
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//	result, err := client.UpdateSynonymsDicts(ctx, esx.UpdateSynonymsDictsRequest{
//		ClientToken: "unique-release-token",
//		Dicts: []esx.SynonymsDict{
//			esx.KeepSynonymsDict("other.txt"),
//			esx.SynonymsDictFromOSS("synonyms.txt", "synonyms-bucket", "releases/v1/synonyms.txt"),
//		},
//	})
//	if err != nil {
//		return err
//	}
//	_ = result.RequestID
//
// InstanceID 从旧操作参数移至 Config；ListSynonymsDicts(ctx)、DescribeInstance(ctx) 仅接收 ctx，
// UpdateSynonymsDictsRequest 仅包含 ClientToken 和 Dicts，查询结果中的 InstanceID 保留服务端值。
// biz/bootstrap 通过 MustRegisterAliyunES(name, isDefault) 声明实例，
// MustBaseInit 读取 aliyunesx.<name>.yaml 并创建固定 AK/SK 客户端。
//
// NewNamed/NewDefault 仅在启动阶段串行初始化，Named/Default 在未初始化或
// CloseAll 开始后 panic。共享实例统一登记 CloseAll 至 lifecycle.Stack；
// 独立 New 登记自己的 Close。先停止并等待使用方，再关闭连接，日志最后关闭。
// Close 幂等，不排空在途请求，不关闭借用凭据，也不影响 ossx。
package esx
