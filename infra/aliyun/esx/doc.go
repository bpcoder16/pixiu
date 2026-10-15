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
//
// # RAM 权限配置
//
// 以下自定义权限策略允许被授权的 RAM 身份管理阿里云 Elasticsearch 资源，
// 并读取两个指定 Bucket 内的对象及相关元数据。复制 JSON 时替换 Bucket 名称；
// doc.go 每行开头的 // 是 Go 文档标记，不属于 JSON，粘贴到 RAM 控制台时须去掉。
//
//	{
//	  "Version": "1",
//	  "Statement": [
//	    {
//	      "Effect": "Allow",
//	      "Action": "elasticsearch:*",
//	      "Resource": "*"
//	    },
//	    {
//	      "Effect": "Allow",
//	      "Action": [
//	        "oss:GetObject",
//	        "oss:GetObjectMetadata",
//	        "oss:GetObjectMeta"
//	      ],
//	      "Resource": [
//	        "acs:oss:*:*:demo-public/*",
//	        "acs:oss:*:*:demo-private/*"
//	      ]
//	    }
//	  ]
//	}
//
// 逐行说明，行号从上述 JSON 的第一个左花括号开始计数：
//   - 第 1 行：开始整个权限策略对象。
//   - 第 2 行：Version 是 RAM 权限策略语法版本，固定填写字符串 "1"。
//   - 第 3 行：Statement 开始授权规则数组，本示例包含 ES 管理和 OSS 读取两条规则。
//   - 第 4 行：开始第一条规则，用于授权 ES 管理操作。
//   - 第 5 行：Effect 为 Allow，表示允许本条规则匹配的操作；不能覆盖其他策略的显式 Deny。
//   - 第 6 行：elasticsearch:* 匹配 ES 的全部 RAM 管理操作，包括创建、修改和删除等操作。
//   - 第 7 行：Resource 为 *，表示本条规则不限定具体 ES 资源，不限于 Config.InstanceID。
//   - 第 8 行：结束第一条规则，逗号表示 Statement 数组还有下一条规则。
//   - 第 9 行：开始第二条规则，用于授权读取 OSS 对象和元数据。
//   - 第 10 行：Effect 为 Allow，表示允许本条规则列出的 OSS 读取操作。
//   - 第 11 行：Action 开始允许执行的操作列表，列表内各操作分别授权。
//   - 第 12 行：oss:GetObject 允许读取或下载对象内容。
//   - 第 13 行：oss:GetObjectMetadata 用于读取对象元数据，名称沿用官方 ES 服务角色策略。
//   - 第 14 行：oss:GetObjectMeta 用于读取对象的简化元数据，如 ETag、大小和最后修改时间。
//   - 第 15 行：结束 Action 列表，逗号表示本条规则还有后续字段。
//   - 第 16 行：Resource 开始本条规则允许访问的 OSS 资源列表。
//   - 第 17 行：允许上述读取操作访问 demo-public 内所有 Key，包括带目录前缀的 Key。
//   - 第 18 行：同样允许访问 demo-private 内所有 Key；两个 Bucket 都须换成实际名称。
//   - 第 19 行：结束 Resource 列表。
//   - 第 20 行：结束第二条规则。
//   - 第 21 行：结束 Statement 规则数组。
//   - 第 22 行：结束整个权限策略对象。
//
// 第一条规则授予的管理权限超出本包的词典清单、词典更新和实例查询接口，
// 不等于 Elasticsearch 集群内部的索引读写权限；集群内部鉴权需另行配置。
// 第二条规则中的 acs 表示阿里云资源，oss 表示 OSS 服务，两个 * 分别位于地域和账号位置，
// 不限定这两个字段；Bucket 名称仍是精确匹配，末尾 /* 匹配该 Bucket 内的全部对象。
// 如仅需读取某个前缀，可改为 acs:oss:*:*:demo-public/releases/*。
// 本策略未授予 OSS 上传、删除、列举对象或修改 ACL 的权限。
// demo-public、demo-private 只是名称示例，不会据此设置公开或私有访问权限。
// 上述三个 OSS 动作与官方 ES 服务角色策略一致；OSS API 文档将 HeadObject、
// GetObjectMeta 的访问权限列为 oss:GetObject，接口名称与 RAM Action 不应直接等同。
//
// # 在 RAM 中授权并用于 esx
//
// 在 RAM 控制台的权限管理 → 权限策略中创建自定义策略，选择脚本编辑并粘贴替换后的 JSON；
// 创建后还须将策略授予实际调用 ES 管理 API 的 RAM 身份，仅创建策略不会生效。
// 使用固定 AK/SK 时，授权对象为 Config.AccessKeyID 所属的 RAM 用户。
//
// ES 管理 API 的调用身份与 ES 服务读取 OSS 词典的身份分别授权；
// 将上述策略授予调用方，不会自动授予 ES 服务读取 OSS 对象的权限。
// 通过 OSS 路径更新词典时，还须按 ES 控制台引导授权普通服务角色
// AliyunElasticsearchAccessingOSSRole，其可信服务为 elasticsearch.aliyuncs.com。
// 该角色按词典来源配置 OSS 读取权限，可参照第二条规则限定 Bucket 或 Key 前缀。
// 本包只调用 ES 管理 API，不创建该角色，也不配置其信任策略或权限策略。
//
// 官方参考：
//   - ES 管理权限：https://help.aliyun.com/zh/ram/developer-reference/aliyunelasticsearchfullaccess
//   - ES 普通服务角色：https://help.aliyun.com/zh/es/security-and-compliance/es-common-service-role
//   - ES 读取 OSS 策略：https://help.aliyun.com/zh/ram/developer-reference/aliyunelasticsearchaccessingossrolepolicy
//   - OSS 授权语法与元素：https://help.aliyun.com/zh/oss/user-guide/authorization-syntax-and-elements
package esx
