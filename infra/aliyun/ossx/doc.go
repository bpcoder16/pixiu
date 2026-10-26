// Package ossx 封装官方 SDK 的对象上传、元数据查询和下载签名。
// 导入路径为 github.com/bpcoder16/pixiu/infra/aliyun/ossx。
// 配置、凭据、连接池和命名注册表独立于 stsx、esx，可分别使用不同账号。
// Name、Region、Bucket 必填；每个实例固定一个 Bucket，多 Bucket 创建多个实例。
// New 按 SDK 规则校验 Region 语法和 Bucket 命名。
// 固定 AK/SK 通过 AccessKeyID、AccessKeySecret 提供，New 内部构造凭据；
// 也可手动注入 Credential，但与 AK/SK 字段互斥。RequestTimeout 默认 60 秒，SlowThreshold 默认 500 毫秒。
// New 只构造，不联网验活；Endpoint 可选，须为不含凭据、查询或非根路径的 HTTP(S) URL。
// 不自动重试或跟随重定向，保留 SDK 的 CRC64 校验；PutObject 不关闭调用方的 Reader。
// 普通 PUT 遵循 OSS 单对象上传限制，不提供分片上传；Key 原样传递，不改变对象 ACL。
// HeadObject 返回大小、MIME 和 ETag，对象缺失用 errors.Is(err, ErrObjectNotFound) 识别；桶错误原样保留。
// PresignGetObject 本地签发 GET URL，有效期还受所用临时凭据的剩余期限限制。
// STS 临时凭据由独立的 stsx.AssumeRole 签发，上传策略和响应组合由业务负责。
// SDK 错误可用 errors.As 识别；失败时保留 context 取消或超时原因供 errors.Is 判定，超时不代表远端没有写入。
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
//
// # RAM 权限配置
//
// 以下自定义权限策略允许被授权的 RAM 身份上传、读取、删除两个指定 Bucket 内的对象，
// 并允许扮演指定 RAM 角色。复制 JSON 时替换 Bucket 名称、<配置专属ID> 和角色名称；
// doc.go 每行开头的 // 是 Go 文档标记，不属于 JSON，粘贴到 RAM 控制台时须去掉。
//
//	{
//	  "Version": "1",
//	  "Statement": [
//	    {
//	      "Effect": "Allow",
//	      "Action": [
//	        "oss:PutObject",
//	        "oss:GetObject",
//	        "oss:DeleteObject"
//	      ],
//	      "Resource": [
//	        "acs:oss:*:*:demo-public/*",
//	        "acs:oss:*:*:demo-private/*"
//	      ]
//	    },
//	    {
//	      "Effect": "Allow",
//	      "Action": "sts:AssumeRole",
//	      "Resource": "acs:ram:*:<配置专属ID>:role/de-card-oss-upload-role"
//	    }
//	  ]
//	}
//
// 逐行说明，行号从上述 JSON 的第一个左花括号开始计数：
//   - 第 1 行：开始整个权限策略对象。
//   - 第 2 行：Version 是 RAM 权限策略语法版本，固定填写字符串 "1"。
//   - 第 3 行：Statement 开始授权规则数组，本示例包含 OSS 和 STS 两条规则。
//   - 第 4 行：开始第一条规则，用于授权 OSS 对象操作。
//   - 第 5 行：Effect 为 Allow，表示允许本条规则匹配的操作；不能覆盖其他策略的显式 Deny。
//   - 第 6 行：Action 开始允许执行的操作列表，列表内各操作分别授权。
//   - 第 7 行：oss:PutObject 允许上传对象，也可覆盖同名 Key 的对象。
//   - 第 8 行：oss:GetObject 允许读取或下载对象。
//   - 第 9 行：oss:DeleteObject 允许删除对象，不是删除 Bucket 的权限。
//   - 第 10 行：结束 Action 列表，逗号表示本条规则还有后续字段。
//   - 第 11 行：Resource 开始本条规则允许访问的资源列表。
//   - 第 12 行：允许上述三种操作访问 demo-public 内所有 Key，包括带目录前缀的 Key。
//   - 第 13 行：同样允许访问 demo-private 内所有 Key；两个 Bucket 都须换成实际名称。
//   - 第 14 行：结束 Resource 列表。
//   - 第 15 行：结束第一条规则，逗号表示 Statement 数组还有下一条规则。
//   - 第 16 行：开始第二条规则，用于授权 STS 角色扮演。
//   - 第 17 行：Effect 为 Allow，表示允许扮演 Resource 指定的角色。
//   - 第 18 行：sts:AssumeRole 允许调用 STS 换取该角色的临时凭证，本身不授予 OSS 操作权限。
//   - 第 19 行：只允许扮演 <配置专属ID> 所属账号下的 de-card-oss-upload-role 角色。
//   - 第 20 行：结束第二条规则。
//   - 第 21 行：结束 Statement 规则数组。
//   - 第 22 行：结束整个权限策略对象。
//
// OSS Resource 中 acs 表示阿里云资源，oss 表示 OSS 服务，两个 * 分别位于地域和账号位置，
// 不限定这两个字段；Bucket 名称仍是精确匹配，末尾 /* 匹配该 Bucket 内的全部对象。
// 如仅需操作某个前缀，可改为 acs:oss:*:*:demo-public/uploads/*。
// 本策略未授予列举 Bucket、列举对象、修改 ACL 或删除 Bucket 的权限。
// demo-public、demo-private 只是名称示例，不会据此设置公开或私有访问权限。
// RAM Resource 中 ram 表示 RAM 服务，* 是地域通配，role/ 后为目标角色名称，须与实际 ARN 一致。
//
// # 查找并填写 <配置专属ID>
//
// <配置专属ID> 是目标 RAM 角色所属阿里云主账号的账号 ID（Account ID，也称 UID），
// 不是 AccessKeyID、RAM 用户 ID、角色 ID、登录名或应用自行生成的 ID。
// 查找方法：
//   - 优先使用 RAM 控制台：身份管理 → 角色 → de-card-oss-upload-role → 基本信息，查看角色 ARN。
//     若角色不存在，先创建实际需要的角色。ARN 格式为 acs:ram::<账号ID>:role/<角色名称>，
//     将其中的账号 ID 填入 <配置专属ID>，角色名称也按 ARN 的实际值填写。
//   - 也可登录目标角色所属账号的阿里云控制台，将鼠标悬停在右上角头像，查看账号 ID；
//     RAM 用户登录时查看的是主账号 ID，也可由主账号在账号中心的概览页面查看。
//
// 例如目标角色 ARN 为 acs:ram::1234567890123456:role/de-card-oss-upload-role，
// 则示例第 19 行填写为：
//
//	"Resource": "acs:ram:*:1234567890123456:role/de-card-oss-upload-role"
//
// 1234567890123456 仅为示例，必须换成实际账号 ID，不保留尖括号；
// 跨账号扮演时填写目标角色所属账号 ID，不是发起 AssumeRole 的 RAM 用户所属账号 ID。
// 策略 Resource 可使用示例中的地域通配 *；调用 AssumeRole 的 RoleArn 参数则应复制完整角色 ARN，
// 使用 acs:ram::<账号ID>:role/<角色名称> 的格式。
//
// # 在 RAM 中授权并用于 ossx
//
// 在 RAM 控制台的权限管理 → 权限策略中创建自定义策略，选择脚本编辑并粘贴替换后的 JSON；
// 创建后还须在身份管理 → 用户中给实际使用 AccessKey 的 RAM 用户添加该策略，仅创建策略不会生效。
// PutObject、PutFile 需要 oss:PutObject；HeadObject 和 PresignGetObject 需要 oss:GetObject。
// stsx.AssumeRole 需要 sts:AssumeRole；业务应通过 Policy 将上传凭据限制到指定对象的 oss:PutObject。
// 固定 AK/SK 直接上传时，可移除整条 STS 规则，仅将 OSS 规则授予该 AK 所属 RAM 用户。
//
// 如需通过 STS 临时凭证上传，还须完成以下配置：
//   - 将第二条 sts:AssumeRole 规则授予发起调用的 RAM 用户或 RAM 角色；AssumeRole 不允许主账号调用。
//   - 给目标角色单独授予第一条 OSS 规则，并在该角色的信任策略 Principal 中允许调用者扮演。
//     用户具有 OSS 权限不会自动使目标角色具有相同权限；上述 JSON 是权限策略，不是角色信任策略。
//   - 独立初始化 stsx.Client，将角色 ARN、会话名和对象策略传给 AssumeRole。
//     STS 连接配置见 aliyunstsx.<name>.yaml；OSS 配置不再包含 STS 字段。
//     服务端上传、查询、下载签名仍使用 OSS 实例自身的凭据。
//
// 官方参考：
//   - OSS RAM Policy：https://help.aliyun.com/zh/oss/user-guide/ram-policy
//   - 查看账号 ID：https://help.aliyun.com/zh/account/check-account-id
//   - 查看角色 ARN：https://help.aliyun.com/zh/ram/user-guide/view-the-information-about-a-ram-role
//   - 角色扮演与信任策略：https://help.aliyun.com/zh/ram/user-guide/assume-a-ram-role
package ossx
