// Package cryptox 提供独立的 HMAC-SHA256、AES-GCM 和密码学安全随机数，只使用标准库。
// HMAC 适用于共享密钥的内部接口参数签名，AES-GCM 适用于分享 token 的保密与防篡改。
// 实例由 NewHMACSHA256 / NewAESGCM 构造并可并发复用；零值不可用。
//
// 以下分享示例需导入 github.com/bpcoder16/pixiu/cryptox。
// 临时生成密钥仅用于展示；生产环境应读取安全持久化的密钥，各用途独立用钥。
//
//	key, err := cryptox.RandomBytes(32)
//	if err != nil {
//		return err
//	}
//	box, err := cryptox.NewAESGCM(key)
//	if err != nil {
//		return err
//	}
//	token, err := box.EncryptToken([]byte(`{"resource":"file-42","exp":1800000300}`), "share-download")
//	if err != nil {
//		return err
//	}
//	payload, err := box.DecryptToken(token, "share-download")
//	if err != nil {
//		return err
//	}
//	// 仅在认证成功后解析 payload，并校验 exp、资源权限和撤销状态。
//	_ = payload
//
// HMAC 使用独立的至少 32 字节随机密钥，以下代码还需导入 errors：
//
//	signer, err := cryptox.NewHMACSHA256(signingKey)
//	if err != nil {
//		return err
//	}
//	signature, err := signer.Sign(message)
//	if err != nil {
//		return err
//	}
//	if !signer.Verify(message, signature) {
//		return errors.New("invalid request signature")
//	}
//
// signingKey 和 message 由应用提供；message 必须包含所有影响业务操作的字段。
// signature 是完整 32 字节认证码，不可截短。
// message 应按固定编码绑定用途、方法、目标 host、路径、查询、body 摘要、时间戳及 nonce。
// 包的外部示例展示长度前缀编码和拒绝重复 query key，应用还须验证时间窗口并原子去重。
// HMAC 验签方也能签名，不能把共享密钥交给不可信客户端；签名不隐藏参数。
//
// AES 字节接口输出 nonce(12) || ciphertext || tag(16)，支持空明文与可选 AAD。
// token 接口输出 v1. 加规范 Base64URL，purpose 必填，编码长度最多 MaxTokenLen 字节；
// 格式版本和 purpose 被认证，同一密钥跨所有实例最多加密 2^32 次，须提前轮换。
// 加密失败返回空结果，解密失败返回 nil 明文；本包不记录密钥、明文或 token。
//
// RandomToken(32) 可生成不携带明文的分享凭证；有效期、撤销和一次性消费由服务端记录实现。
// 本包不替代 HTTPS、业务授权、重放防护或密码存储方案，也不提供 JWT/JWE。
// 完整线格式、安全约束与验证方式见 docs/cryptox-design.md。
package cryptox
