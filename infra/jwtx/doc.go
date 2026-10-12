// Package jwtx 提供独立的 HS256、RS256、ES256、PS256 JWT 签发器与验签器，复用 golang-jwt/jwt v5。
// 实例固定算法与密钥并可并发使用；声明的业务含义、刷新会话、撤销与 HTTP 适配由调用方负责。
// 设计见 docs/jwtx-design.md。
//
// 以下示例需导入 crypto/rand、time、github.com/golang-jwt/jwt/v5 和
// github.com/bpcoder16/pixiu/infra/jwtx。生产环境应从安全配置读取持久密钥，
// 下例临时生成密钥仅用于展示调用：
//
//	secret := make([]byte, 32)
//	if _, err := rand.Read(secret); err != nil {
//		return err
//	}
//	signer, err := jwtx.NewSigner(jwtx.SignConfig{
//		Algorithm: jwtx.HS256,
//		Secret:    secret,
//	})
//	if err != nil {
//		return err
//	}
//	verifier, err := jwtx.NewVerifier(jwtx.VerifyConfig{
//		Algorithm: jwtx.HS256,
//		Secret:    secret,
//		Issuer:    "example",
//		Audience:  []string{"app"},
//	})
//	if err != nil {
//		return err
//	}
//	now := time.Now()
//	raw, err := signer.Sign(jwt.RegisteredClaims{
//		Issuer:    "example",
//		Subject:   "user-42",
//		Audience:  jwt.ClaimStrings{"app"},
//		IssuedAt:  jwt.NewNumericDate(now),
//		ExpiresAt: jwt.NewNumericDate(now.Add(15 * time.Minute)),
//	})
//	if err != nil {
//		return err
//	}
//	var claims jwt.RegisteredClaims
//	if err := verifier.Verify(raw, &claims); err != nil {
//		return err
//	}
//	// 仅在 Verify 成功后使用 claims.Subject 等身份字段。
//	return nil
//
// 自定义 claims 推荐按值嵌入 jwt.RegisteredClaims，保留自身 JSON 布局。
// Sign 要求 exp 存在但不修改或验证业务声明；Verify 始终要求 exp 并检查时间。
// 每次验签须使用新的空 claims 对象，出错时丢弃可能已解码的不可信内容。
// 可使用 errors.Is(err, jwt.ErrTokenExpired) 等判断底层错误，HTTP 错误码由应用映射。
// HS256 密钥至少 32 字节；RS256、PS256 使用至少 2048 位的 RSA 密钥。
// ES256 只接受 P-256，私钥支持 SEC1 / PKCS#8 PEM，公钥支持 PKIX / 证书 PEM。
// PS256 沿用 RSA PEM 格式，签发和验签均要求标准的 32 字节盐长度。
// 非对称算法通过 PrivateKeyPEM / PublicKeyPEM 配置，不传 Secret；验签器只需公钥。
// 证书 PEM 仅提取公钥，不校验证书链或证书有效期。
package jwtx
