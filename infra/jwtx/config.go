package jwtx

import (
	"crypto"
	"crypto/rsa"
	"fmt"
	"reflect"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Algorithm 是实例固定使用的签名算法。
// 性能须分别比较签发和验签，并考虑 Go 版本、CPU、密钥长度及 claims 大小。
// 以下性能描述基于本机基准，环境与数据见 docs/jwtx-design.md；可运行 BenchmarkJWTAlgorithms 复测。
type Algorithm string

const (
	// HS256 使用 HMAC-SHA256；本机基准中签发、验签的耗时均最低。
	// 使用共享密钥，持有验签密钥的一方也具备签发能力。
	HS256 Algorithm = "HS256"

	// RS256 使用 RSA PKCS#1 v1.5；本机 2048 位 RSA 基准中签发成本明显高于验签。
	// RSA 密钥长度影响运算成本，不能将 2048 位基准直接用于更长的密钥。
	RS256 Algorithm = "RS256"

	// ES256 使用 P-256 ECDSA；本机基准中签发快于 2048 位 RSA，验签慢于 RSA。
	// 原始签名为 64 字节，小于 2048 位 RSA 的 256 字节，可减少 Token 传输体积。
	ES256 Algorithm = "ES256"

	// PS256 使用 RSA-PSS；同一把 2048 位 RSA 密钥下，本机签发、验签成本与 RS256 接近。
	// PSS 另有随机盐和编码处理，不应假定它与 RS256 存在固定的性能排序。
	PS256 Algorithm = "PS256"
)

// SignConfig 配置签发器；必须显式指定算法及其对应的密钥，不自动生成密钥。
// 密钥由调用方从安全配置或文件读取；字段接收密钥内容，不接受文件路径。
// 缺少必填字段、传入该算法不接受的非空密钥字段或密钥无效时，NewSigner 返回错误。
type SignConfig struct {
	// Algorithm 必填，无默认值，不根据密钥类型自动推断。
	// 支持 HS256（HMAC）、RS256（RSA PKCS#1 v1.5）、ES256（ECDSA）、PS256（RSA-PSS），均使用 SHA-256。
	Algorithm Algorithm

	// Secret 仅在 HS256 时必填，至少 32 字节；此时 PrivateKeyPEM 必须留空。
	// RS256、ES256、PS256 不接受 Secret，必须留空（nil 或长度为 0）。
	// 接收原始密钥字节；配置中若保存的是 Base64 或十六进制文本，须由调用方先解码。
	// 生产环境应使用安全随机生成并持久保存的密钥，启动时读取，不应每次启动重新生成。
	Secret []byte

	// PrivateKeyPEM 在 RS256、ES256、PS256 时必填；此时 Secret 必须留空。
	// HS256 不接受 PrivateKeyPEM，必须留空（nil 或长度为 0）。
	// RS256、PS256：RSA 私钥，至少 2048 位，支持 PKCS#1 / PKCS#8 PEM。
	// ES256：ECDSA 私钥，必须使用 P-256 曲线，支持 SEC1 / PKCS#8 PEM。
	// 接收完整、未加密的私钥 PEM 内容，不能传入公钥或文件路径。
	PrivateKeyPEM []byte
}

// VerifyConfig 配置验签器；exp 始终必需，算法与密钥在构造后固定。
// 密钥由调用方从安全配置或文件读取；字段接收密钥内容，不接受文件路径。
// 缺少必填字段、传入该算法不接受的非空密钥字段或配置无效时，NewVerifier 返回错误。
type VerifyConfig struct {
	// Algorithm 必填，无默认值，不根据密钥类型或 token 中的 alg 自动推断。
	// 支持 HS256、RS256、ES256、PS256；token 中的 alg 必须与此配置精确匹配。
	Algorithm Algorithm

	// Secret 仅在 HS256 时必填，至少 32 字节；此时 PublicKeyPEM 必须留空。
	// 必须与签发器使用相同的原始密钥字节；Base64 或十六进制文本须由调用方先解码。
	// RS256、ES256、PS256 不接受 Secret，必须留空（nil 或长度为 0）。
	Secret []byte

	// PublicKeyPEM 在 RS256、ES256、PS256 时必填；此时 Secret 必须留空。
	// HS256 不接受 PublicKeyPEM，必须留空（nil 或长度为 0）。
	// RS256、PS256：RSA 公钥，至少 2048 位，支持 PKIX / PKCS#1 PEM。
	// ES256：ECDSA 公钥，必须使用 P-256 曲线，支持 PKIX PEM。
	// 均支持证书 PEM，但仅提取公钥，不校验证书链或证书有效期。
	// 接收与签发私钥配对的完整公钥或证书 PEM 内容，不能传入私钥或文件路径。
	PublicKeyPEM []byte

	// Issuer 非空时要求 iss 存在且精确匹配；为空时不校验签发者。
	// 不接受仅含空白的配置值，不自动去除首尾空白。
	Issuer string

	// Audience 非空时要求 aud 至少匹配其中一个值；为空时不校验接收方。
	// 每个元素均不得为空字符串或仅含空白，不自动去除首尾空白。
	Audience []string

	// Leeway 是时间校验允许的偏差，默认 0，不得为负。
	// 用于 exp、nbf，以及开启 CheckIssuedAt 后的 iat 校验，以容忍时钟偏差。
	Leeway time.Duration

	// CheckIssuedAt 默认 false；开启后检查已有 iat 不晚于当前时间（计入 Leeway）。
	// 不要求 iat 存在，也不会自动补写签发时间。
	CheckIssuedAt bool
}

func ps256Method() *jwt.SigningMethodRSAPSS {
	// 实例独立持有标准 PSS 参数，不采用上游兼容任意盐长度的 VerifyOptions。
	return &jwt.SigningMethodRSAPSS{
		SigningMethodRSA: &jwt.SigningMethodRSA{
			Name: string(PS256),
			Hash: crypto.SHA256,
		},
		Options: &rsa.PSSOptions{
			SaltLength: rsa.PSSSaltLengthEqualsHash,
		},
	}
}

func copySecret(secret []byte) ([]byte, error) {
	if len(secret) < 32 {
		return nil, fmt.Errorf("jwtx: HS256 secret must contain at least 32 bytes")
	}
	return append([]byte(nil), secret...), nil
}

func checkClaims(claims jwt.Claims) error {
	if claims == nil {
		return fmt.Errorf("jwtx: nil claims: %w", jwt.ErrTokenInvalidClaims)
	}
	value := reflect.ValueOf(claims)
	if (value.Kind() == reflect.Pointer || value.Kind() == reflect.Map) && value.IsNil() {
		return fmt.Errorf("jwtx: nil claims: %w", jwt.ErrTokenInvalidClaims)
	}
	return nil
}
