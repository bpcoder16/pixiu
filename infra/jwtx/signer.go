package jwtx

import (
	"crypto/elliptic"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

// Signer 是可并发复用的签发器；零值不可用，须经 NewSigner 创建。
type Signer struct {
	method jwt.SigningMethod
	key    any
}

// NewSigner 校验配置并复制或解析密钥，不保留配置切片。
func NewSigner(cfg SignConfig) (*Signer, error) {
	switch cfg.Algorithm {
	case HS256:
		if len(cfg.PrivateKeyPEM) != 0 {
			return nil, fmt.Errorf("jwtx: HS256 does not accept PrivateKeyPEM")
		}
		secret, err := copySecret(cfg.Secret)
		if err != nil {
			return nil, err
		}
		return &Signer{
			method: jwt.SigningMethodHS256,
			key:    secret,
		}, nil
	case RS256, PS256:
		if len(cfg.Secret) != 0 {
			return nil, fmt.Errorf("jwtx: %s does not accept Secret", cfg.Algorithm)
		}
		key, err := jwt.ParseRSAPrivateKeyFromPEM(cfg.PrivateKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("jwtx: parse RSA private key: %w", err)
		}
		if key.N.BitLen() < 2048 {
			return nil, fmt.Errorf("jwtx: RSA key must contain at least 2048 bits")
		}
		if err = key.Validate(); err != nil {
			return nil, fmt.Errorf("jwtx: invalid RSA private key: %w", err)
		}
		// 在发布实例前完成预计算，之后各签发调用只读取密钥。
		key.Precompute()
		var method jwt.SigningMethod = jwt.SigningMethodRS256
		if cfg.Algorithm == PS256 {
			method = ps256Method()
		}
		return &Signer{
			method: method,
			key:    key,
		}, nil
	case ES256:
		if len(cfg.Secret) != 0 {
			return nil, fmt.Errorf("jwtx: ES256 does not accept Secret")
		}
		key, err := jwt.ParseECPrivateKeyFromPEM(cfg.PrivateKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("jwtx: parse EC private key: %w", err)
		}
		if key.Curve != elliptic.P256() {
			return nil, fmt.Errorf("jwtx: ES256 requires the P-256 curve")
		}
		return &Signer{
			method: jwt.SigningMethodES256,
			key:    key,
		}, nil
	default:
		return nil, fmt.Errorf("jwtx: unsupported algorithm %q", cfg.Algorithm)
	}
}

// Sign 要求 exp 存在并原样签名，不补写声明或执行时间、业务校验。
// 调用期间不得修改 claims；自定义 getter 和 JSON 编码须由调用方保证正确。
// jwt.MapClaims 的 exp 应使用 float64 或 json.Number 秒值，遵循上游 getter 契约。
func (s *Signer) Sign(claims jwt.Claims) (string, error) {
	if err := checkClaims(claims); err != nil {
		return "", err
	}
	exp, err := claims.GetExpirationTime()
	if err != nil {
		return "", fmt.Errorf("jwtx: read exp: %w", err)
	}
	if exp == nil {
		return "", fmt.Errorf("jwtx: exp is required: %w", jwt.ErrTokenRequiredClaimMissing)
	}
	raw, err := jwt.NewWithClaims(s.method, claims).SignedString(s.key)
	if err != nil {
		return "", fmt.Errorf("jwtx: sign token: %w", err)
	}
	return raw, nil
}
