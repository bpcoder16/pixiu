package jwtx

import (
	"crypto/elliptic"
	"fmt"
	"reflect"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// Verifier 是可并发复用的验签器；零值不可用，须经 NewVerifier 创建。
type Verifier struct {
	parser  *jwt.Parser
	keyFunc jwt.Keyfunc
}

// NewVerifier 解析密钥并固定校验策略；证书 PEM 仅用于提取公钥。
func NewVerifier(cfg VerifyConfig) (*Verifier, error) {
	if cfg.Leeway < 0 {
		return nil, fmt.Errorf("jwtx: Leeway must not be negative")
	}
	if cfg.Issuer != "" && strings.TrimSpace(cfg.Issuer) == "" {
		return nil, fmt.Errorf("jwtx: Issuer must not be whitespace")
	}
	for _, audience := range cfg.Audience {
		if strings.TrimSpace(audience) == "" {
			return nil, fmt.Errorf("jwtx: Audience must not contain empty or whitespace values")
		}
	}
	key, err := verificationKey(cfg)
	if err != nil {
		return nil, err
	}
	opts := []jwt.ParserOption{
		jwt.WithValidMethods([]string{string(cfg.Algorithm)}),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(cfg.Leeway),
	}
	if cfg.Issuer != "" {
		opts = append(opts, jwt.WithIssuer(cfg.Issuer))
	}
	if len(cfg.Audience) != 0 {
		opts = append(opts, jwt.WithAudience(append([]string(nil), cfg.Audience...)...))
	}
	if cfg.CheckIssuedAt {
		opts = append(opts, jwt.WithIssuedAt())
	}
	var pss *jwt.SigningMethodRSAPSS
	if cfg.Algorithm == PS256 {
		pss = ps256Method()
	}
	return &Verifier{
		parser: jwt.NewParser(opts...),
		keyFunc: func(token *jwt.Token) (any, error) {
			if pss != nil {
				// Parser 已通过精确算法检查；只替换本次 token 的方法以严格验证盐长度。
				// 不修改全局注册的方法，避免影响应用内其他 JWT 使用方。
				token.Method = pss
			}
			// 固定配置的信任来源，忽略 token 自带的 kid、jku 等密钥提示。
			return key, nil
		},
	}, nil
}

func verificationKey(cfg VerifyConfig) (any, error) {
	switch cfg.Algorithm {
	case HS256:
		if len(cfg.PublicKeyPEM) != 0 {
			return nil, fmt.Errorf("jwtx: HS256 does not accept PublicKeyPEM")
		}
		return copySecret(cfg.Secret)
	case RS256, PS256:
		if len(cfg.Secret) != 0 {
			return nil, fmt.Errorf("jwtx: %s does not accept Secret", cfg.Algorithm)
		}
		key, err := jwt.ParseRSAPublicKeyFromPEM(cfg.PublicKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("jwtx: parse RSA public key: %w", err)
		}
		if key.N.BitLen() < 2048 {
			return nil, fmt.Errorf("jwtx: RSA key must contain at least 2048 bits")
		}
		return key, nil
	case ES256:
		if len(cfg.Secret) != 0 {
			return nil, fmt.Errorf("jwtx: ES256 does not accept Secret")
		}
		key, err := jwt.ParseECPublicKeyFromPEM(cfg.PublicKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("jwtx: parse EC public key: %w", err)
		}
		if key.Curve != elliptic.P256() {
			return nil, fmt.Errorf("jwtx: ES256 requires the P-256 curve")
		}
		return key, nil
	default:
		return nil, fmt.Errorf("jwtx: unsupported algorithm %q", cfg.Algorithm)
	}
}

// Verify 验证签名与声明；仅返回 nil 时 claims 才可信。
// 每次须传入新的空 claims 指针或已初始化的 jwt.MapClaims，不可并发共享目标。
// 失败时目标可能已写入不可信内容；raw 是 token 原文，不含 Bearer 前缀。
func (v *Verifier) Verify(raw string, claims jwt.Claims) error {
	if err := checkClaims(claims); err != nil {
		return err
	}
	if _, ok := claims.(jwt.MapClaims); !ok && reflect.ValueOf(claims).Kind() != reflect.Pointer {
		return fmt.Errorf("jwtx: claims must be a pointer or jwt.MapClaims: %w", jwt.ErrTokenInvalidClaims)
	}
	if _, err := v.parser.ParseWithClaims(raw, claims, v.keyFunc); err != nil {
		return fmt.Errorf("jwtx: verify token: %w", err)
	}
	return nil
}
