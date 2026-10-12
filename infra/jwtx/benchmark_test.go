package jwtx_test

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/jwtx"
	"github.com/golang-jwt/jwt/v5"
)

// BenchmarkJWTAlgorithms 比较相同 claims 下四种算法的完整签发与验签成本。
// 密钥生成、PEM 解析和实例初始化不计时；验签每次使用新的空 claims。
// RS256、PS256 共用同一把 2048 位 RSA 密钥，ES256 使用 P-256，HS256 使用 32 字节密钥。
func BenchmarkJWTAlgorithms(b *testing.B) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		b.Fatal(err)
	}
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		b.Fatal(err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		b.Fatal(err)
	}
	now := time.Now()
	claims := jwt.RegisteredClaims{
		Issuer:    "auth",
		Subject:   "user-42",
		Audience:  jwt.ClaimStrings{"app"},
		ID:        "token-42",
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(24 * time.Hour)),
	}
	for _, fixture := range []struct {
		algorithm jwtx.Algorithm
		key       crypto.Signer
	}{
		{algorithm: jwtx.HS256},
		{algorithm: jwtx.RS256, key: rsaKey},
		{algorithm: jwtx.ES256, key: ecKey},
		{algorithm: jwtx.PS256, key: rsaKey},
	} {
		b.Run(string(fixture.algorithm), func(b *testing.B) {
			signer, verifier := benchmarkJWTInstances(b, fixture.algorithm, fixture.key, secret)
			raw, err := signer.Sign(claims)
			if err != nil {
				b.Fatal(err)
			}
			var parsed jwt.RegisteredClaims
			if err := verifier.Verify(raw, &parsed); err != nil {
				b.Fatal(err)
			}
			b.Run("Sign", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := signer.Sign(claims); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Verify", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var parsed jwt.RegisteredClaims
					if err := verifier.Verify(raw, &parsed); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func benchmarkJWTInstances(b *testing.B, algorithm jwtx.Algorithm, key crypto.Signer, secret []byte) (*jwtx.Signer, *jwtx.Verifier) {
	b.Helper()
	signCfg := jwtx.SignConfig{
		Algorithm: algorithm,
	}
	verifyCfg := jwtx.VerifyConfig{
		Algorithm:     algorithm,
		Issuer:        "auth",
		Audience:      []string{"app"},
		CheckIssuedAt: true,
	}
	if algorithm == jwtx.HS256 {
		signCfg.Secret = secret
		verifyCfg.Secret = secret
	} else {
		privateDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			b.Fatal(err)
		}
		publicDER, err := x509.MarshalPKIXPublicKey(key.Public())
		if err != nil {
			b.Fatal(err)
		}
		signCfg.PrivateKeyPEM = pem.EncodeToMemory(&pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: privateDER,
		})
		verifyCfg.PublicKeyPEM = pem.EncodeToMemory(&pem.Block{
			Type:  "PUBLIC KEY",
			Bytes: publicDER,
		})
	}
	signer, err := jwtx.NewSigner(signCfg)
	if err != nil {
		b.Fatal(err)
	}
	verifier, err := jwtx.NewVerifier(verifyCfg)
	if err != nil {
		b.Fatal(err)
	}
	return signer, verifier
}
