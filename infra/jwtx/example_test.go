package jwtx_test

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"time"

	"github.com/bpcoder16/pixiu/infra/jwtx"
	"github.com/golang-jwt/jwt/v5"
)

func Example() {
	// 生产环境从安全配置读取持久密钥；这里临时生成用于展示。
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		panic(err)
	}
	signer, err := jwtx.NewSigner(jwtx.SignConfig{
		Algorithm: jwtx.HS256,
		Secret:    secret,
	})
	if err != nil {
		panic(err)
	}
	verifier, err := jwtx.NewVerifier(jwtx.VerifyConfig{
		Algorithm: jwtx.HS256,
		Secret:    secret,
		Issuer:    "example",
		Audience:  []string{"app"},
	})
	if err != nil {
		panic(err)
	}
	type appClaims struct {
		UserID string `json:"uid"`
		Portal string `json:"portal"`
		jwt.RegisteredClaims
	}
	now := time.Now()
	raw, err := signer.Sign(appClaims{
		UserID: "user-42",
		Portal: "app",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "example",
			Audience:  jwt.ClaimStrings{"app"},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(15 * time.Minute)),
		},
	})
	if err != nil {
		panic(err)
	}
	var claims appClaims
	if err = verifier.Verify(raw, &claims); err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			fmt.Println("token 已过期")
		}
		return
	}
	fmt.Println(claims.UserID, claims.Portal)
	// Output: user-42 app
}

func ExampleNewSigner_asymmetric() {
	// 示例生成临时密钥；实际应用从安全配置读取持久 PEM。
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	for _, config := range []struct {
		algorithm jwtx.Algorithm
		key       crypto.Signer
	}{
		{
			algorithm: jwtx.ES256,
			key:       ecKey,
		},
		{
			algorithm: jwtx.PS256,
			key:       rsaKey,
		},
	} {
		privateDER, err := x509.MarshalPKCS8PrivateKey(config.key)
		if err != nil {
			panic(err)
		}
		publicDER, err := x509.MarshalPKIXPublicKey(config.key.Public())
		if err != nil {
			panic(err)
		}
		signer, err := jwtx.NewSigner(jwtx.SignConfig{
			Algorithm: config.algorithm,
			PrivateKeyPEM: pem.EncodeToMemory(&pem.Block{
				Type:  "PRIVATE KEY",
				Bytes: privateDER,
			}),
		})
		if err != nil {
			panic(err)
		}
		verifier, err := jwtx.NewVerifier(jwtx.VerifyConfig{
			Algorithm: config.algorithm,
			PublicKeyPEM: pem.EncodeToMemory(&pem.Block{
				Type:  "PUBLIC KEY",
				Bytes: publicDER,
			}),
			Issuer:   "auth",
			Audience: []string{"app"},
		})
		if err != nil {
			panic(err)
		}
		raw, err := signer.Sign(jwt.RegisteredClaims{
			Issuer:    "auth",
			Subject:   "user-42",
			Audience:  jwt.ClaimStrings{"app"},
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
		})
		if err != nil {
			panic(err)
		}
		var claims jwt.RegisteredClaims
		if err = verifier.Verify(raw, &claims); err != nil {
			panic(err)
		}
		fmt.Println(config.algorithm, claims.Subject)
	}
	// Output:
	// ES256 user-42
	// PS256 user-42
}
