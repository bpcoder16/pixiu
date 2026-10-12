package jwtx

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func ecKey(t *testing.T, curve elliptic.Curve) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func ecPrivatePEM(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: der,
	})
}

func pkcs8PEM(t *testing.T, key any) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: der,
	})
}

func certificatePEM(t *testing.T, key crypto.Signer) []byte {
	t.Helper()
	// 验签器只提取公钥；过期的自签证书也可作为公钥容器。
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-2 * time.Hour),
		NotAfter:     time.Now().Add(-time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	})
}

func TestES256AndPS256Interoperability(t *testing.T) {
	ec := ecKey(t, elliptic.P256())
	rsaPriv := rsaKey(t, 2048)
	for _, tt := range []struct {
		algorithm Algorithm
		method    jwt.SigningMethod
		key       crypto.Signer
		private   []byte
	}{
		{
			algorithm: ES256,
			method:    jwt.SigningMethodES256,
			key:       ec,
			private:   ecPrivatePEM(t, ec),
		},
		{
			algorithm: PS256,
			method:    jwt.SigningMethodPS256,
			key:       rsaPriv,
			private:   privatePEM(rsaPriv),
		},
	} {
		t.Run(string(tt.algorithm), func(t *testing.T) {
			claims := accessClaims{
				UserID:           "user-42",
				Portal:           "app",
				RegisteredClaims: validClaims(),
			}
			claims.Issuer = "auth"
			claims.Audience = jwt.ClaimStrings{"app"}
			publics := [][]byte{publicPEM(t, tt.key.Public()), certificatePEM(t, tt.key)}
			if tt.algorithm == PS256 {
				publics = append(publics, pem.EncodeToMemory(&pem.Block{
					Type:  "RSA PUBLIC KEY",
					Bytes: x509.MarshalPKCS1PublicKey(&rsaPriv.PublicKey),
				}))
			}
			for _, private := range [][]byte{tt.private, pkcs8PEM(t, tt.key)} {
				signer, err := NewSigner(SignConfig{
					Algorithm:     tt.algorithm,
					PrivateKeyPEM: private,
				})
				if err != nil {
					t.Fatal(err)
				}
				raw, err := signer.Sign(claims)
				if err != nil {
					t.Fatal(err)
				}
				var parsed accessClaims
				token, err := jwt.ParseWithClaims(raw, &parsed, func(*jwt.Token) (any, error) {
					return tt.key.Public(), nil
				}, jwt.WithValidMethods([]string{string(tt.algorithm)}))
				if err != nil {
					t.Fatalf("原生库验签失败: %v", err)
				}
				if parsed.UserID != claims.UserID || parsed.Portal != claims.Portal {
					t.Fatalf("claims 不匹配: %+v", parsed)
				}
				if tt.algorithm == ES256 && len(token.Signature) != 64 {
					t.Fatalf("ES256 签名长度 = %d", len(token.Signature))
				}
				if tt.algorithm == PS256 {
					input := raw[:strings.LastIndexByte(raw, '.')]
					digest := sha256.Sum256([]byte(input))
					if err = rsa.VerifyPSS(&rsaPriv.PublicKey, crypto.SHA256, digest[:], token.Signature, &rsa.PSSOptions{
						SaltLength: rsa.PSSSaltLengthEqualsHash,
					}); err != nil {
						t.Fatalf("PS256 签发未使用标准盐长度: %v", err)
					}
				}
				for _, public := range publics {
					verifier, err := NewVerifier(VerifyConfig{
						Algorithm:    tt.algorithm,
						PublicKeyPEM: public,
						Issuer:       "auth",
						Audience:     []string{"app"},
					})
					if err != nil {
						t.Fatal(err)
					}
					for _, signed := range []string{raw, nativeToken(t, tt.method, claims, tt.key)} {
						var got accessClaims
						if err = verifier.Verify(signed, &got); err != nil {
							t.Fatal(err)
						}
						if got.UserID != claims.UserID || got.Portal != claims.Portal {
							t.Fatalf("claims 不匹配: %+v", got)
						}
					}
					missingExp := claims
					missingExp.ExpiresAt = nil
					if err = verifier.Verify(nativeToken(t, tt.method, missingExp, tt.key), &accessClaims{}); !errors.Is(err, jwt.ErrTokenRequiredClaimMissing) {
						t.Fatalf("缺少 exp 的错误 = %v", err)
					}
				}
			}
		})
	}
}

func TestES256AndPS256RejectInvalidKeys(t *testing.T) {
	ec := ecKey(t, elliptic.P256())
	p384 := ecKey(t, elliptic.P384())
	p521 := ecKey(t, elliptic.P521())
	rsaPriv := rsaKey(t, 2048)
	weakRSA := rsaKey(t, 1024)
	for _, tt := range []struct {
		algorithm Algorithm
		private   []byte
		public    []byte
		badPriv   [][]byte
		badPub    [][]byte
	}{
		{
			algorithm: ES256,
			private:   ecPrivatePEM(t, ec),
			public:    publicPEM(t, &ec.PublicKey),
			badPriv: [][]byte{
				ecPrivatePEM(t, p384),
				pkcs8PEM(t, p521),
				pkcs8PEM(t, rsaPriv),
			},
			badPub: [][]byte{
				publicPEM(t, &p384.PublicKey),
				certificatePEM(t, p521),
				certificatePEM(t, rsaPriv),
			},
		},
		{
			algorithm: PS256,
			private:   privatePEM(rsaPriv),
			public:    publicPEM(t, &rsaPriv.PublicKey),
			badPriv: [][]byte{
				privatePEM(weakRSA),
				pkcs8PEM(t, ec),
			},
			badPub: [][]byte{
				publicPEM(t, &weakRSA.PublicKey),
				certificatePEM(t, ec),
			},
		},
	} {
		t.Run(string(tt.algorithm), func(t *testing.T) {
			for i, key := range append(tt.badPriv, nil, []byte("invalid"), tt.public) {
				if _, err := NewSigner(SignConfig{
					Algorithm:     tt.algorithm,
					PrivateKeyPEM: key,
				}); err == nil {
					t.Errorf("接受无效私钥 #%d", i)
				}
			}
			for i, key := range append(tt.badPub, nil, []byte("invalid"), tt.private) {
				if _, err := NewVerifier(VerifyConfig{
					Algorithm:    tt.algorithm,
					PublicKeyPEM: key,
				}); err == nil {
					t.Errorf("接受无效公钥 #%d", i)
				}
			}
			if _, err := NewSigner(SignConfig{
				Algorithm:     tt.algorithm,
				PrivateKeyPEM: tt.private,
				Secret:        testSecret(),
			}); err == nil {
				t.Error("签发器接受同时配置 Secret 和 PEM")
			}
			if _, err := NewVerifier(VerifyConfig{
				Algorithm:    tt.algorithm,
				PublicKeyPEM: tt.public,
				Secret:       testSecret(),
			}); err == nil {
				t.Error("验签器接受同时配置 Secret 和 PEM")
			}
		})
	}
}

func TestES256RejectWrongSignatures(t *testing.T) {
	key := ecKey(t, elliptic.P256())
	pub := publicPEM(t, &key.PublicKey)
	v, err := NewVerifier(VerifyConfig{
		Algorithm:    ES256,
		PublicKeyPEM: pub,
	})
	if err != nil {
		t.Fatal(err)
	}
	claims := validClaims()
	valid := nativeToken(t, jwt.SigningMethodES256, claims, key)
	parts := strings.Split(valid, ".")
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	der, err := asn1.Marshal(struct {
		R *big.Int
		S *big.Int
	}{
		R: new(big.Int).SetBytes(sig[:32]),
		S: new(big.Int).SetBytes(sig[32:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	bad := []string{
		nativeToken(t, jwt.SigningMethodHS256, claims, pub),
		nativeToken(t, jwt.SigningMethodNone, claims, jwt.UnsafeAllowNoneSignatureType),
		nativeToken(t, jwt.SigningMethodES384, claims, ecKey(t, elliptic.P384())),
		nativeToken(t, jwt.SigningMethodES512, claims, ecKey(t, elliptic.P521())),
		nativeToken(t, jwt.SigningMethodES256, claims, ecKey(t, elliptic.P256())),
	}
	for _, signature := range [][]byte{sig[:63], append(append([]byte(nil), sig...), 0), der} {
		bad = append(bad, parts[0]+"."+parts[1]+"."+base64.RawURLEncoding.EncodeToString(signature))
	}
	parts[1] = base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"attacker","exp":4102444800}`))
	bad = append(bad, strings.Join(parts, "."))
	for i, raw := range bad {
		if err = v.Verify(raw, &jwt.RegisteredClaims{}); !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
			t.Fatalf("错误签名 #%d: %v", i, err)
		}
	}
}

func TestPS256StrictSaltAndAlgorithm(t *testing.T) {
	key := rsaKey(t, 2048)
	pub := publicPEM(t, &key.PublicKey)
	v, err := NewVerifier(VerifyConfig{
		Algorithm:    PS256,
		PublicKeyPEM: pub,
	})
	if err != nil {
		t.Fatal(err)
	}
	claims := validClaims()
	for _, saltLength := range []int{8, 32, 64, rsa.PSSSaltLengthAuto} {
		method := *jwt.SigningMethodPS256
		method.Options = &rsa.PSSOptions{SaltLength: saltLength}
		raw := nativeToken(t, &method, claims, key)
		err = v.Verify(raw, &jwt.RegisteredClaims{})
		if saltLength == 32 {
			if err != nil {
				t.Fatalf("拒绝标准盐长度: %v", err)
			}
		} else if !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
			t.Fatalf("盐长度 %d 的错误 = %v", saltLength, err)
		}
		// jwtx 不应更改上游的全局兼容策略。
		if _, err = jwt.Parse(raw, func(*jwt.Token) (any, error) {
			return &key.PublicKey, nil
		}, jwt.WithValidMethods([]string{"PS256"})); err != nil {
			t.Fatalf("上游兼容验签受到影响: %v", err)
		}
	}
	for _, method := range []jwt.SigningMethod{jwt.SigningMethodRS256, jwt.SigningMethodPS384, jwt.SigningMethodPS512} {
		raw := nativeToken(t, method, claims, key)
		if err = v.Verify(raw, &jwt.RegisteredClaims{}); !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
			t.Fatalf("接受其他 RSA 算法 %s: %v", method.Alg(), err)
		}
		parts := strings.Split(raw, ".")
		parts[0] = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"PS256","typ":"JWT"}`))
		if err = v.Verify(strings.Join(parts, "."), &jwt.RegisteredClaims{}); !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
			t.Fatalf("接受伪造 PS256 header: %v", err)
		}
	}
	for _, raw := range []string{
		nativeToken(t, jwt.SigningMethodHS256, claims, pub),
		nativeToken(t, jwt.SigningMethodNone, claims, jwt.UnsafeAllowNoneSignatureType),
		nativeToken(t, jwt.SigningMethodPS256, claims, rsaKey(t, 2048)),
	} {
		if err = v.Verify(raw, &jwt.RegisteredClaims{}); !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
			t.Fatalf("PS256 错误签名未拒绝: %v", err)
		}
	}
	rs, err := NewVerifier(VerifyConfig{
		Algorithm:    RS256,
		PublicKeyPEM: pub,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = rs.Verify(nativeToken(t, jwt.SigningMethodPS256, claims, key), &jwt.RegisteredClaims{}); !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
		t.Fatalf("RS256 接受 PS256: %v", err)
	}
}
