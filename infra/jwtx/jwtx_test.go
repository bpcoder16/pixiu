package jwtx

import (
	"bytes"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type accessClaims struct {
	UserID     string `json:"uid"`
	Platform   int8   `json:"plt"`
	Portal     string `json:"portal"`
	IsIdentity bool   `json:"isIdentity"`
	jwt.RegisteredClaims
}

func testSecret() []byte {
	return []byte("0123456789abcdef0123456789abcdef")
}

func validClaims() jwt.RegisteredClaims {
	return jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		IssuedAt:  jwt.NewNumericDate(time.Now().Add(-time.Minute)),
	}
}

func nativeToken(t *testing.T, method jwt.SigningMethod, claims jwt.Claims, key any) string {
	t.Helper()
	raw, err := jwt.NewWithClaims(method, claims).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func hsVerifier(t *testing.T, cfg VerifyConfig) *Verifier {
	t.Helper()
	cfg.Algorithm = HS256
	cfg.Secret = testSecret()
	v, err := NewVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func rsaKey(t *testing.T, bits int) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func publicPEM(t *testing.T, key any) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: der,
	})
}

func privatePEM(key *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
}

func TestHS256CustomClaimsInteroperability(t *testing.T) {
	signer, err := NewSigner(SignConfig{
		Algorithm: HS256,
		Secret:    testSecret(),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := accessClaims{
		UserID:           "u_42",
		Platform:         1,
		Portal:           "app",
		IsIdentity:       true,
		RegisteredClaims: validClaims(),
	}
	raw, err := signer.Sign(want)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	data, err := base64.RawURLEncoding.DecodeString(strings.Split(raw, ".")[1])
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload) != 6 || payload["uid"] != want.UserID || payload["portal"] != want.Portal {
		t.Fatalf("claims 布局发生变化: %v", payload)
	}
	var got accessClaims
	if _, err = jwt.ParseWithClaims(raw, &got, func(*jwt.Token) (any, error) {
		return testSecret(), nil
	}, jwt.WithValidMethods([]string{"HS256"})); err != nil {
		t.Fatalf("原生库验签失败: %v", err)
	}
	if got.UserID != want.UserID || !got.IsIdentity || got.Platform != want.Platform {
		t.Fatalf("claims = %+v", got)
	}
	verifier := hsVerifier(t, VerifyConfig{})
	for _, token := range []string{raw, nativeToken(t, jwt.SigningMethodHS256, want, testSecret())} {
		var parsed accessClaims
		if err = verifier.Verify(token, &parsed); err != nil {
			t.Fatal(err)
		}
		if parsed.UserID != want.UserID || parsed.Portal != want.Portal {
			t.Fatalf("claims = %+v", parsed)
		}
	}
}

func TestRS256PassportVerificationAndSigning(t *testing.T) {
	key := rsaKey(t, 2048)
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	claims := jwt.MapClaims{
		"sub":    "42",
		"jti":    "passport-token",
		"aud":    "2",
		"exp":    float64(time.Now().Add(time.Hour).Unix()),
		"iat":    float64(time.Now().Add(-time.Minute).Unix()),
		"nbf":    float64(time.Now().Add(-time.Minute).Unix()),
		"scopes": []string{"read"},
	}
	for _, public := range [][]byte{
		publicPEM(t, &key.PublicKey),
		pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PUBLIC KEY",
			Bytes: x509.MarshalPKCS1PublicKey(&key.PublicKey),
		}),
	} {
		verifier, err := NewVerifier(VerifyConfig{
			Algorithm:     RS256,
			PublicKeyPEM:  public,
			Audience:      []string{"2"},
			CheckIssuedAt: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		var got jwt.RegisteredClaims
		if err = verifier.Verify(nativeToken(t, jwt.SigningMethodRS256, claims, key), &got); err != nil {
			t.Fatal(err)
		}
		if got.Subject != "42" || got.ID != "passport-token" {
			t.Fatalf("Passport 身份不匹配: %+v", got)
		}
		for _, private := range [][]byte{
			privatePEM(key),
			pem.EncodeToMemory(&pem.Block{
				Type:  "PRIVATE KEY",
				Bytes: pkcs8,
			}),
		} {
			signer, err := NewSigner(SignConfig{
				Algorithm:     RS256,
				PrivateKeyPEM: private,
			})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := signer.Sign(claims)
			if err != nil {
				t.Fatal(err)
			}
			if err = verifier.Verify(raw, &jwt.RegisteredClaims{}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestRejectInvalidConfiguration(t *testing.T) {
	weakRSA := rsaKey(t, 1024)
	for _, cfg := range []SignConfig{
		{},
		{
			Algorithm: "HS512",
			Secret:    testSecret(),
		},
		{Algorithm: HS256},
		{
			Algorithm: HS256,
			Secret:    bytes.Repeat([]byte{'s'}, 31),
		},
		{
			Algorithm:     HS256,
			Secret:        testSecret(),
			PrivateKeyPEM: []byte("unused"),
		},
		{Algorithm: RS256},
		{
			Algorithm:     RS256,
			PrivateKeyPEM: []byte("invalid"),
		},
		{
			Algorithm:     RS256,
			PrivateKeyPEM: privatePEM(weakRSA),
		},
		{
			Algorithm:     RS256,
			PrivateKeyPEM: publicPEM(t, &weakRSA.PublicKey),
		},
		{
			Algorithm: RS256,
			Secret:    testSecret(),
		},
	} {
		if _, err := NewSigner(cfg); err == nil {
			t.Errorf("签发器接受无效配置，算法=%q", cfg.Algorithm)
		}
	}
	for _, cfg := range []VerifyConfig{
		{},
		{
			Algorithm: "none",
			Secret:    testSecret(),
		},
		{Algorithm: HS256},
		{
			Algorithm: HS256,
			Secret:    bytes.Repeat([]byte{'s'}, 31),
		},
		{
			Algorithm:    HS256,
			Secret:       testSecret(),
			PublicKeyPEM: []byte("unused"),
		},
		{
			Algorithm: HS256,
			Secret:    testSecret(),
			Leeway:    -time.Second,
		},
		{
			Algorithm: HS256,
			Secret:    testSecret(),
			Issuer:    " \t",
		},
		{
			Algorithm: HS256,
			Secret:    testSecret(),
			Audience:  []string{"app", ""},
		},
		{
			Algorithm: HS256,
			Secret:    testSecret(),
			Audience:  []string{" "},
		},
		{Algorithm: RS256},
		{
			Algorithm:    RS256,
			PublicKeyPEM: []byte("invalid"),
		},
		{
			Algorithm:    RS256,
			PublicKeyPEM: publicPEM(t, &weakRSA.PublicKey),
		},
		{
			Algorithm:    RS256,
			PublicKeyPEM: privatePEM(weakRSA),
		},
		{
			Algorithm: RS256,
			Secret:    testSecret(),
		},
	} {
		if _, err := NewVerifier(cfg); err == nil {
			t.Errorf("验签器接受无效配置，算法=%q", cfg.Algorithm)
		}
	}
}

func TestConfigurationSlicesAreCopied(t *testing.T) {
	secret := testSecret()
	audiences := []string{"app"}
	signer, err := NewSigner(SignConfig{
		Algorithm: HS256,
		Secret:    secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewVerifier(VerifyConfig{
		Algorithm: HS256,
		Secret:    secret,
		Audience:  audiences,
	})
	if err != nil {
		t.Fatal(err)
	}
	secret[0] ^= 0xff
	audiences[0] = "admin"
	claims := validClaims()
	claims.Audience = jwt.ClaimStrings{"app"}
	raw, err := signer.Sign(claims)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = jwt.Parse(raw, func(*jwt.Token) (any, error) {
		return testSecret(), nil
	}); err != nil {
		t.Fatalf("修改配置影响签发密钥: %v", err)
	}
	if err = verifier.Verify(raw, &jwt.RegisteredClaims{}); err != nil {
		t.Fatalf("修改配置影响验签策略: %v", err)
	}
}

func TestRejectWrongAlgorithmsKeysAndMalformedTokens(t *testing.T) {
	claims := validClaims()
	valid := nativeToken(t, jwt.SigningMethodHS256, claims, testSecret())
	parts := strings.Split(valid, ".")
	parts[1] = base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"attacker","exp":4102444800}`))
	verifier := hsVerifier(t, VerifyConfig{})
	for _, raw := range []string{
		nativeToken(t, jwt.SigningMethodHS384, claims, testSecret()),
		nativeToken(t, jwt.SigningMethodHS512, claims, testSecret()),
		nativeToken(t, jwt.SigningMethodNone, claims, jwt.UnsafeAllowNoneSignatureType),
		nativeToken(t, jwt.SigningMethodHS256, claims, bytes.Repeat([]byte{'x'}, 32)),
		strings.Join(parts, "."),
	} {
		if err := verifier.Verify(raw, &jwt.RegisteredClaims{}); !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
			t.Fatalf("验签错误 = %v，期望签名错误", err)
		}
	}
	for _, raw := range []string{"", "broken", "a.b.c.d", "Bearer " + valid} {
		if err := verifier.Verify(raw, &jwt.RegisteredClaims{}); !errors.Is(err, jwt.ErrTokenMalformed) {
			t.Fatalf("格式错误 = %v", err)
		}
	}
	key := rsaKey(t, 2048)
	pub := publicPEM(t, &key.PublicKey)
	rsaVerifier, err := NewVerifier(VerifyConfig{
		Algorithm:    RS256,
		PublicKeyPEM: pub,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		nativeToken(t, jwt.SigningMethodHS256, claims, pub),
		nativeToken(t, jwt.SigningMethodRS384, claims, key),
		nativeToken(t, jwt.SigningMethodRS256, claims, rsaKey(t, 2048)),
	} {
		if err = rsaVerifier.Verify(raw, &jwt.RegisteredClaims{}); !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
			t.Fatalf("RSA 验签错误 = %v", err)
		}
	}
}

func TestTimeAndExpectedClaimsPolicy(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name   string
		cfg    VerifyConfig
		change func(*jwt.RegisteredClaims)
		want   error
	}{
		{name: "允许缺少业务身份字段"},
		{
			name:   "缺少过期时间",
			change: func(c *jwt.RegisteredClaims) { c.ExpiresAt = nil },
			want:   jwt.ErrTokenRequiredClaimMissing,
		},
		{
			name:   "已过期",
			change: func(c *jwt.RegisteredClaims) { c.ExpiresAt = jwt.NewNumericDate(now.Add(-time.Minute)) },
			want:   jwt.ErrTokenExpired,
		},
		{
			name:   "到达过期边界",
			change: func(c *jwt.RegisteredClaims) { c.ExpiresAt = jwt.NewNumericDate(now) },
			want:   jwt.ErrTokenExpired,
		},
		{
			name:   "允许过期偏差",
			cfg:    VerifyConfig{Leeway: time.Minute},
			change: func(c *jwt.RegisteredClaims) { c.ExpiresAt = jwt.NewNumericDate(now.Add(-10 * time.Second)) },
		},
		{
			name:   "超过过期偏差",
			cfg:    VerifyConfig{Leeway: time.Minute},
			change: func(c *jwt.RegisteredClaims) { c.ExpiresAt = jwt.NewNumericDate(now.Add(-2 * time.Minute)) },
			want:   jwt.ErrTokenExpired,
		},
		{
			name:   "尚未生效",
			change: func(c *jwt.RegisteredClaims) { c.NotBefore = jwt.NewNumericDate(now.Add(time.Minute)) },
			want:   jwt.ErrTokenNotValidYet,
		},
		{
			name:   "允许生效偏差",
			cfg:    VerifyConfig{Leeway: time.Minute},
			change: func(c *jwt.RegisteredClaims) { c.NotBefore = jwt.NewNumericDate(now.Add(10 * time.Second)) },
		},
		{
			name:   "默认不检查签发时间",
			change: func(c *jwt.RegisteredClaims) { c.IssuedAt = jwt.NewNumericDate(now.Add(time.Minute)) },
		},
		{
			name:   "检查未来签发时间",
			cfg:    VerifyConfig{CheckIssuedAt: true},
			change: func(c *jwt.RegisteredClaims) { c.IssuedAt = jwt.NewNumericDate(now.Add(time.Minute)) },
			want:   jwt.ErrTokenUsedBeforeIssued,
		},
		{
			name:   "签发时间可缺省",
			cfg:    VerifyConfig{CheckIssuedAt: true},
			change: func(c *jwt.RegisteredClaims) { c.IssuedAt = nil },
		},
		{
			name: "缺少issuer",
			cfg:  VerifyConfig{Issuer: "issuer"},
			want: jwt.ErrTokenRequiredClaimMissing,
		},
		{
			name:   "错误issuer",
			cfg:    VerifyConfig{Issuer: "issuer"},
			change: func(c *jwt.RegisteredClaims) { c.Issuer = "other" },
			want:   jwt.ErrTokenInvalidIssuer,
		},
		{
			name:   "匹配issuer",
			cfg:    VerifyConfig{Issuer: "issuer"},
			change: func(c *jwt.RegisteredClaims) { c.Issuer = "issuer" },
		},
		{
			name: "缺少audience",
			cfg:  VerifyConfig{Audience: []string{"app"}},
			want: jwt.ErrTokenRequiredClaimMissing,
		},
		{
			name:   "错误audience",
			cfg:    VerifyConfig{Audience: []string{"app"}},
			change: func(c *jwt.RegisteredClaims) { c.Audience = jwt.ClaimStrings{"admin"} },
			want:   jwt.ErrTokenInvalidAudience,
		},
		{
			name:   "匹配任一audience",
			cfg:    VerifyConfig{Audience: []string{"app", "admin"}},
			change: func(c *jwt.RegisteredClaims) { c.Audience = jwt.ClaimStrings{"other", "admin"} },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claims := validClaims()
			if tt.change != nil {
				tt.change(&claims)
			}
			err := hsVerifier(t, tt.cfg).Verify(nativeToken(t, jwt.SigningMethodHS256, claims, testSecret()), &jwt.RegisteredClaims{})
			if !errors.Is(err, tt.want) {
				t.Fatalf("Verify() = %v，期望 %v", err, tt.want)
			}
		})
	}
}

var errWrongPortal = errors.New("不允许此入口")

type restrictedClaims struct {
	accessClaims
}

func (c restrictedClaims) Validate() error {
	if c.Portal != "app" {
		return errWrongPortal
	}
	return nil
}

func TestCustomValidationAndMapClaims(t *testing.T) {
	claims := accessClaims{
		Portal:           "admin",
		RegisteredClaims: validClaims(),
	}
	raw := nativeToken(t, jwt.SigningMethodHS256, claims, testSecret())
	verifier := hsVerifier(t, VerifyConfig{})
	if err := verifier.Verify(raw, &restrictedClaims{}); !errors.Is(err, errWrongPortal) || !errors.Is(err, jwt.ErrTokenInvalidClaims) {
		t.Fatalf("自定义校验错误丢失: %v", err)
	}
	parsed := jwt.MapClaims{}
	if err := verifier.Verify(raw, parsed); err != nil {
		t.Fatal(err)
	}
	if parsed["portal"] != "admin" {
		t.Fatalf("MapClaims = %v", parsed)
	}
}

func TestSignRequiresExpiryAndVerifyRequiresDestination(t *testing.T) {
	signer, err := NewSigner(SignConfig{
		Algorithm: HS256,
		Secret:    testSecret(),
	})
	if err != nil {
		t.Fatal(err)
	}
	verifier := hsVerifier(t, VerifyConfig{})
	raw := nativeToken(t, jwt.SigningMethodHS256, validClaims(), testSecret())
	for _, claims := range []jwt.Claims{nil, (*accessClaims)(nil), jwt.MapClaims(nil)} {
		if _, err = signer.Sign(claims); !errors.Is(err, jwt.ErrTokenInvalidClaims) {
			t.Fatalf("Sign(nil) = %v", err)
		}
		if err = verifier.Verify(raw, claims); !errors.Is(err, jwt.ErrTokenInvalidClaims) {
			t.Fatalf("Verify(nil) = %v", err)
		}
	}
	if err = verifier.Verify(raw, jwt.RegisteredClaims{}); !errors.Is(err, jwt.ErrTokenInvalidClaims) {
		t.Fatalf("Verify(不可写目标) = %v", err)
	}
	if _, err = signer.Sign(jwt.RegisteredClaims{}); !errors.Is(err, jwt.ErrTokenRequiredClaimMissing) {
		t.Fatalf("Sign(无exp) = %v", err)
	}
	if _, err = signer.Sign(jwt.MapClaims{"exp": "invalid"}); !errors.Is(err, jwt.ErrInvalidType) {
		t.Fatalf("Sign(错误exp类型) = %v", err)
	}
	// 签发只检查 exp 存在，不改写时间，也不代替验签阶段的业务校验。
	future := restrictedClaims{accessClaims{
		Portal:           "admin",
		RegisteredClaims: validClaims(),
	}}
	future.NotBefore = jwt.NewNumericDate(time.Now().Add(30 * time.Minute))
	if _, err = signer.Sign(future); err != nil {
		t.Fatalf("预先签发失败: %v", err)
	}
}

func TestConcurrentSigningAndVerification(t *testing.T) {
	key := rsaKey(t, 2048)
	ec := ecKey(t, elliptic.P256())
	for _, algorithm := range []Algorithm{HS256, RS256, ES256, PS256} {
		t.Run(string(algorithm), func(t *testing.T) {
			signCfg := SignConfig{Algorithm: algorithm}
			verifyCfg := VerifyConfig{Algorithm: algorithm}
			if algorithm == HS256 {
				signCfg.Secret = testSecret()
				verifyCfg.Secret = testSecret()
			} else if algorithm == ES256 {
				signCfg.PrivateKeyPEM = ecPrivatePEM(t, ec)
				verifyCfg.PublicKeyPEM = publicPEM(t, &ec.PublicKey)
			} else {
				signCfg.PrivateKeyPEM = privatePEM(key)
				verifyCfg.PublicKeyPEM = publicPEM(t, &key.PublicKey)
			}
			signer, err := NewSigner(signCfg)
			if err != nil {
				t.Fatal(err)
			}
			verifier, err := NewVerifier(verifyCfg)
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for i := 0; i < 12; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 5; j++ {
						raw, err := signer.Sign(validClaims())
						if err != nil {
							t.Error(err)
							return
						}
						if err = verifier.Verify(raw, &jwt.RegisteredClaims{}); err != nil {
							t.Error(err)
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}
