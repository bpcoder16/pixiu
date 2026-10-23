package cryptox

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestHMACSHA256(t *testing.T) {
	key := bytes.Repeat([]byte{0xaa}, 131)
	s, err := NewHMACSHA256(key)
	if err != nil {
		t.Fatal(err)
	}
	message := []byte("Test Using Larger Than Block-Size Key - Hash Key First")
	signature, err := s.Sign(message)
	// RFC 4231 第 6 个测试向量也验证超过 SHA-256 block 长度的密钥。
	if err != nil || hex.EncodeToString(signature) != "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54" {
		t.Fatalf("RFC 4231: %x, %v", signature, err)
	}
	key[0] ^= 1
	if !s.Verify(message, signature) {
		t.Fatal("修改构造输入影响了实例")
	}
	saved := bytes.Clone(signature)
	for range 16 {
		_, _ = s.Sign([]byte("other message"))
	}
	if !bytes.Equal(signature, saved) {
		t.Fatal("后续调用修改了先前返回的认证码")
	}
	for _, n := range []int{0, 1, 31, 33, 64} {
		if s.Verify(message, make([]byte, n)) {
			t.Fatalf("接受了长度 %d 的签名", n)
		}
	}
	for i := range signature {
		bad := bytes.Clone(signature)
		bad[i] ^= 1
		if s.Verify(message, bad) {
			t.Fatalf("接受篡改签名 %d", i)
		}
	}
	if s.Verify(append(bytes.Clone(message), '!'), signature) {
		t.Fatal("接受了被修改的消息")
	}
	for _, message := range [][]byte{nil, []byte("中文参数"), bytes.Repeat([]byte{1}, 4096)} {
		mac := hmac.New(sha256.New, key)
		mac.Write(message)
		other, _ := NewHMACSHA256(key)
		got, err := other.Sign(message)
		if err != nil || !bytes.Equal(got, mac.Sum(nil)) || !other.Verify(message, got) {
			t.Fatalf("与标准库不一致: %v", err)
		}
	}
}

func TestAESGCMVectorsAndInterop(t *testing.T) {
	zero, _ := NewAESGCM(make([]byte, 16))
	// NIST GCM: 全零 128 位 key、96 位 nonce、128 位 plaintext。
	vector, _ := hex.DecodeString("0000000000000000000000000388dace60b6a392f328c2b971b2fe78ab6e47d42cec13bdf53a67b21257bddf")
	got, err := zero.Decrypt(vector, nil)
	if err != nil || !bytes.Equal(got, make([]byte, 16)) {
		t.Fatalf("NIST: %x, %v", got, err)
	}
	for _, keyLen := range []int{16, 24, 32} {
		key := bytes.Repeat([]byte{3}, keyLen)
		c, err := NewAESGCM(key)
		if err != nil {
			t.Fatal(err)
		}
		block, _ := aes.NewCipher(key)
		standard, _ := cipher.NewGCMWithRandomNonce(block)
		key[0] ^= 1
		for _, plaintext := range [][]byte{nil, []byte("资源 42"), bytes.Repeat([]byte{8}, 4096)} {
			aad := []byte("scope")
			saved := bytes.Clone(plaintext)
			encrypted, err := c.Encrypt(plaintext, aad)
			if err != nil || len(encrypted) != len(plaintext)+28 {
				t.Fatalf("Encrypt: len=%d, %v", len(encrypted), err)
			}
			got, err := standard.Open(nil, nil, encrypted, aad)
			if err != nil || !bytes.Equal(got, plaintext) {
				t.Fatalf("标准库解密失败: %v", err)
			}
			got, err = c.Decrypt(standard.Seal(nil, nil, plaintext, aad), aad)
			if err != nil || !bytes.Equal(got, plaintext) || !bytes.Equal(plaintext, saved) {
				t.Fatalf("互通/输入持有约定失败: %v", err)
			}
			other, _ := c.Encrypt(plaintext, aad)
			if bytes.Equal(encrypted[:12], other[:12]) {
				t.Fatal("相邻加密复用了 nonce")
			}
		}
	}
}

func TestAESRejectsTampering(t *testing.T) {
	c, _ := NewAESGCM(make([]byte, 32))
	aad := []byte("share")
	encrypted, _ := c.Encrypt([]byte("resource=42"), aad)
	saved := bytes.Clone(encrypted)
	for i := range encrypted {
		bad := bytes.Clone(encrypted)
		bad[i] ^= 1
		got, err := c.Decrypt(bad, aad)
		if !errors.Is(err, ErrDecrypt) || got != nil {
			t.Fatalf("篡改 %d: %x, %v", i, got, err)
		}
		got, err = c.Decrypt(encrypted[:i], aad)
		if !errors.Is(err, ErrDecrypt) || got != nil {
			t.Fatalf("截断 %d: %x, %v", i, got, err)
		}
	}
	if got, err := c.Decrypt(encrypted, []byte("other")); got != nil || !errors.Is(err, ErrDecrypt) {
		t.Fatalf("错误 AAD: %x, %v", got, err)
	}
	other, _ := NewAESGCM(bytes.Repeat([]byte{1}, 32))
	if got, err := other.Decrypt(encrypted, aad); got != nil || !errors.Is(err, ErrDecrypt) {
		t.Fatalf("错误密钥: %x, %v", got, err)
	}
	if !bytes.Equal(encrypted, saved) {
		t.Fatal("解密修改了调用方密文")
	}
}

func TestTokenFormatAndBoundaries(t *testing.T) {
	c, _ := NewAESGCM(make([]byte, 32))
	const purpose = "share-download"
	// 覆盖 Base64 的三种尾段长度，以及最大可编码 payload 的临界长度。
	maxPayload := (MaxTokenLen-3)*3/4 - 28
	for _, n := range []int{0, 1, 2, 64, 4096, maxPayload - 1, maxPayload} {
		plaintext := bytes.Repeat([]byte{9}, n)
		token, err := c.EncryptToken(plaintext, purpose)
		if err != nil || !strings.HasPrefix(token, "v1.") || len(token) > MaxTokenLen {
			t.Fatalf("EncryptToken(%d): len=%d, %v", n, len(token), err)
		}
		got, err := c.DecryptToken(token, purpose)
		if err != nil || !bytes.Equal(got, plaintext) {
			t.Fatalf("DecryptToken(%d): %v", n, err)
		}
		raw, err := base64.RawURLEncoding.DecodeString(token[3:])
		if err != nil {
			t.Fatal(err)
		}
		got, err = c.Decrypt(raw, []byte("cryptox:token:v1\x00"+purpose))
		if err != nil || !bytes.Equal(got, plaintext) {
			t.Fatalf("线格式不匹配: %v", err)
		}
		if got, err := c.DecryptToken(token, "other"); got != nil || !errors.Is(err, ErrDecrypt) {
			t.Fatalf("用途未绑定: %x, %v", got, err)
		}
		if got, err := c.Decrypt(raw, nil); got != nil || !errors.Is(err, ErrDecrypt) {
			t.Fatalf("token 未绑定 AAD: %x, %v", got, err)
		}
	}
	if got, err := c.EncryptToken(make([]byte, maxPayload+1), purpose); got != "" || !errors.Is(err, ErrTooLarge) {
		t.Fatalf("超长 plaintext: %q, %v", got, err)
	}
	if got, err := c.DecryptToken(strings.Repeat("A", MaxTokenLen+1), purpose); got != nil || !errors.Is(err, ErrTooLarge) {
		t.Fatalf("超长 token: %x, %v", got, err)
	}
	for _, purpose := range []string{"", strings.Repeat("a", 257)} {
		if _, err := c.EncryptToken(nil, purpose); !errors.Is(err, ErrInvalidPurpose) {
			t.Fatalf("无效用途: %v", err)
		}
		if got, err := c.DecryptToken("v1.AAAA", purpose); got != nil || !errors.Is(err, ErrInvalidPurpose) {
			t.Fatalf("无效用途解密: %x, %v", got, err)
		}
	}
	if _, err := c.EncryptToken(nil, strings.Repeat("a", 256)); err != nil {
		t.Fatal(err)
	}
}

func TestTokenRejectsNoncanonicalEncoding(t *testing.T) {
	c, _ := NewAESGCM(make([]byte, 32))
	token, _ := c.EncryptToken(nil, "share")
	badTokens := []string{"", "v1.", "v2." + token[3:], token + "=", token + ".", token + "\n", token[:5] + "\r\n" + token[5:], token + " ", "v1.++++", "v1.____", "v1.中文"}
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	// 空明文和单字节明文分别产生 4 位、2 位未使用尾部位，均必须为零。
	for n := range 2 {
		encoded, err := c.EncryptToken(make([]byte, n), "share")
		if err != nil {
			t.Fatal(err)
		}
		last := strings.IndexByte(alphabet, encoded[len(encoded)-1])
		for bits := 1; bits < 1<<(4-2*n); bits++ {
			badTokens = append(badTokens, encoded[:len(encoded)-1]+string(alphabet[last|bits]))
		}
	}
	// 将字母表外的每种字节插入有效密文，覆盖开头、中间和末尾，尤其是解码器会忽略的 CR/LF。
	for ch := range 256 {
		if strings.IndexByte(alphabet, byte(ch)) >= 0 {
			continue
		}
		for _, pos := range []int{3, len(token) / 2, len(token)} {
			badTokens = append(badTokens, token[:pos]+string([]byte{byte(ch)})+token[pos:])
		}
	}
	for _, raw := range badTokens {
		if got, err := c.DecryptToken(raw, "share"); got != nil || !errors.Is(err, ErrDecrypt) {
			t.Fatalf("接受了非规范 token %q: %x, %v", raw, got, err)
		}
	}
}

func TestInitializationAndKeyLengths(t *testing.T) {
	for _, n := range []int{0, 1, 15, 17, 23, 25, 31, 33} {
		if _, err := NewAESGCM(make([]byte, n)); err == nil {
			t.Fatalf("接受 AES key 长度 %d", n)
		}
	}
	for _, n := range []int{0, 1, 16, 31} {
		if _, err := NewHMACSHA256(make([]byte, n)); err == nil {
			t.Fatalf("接受 HMAC key 长度 %d", n)
		}
	}
	for _, c := range []*AESGCM{nil, {}} {
		if got, err := c.Encrypt(nil, nil); got != nil || !errors.Is(err, ErrUninitialized) {
			t.Fatalf("未初始化 Encrypt: %x, %v", got, err)
		}
		if got, err := c.Decrypt(nil, nil); got != nil || !errors.Is(err, ErrUninitialized) {
			t.Fatalf("未初始化 Decrypt: %x, %v", got, err)
		}
		if got, err := c.EncryptToken(nil, "share"); got != "" || !errors.Is(err, ErrUninitialized) {
			t.Fatalf("未初始化 EncryptToken: %q, %v", got, err)
		}
		if got, err := c.DecryptToken("v1.AAAA", "share"); got != nil || !errors.Is(err, ErrUninitialized) {
			t.Fatalf("未初始化 DecryptToken: %x, %v", got, err)
		}
	}
	for _, s := range []*HMACSHA256{nil, {}} {
		if got, err := s.Sign(nil); got != nil || !errors.Is(err, ErrUninitialized) || s.Verify(nil, make([]byte, 32)) {
			t.Fatalf("未初始化 HMAC: %x, %v", got, err)
		}
	}
}

func TestRandomSizes(t *testing.T) {
	for _, n := range []int{-1, 0, MaxRandomSize + 1, int(^uint(0) >> 1)} {
		if got, err := RandomBytes(n); got != nil || !errors.Is(err, ErrInvalidSize) {
			t.Fatalf("RandomBytes(%d): %v", n, err)
		}
	}
	for _, n := range []int{-1, 0, 15, MaxTokenLen*3/4 + 1, int(^uint(0) >> 1)} {
		if got, err := RandomToken(n); got != "" || !errors.Is(err, ErrInvalidSize) {
			t.Fatalf("RandomToken(%d): %v", n, err)
		}
	}
	for _, n := range []int{1, 16, 24, 32, MaxRandomSize} {
		got, err := RandomBytes(n)
		if err != nil || len(got) != n {
			t.Fatalf("RandomBytes(%d): %v", n, err)
		}
	}
	for _, n := range []int{16, 32, MaxTokenLen * 3 / 4} {
		got, err := RandomToken(n)
		raw, decodeErr := base64.RawURLEncoding.Strict().DecodeString(got)
		if err != nil || decodeErr != nil || len(raw) != n || len(got) > MaxTokenLen {
			t.Fatalf("RandomToken(%d): %v, %v", n, err, decodeErr)
		}
		other, _ := RandomToken(n)
		if other == got {
			t.Fatal("随机 token 重复")
		}
	}
}

func TestConcurrentReuse(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	c, _ := NewAESGCM(key)
	s, _ := NewHMACSHA256(key)
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			message := bytes.Repeat([]byte{byte(i)}, 256)
			for range 32 {
				signature, err := s.Sign(message)
				if err != nil || !s.Verify(message, signature) {
					t.Error("并发签名失败")
				}
				token, err := c.EncryptToken(message, "concurrent")
				if err != nil {
					t.Error(err)
					return
				}
				got, err := c.DecryptToken(token, "concurrent")
				if err != nil || !bytes.Equal(got, message) {
					t.Error("并发解密失败")
				}
			}
		})
	}
	wg.Wait()
}
