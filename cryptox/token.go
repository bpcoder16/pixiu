package cryptox

import (
	"encoding/base64"
	"strings"
)

// MaxTokenLen 是 token 便捷接口允许的最大编码字节长度，不包含 URL 参数名。
const MaxTokenLen = 8192

const (
	tokenPrefix    = "v1."
	tokenAADPrefix = "cryptox:token:v1\x00"
)

var tokenEncoding = base64.RawURLEncoding.Strict()

// EncryptToken 生成 v1. 前缀的无填充 Base64URL 密文；purpose 为 1–256 字节的用途。
// 版本与 purpose 参与认证。结果最多 MaxTokenLen 字节；payload 有效期由应用管理。
func (c *AESGCM) EncryptToken(plaintext []byte, purpose string) (string, error) {
	if c == nil || c.aead == nil {
		return "", ErrUninitialized
	}
	if len(purpose) == 0 || len(purpose) > 256 {
		return "", ErrInvalidPurpose
	}
	// 先限制输入再计算编码大小，避免 int 溢出和不必要的大分配。
	if len(plaintext) > (MaxTokenLen-len(tokenPrefix))*3/4-c.aead.Overhead() {
		return "", ErrTooLarge
	}
	encrypted, err := c.Encrypt(plaintext, []byte(tokenAADPrefix+purpose))
	if err != nil {
		return "", err
	}
	encoded := make([]byte, len(tokenPrefix)+tokenEncoding.EncodedLen(len(encrypted)))
	copy(encoded, tokenPrefix)
	tokenEncoding.Encode(encoded[len(tokenPrefix):], encrypted)
	return string(encoded), nil
}

// DecryptToken 只接受规范编码和固定 v1 格式；失败不返回明文。
// 只有返回 nil 错误后才能解析 payload 并检查有效期、资源和权限。
func (c *AESGCM) DecryptToken(token, purpose string) ([]byte, error) {
	if c == nil || c.aead == nil {
		return nil, ErrUninitialized
	}
	if len(purpose) == 0 || len(purpose) > 256 {
		return nil, ErrInvalidPurpose
	}
	if len(token) > MaxTokenLen {
		return nil, ErrTooLarge
	}
	if !strings.HasPrefix(token, tokenPrefix) {
		return nil, ErrDecrypt
	}
	encoded := token[len(tokenPrefix):]
	if len(encoded) < tokenEncoding.EncodedLen(c.aead.Overhead()) {
		return nil, ErrDecrypt
	}
	// Strict 仍会忽略 CR/LF，须单独拒绝；其他非法字符和非零尾部位交给解码器检查。
	if strings.ContainsAny(encoded, "\r\n") {
		return nil, ErrDecrypt
	}
	encrypted, err := tokenEncoding.DecodeString(encoded)
	if err != nil {
		return nil, ErrDecrypt
	}
	return c.Decrypt(encrypted, []byte(tokenAADPrefix+purpose))
}
