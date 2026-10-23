package cryptox

import (
	"crypto/aes"
	"crypto/cipher"
)

// GCM 每条消息的限制，与标准库 Seal 的检查一致；用 uint64 避免 32 位平台溢出。
const maxGCMPlaintext uint64 = ((1 << 32) - 2) * aes.BlockSize

// AESGCM 使用标准库 AES-GCM 和随机 nonce，可并发复用；零值不可用。
type AESGCM struct {
	aead cipher.AEAD
}

// NewAESGCM 接受 16/24/32 字节随机密钥，通常推荐 32 字节。
// 同一密钥跨全部实例累计加密不得超过 2^32 次；轮换由应用负责。
func NewAESGCM(key []byte) (*AESGCM, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		return nil, err
	}
	return &AESGCM{aead: aead}, nil
}

// Encrypt 返回 nonce(12) || ciphertext || tag(16)，不修改输入。
// aad 被认证但不被加密或保存；解密时须提供相同字节。允许空 plaintext 和 aad。
func (c *AESGCM) Encrypt(plaintext, aad []byte) ([]byte, error) {
	if c == nil || c.aead == nil {
		return nil, ErrUninitialized
	}
	if uint64(len(plaintext)) > maxGCMPlaintext {
		return nil, ErrTooLarge
	}
	return c.aead.Seal(nil, nil, plaintext, aad), nil
}

// Decrypt 验证认证标签后返回独立明文；格式或认证失败返回 ErrDecrypt 和 nil。
func (c *AESGCM) Decrypt(ciphertext, aad []byte) ([]byte, error) {
	if c == nil || c.aead == nil {
		return nil, ErrUninitialized
	}
	if uint64(len(ciphertext)) > maxGCMPlaintext+uint64(c.aead.Overhead()) {
		return nil, ErrTooLarge
	}
	if len(ciphertext) < c.aead.Overhead() {
		return nil, ErrDecrypt
	}
	plaintext, err := c.aead.Open(nil, nil, ciphertext, aad)
	if err != nil {
		// Open 失败时可能写过目标缓冲区；不向调用方泄露任何部分结果。
		return nil, ErrDecrypt
	}
	return plaintext, nil
}
