package cryptox

import "crypto/rand"

// MaxRandomSize 是单次 RandomBytes 的最大字节数量，限制无意的大内存分配。
const MaxRandomSize = 1 << 20

// RandomBytes 返回 1–MaxRandomSize 个密码学安全随机字节。
// AES-256/HMAC 密钥通常选择 32 字节；实际密钥应安全持久化并按用途隔离。
func RandomBytes(size int) ([]byte, error) {
	if size < 1 || size > MaxRandomSize {
		return nil, ErrInvalidSize
	}
	result := make([]byte, size)
	if _, err := rand.Read(result); err != nil {
		return nil, err
	}
	return result, nil
}

// RandomToken 将 16–6144 个随机字节编码为无填充 Base64URL；常规推荐 size=32。
// 它不携带可解密内容；撤销、有效期和一次性消费需配合服务端存储。
func RandomToken(size int) (string, error) {
	if size < 16 || size > MaxTokenLen*3/4 {
		return "", ErrInvalidSize
	}
	data, err := RandomBytes(size)
	if err != nil {
		return "", err
	}
	return tokenEncoding.EncodeToString(data), nil
}
