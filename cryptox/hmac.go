package cryptox

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"hash"
	"sync"
)

// HMACSHA256 使用固定密钥签名和验签，可并发复用；零值不可用。
type HMACSHA256 struct {
	pool *sync.Pool
}

type hmacState struct {
	mac    hash.Hash
	digest [sha256.Size]byte
}

// NewHMACSHA256 复制至少 32 字节的密钥，密钥应由 crypto/rand 生成。
// 持有相同密钥的验签方也具有签名能力。
func NewHMACSHA256(key []byte) (*HMACSHA256, error) {
	if len(key) < sha256.Size {
		return nil, fmt.Errorf("cryptox: HMAC-SHA256 key must contain at least 32 bytes")
	}
	secret := bytes.Clone(key)
	return &HMACSHA256{
		pool: &sync.Pool{
			New: func() any {
				return &hmacState{mac: hmac.New(sha256.New, secret)}
			},
		},
	}, nil
}

// Sign 返回独立持有的完整 32 字节认证码，不改变 message。
func (s *HMACSHA256) Sign(message []byte) ([]byte, error) {
	if s == nil || s.pool == nil {
		return nil, ErrUninitialized
	}
	state := s.sum(message)
	result := bytes.Clone(state.digest[:])
	s.pool.Put(state)
	return result, nil
}

// Verify 拒绝截短的认证码，使用恒定时间比较；未初始化的实例返回 false。
func (s *HMACSHA256) Verify(message, signature []byte) bool {
	if s == nil || s.pool == nil || len(signature) != sha256.Size {
		return false
	}
	state := s.sum(message)
	valid := hmac.Equal(state.digest[:], signature)
	s.pool.Put(state)
	return valid
}

func (s *HMACSHA256) sum(message []byte) *hmacState {
	// 每次调用独占一个状态，避免共享 hash 的数据竞争，也不串行化签名。
	// 固定摘要缓冲区随状态复用；验签结束后才能归还，签名须复制后再归还。
	state := s.pool.Get().(*hmacState)
	state.mac.Reset()
	_, _ = state.mac.Write(message) // 标准库 hash.Write 保证不返回错误。
	state.mac.Sum(state.digest[:0])
	return state
}
