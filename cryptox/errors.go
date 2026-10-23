package cryptox

import "errors"

var (
	// ErrUninitialized 表示实例未经构造函数初始化。
	ErrUninitialized = errors.New("cryptox: uninitialized instance")
	// ErrDecrypt 统一表示密文格式或认证失败，不包含不可信输入。
	ErrDecrypt = errors.New("cryptox: decryption failed")
	// ErrTooLarge 表示输入超过对应接口的长度限制。
	ErrTooLarge = errors.New("cryptox: input too large")
	// ErrInvalidPurpose 表示 token 用途不满足 1–256 字节限制。
	ErrInvalidPurpose = errors.New("cryptox: purpose must contain 1 to 256 bytes")
	// ErrInvalidSize 表示请求的随机字节数量不在允许范围内。
	ErrInvalidSize = errors.New("cryptox: invalid random size")
)
