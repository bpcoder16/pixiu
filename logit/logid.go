package logit

import "uuid"

// NewLogID 返回 RFC 9562 UUIDv4 的小写 canonical 字符串。
func NewLogID() string {
	return uuid.NewV4().String()
}
