package common

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"github.com/bpcoder16/pixiu/infra/env"
)

// GenerateIDWithYYMMDDHHmmssSSS 返回原样前缀 + 应用时区的两位年月日时分秒 + 三位毫秒 + 四位随机数字（0000–9999）。
// 调用前须初始化 Pixiu env；结果可能重复，落库时须通过唯一索引和冲突重试保证唯一。
func GenerateIDWithYYMMDDHHmmssSSS(prefix string) (string, error) {
	number, err := rand.Int(rand.Reader, big.NewInt(10000))
	if err != nil {
		return "", fmt.Errorf("generate ID random suffix: %w", err)
	}
	now := time.Now().In(env.TimeLocation())
	timestamp := now.Format("060102150405")
	millisecond := now.Nanosecond() / int(time.Millisecond)
	return fmt.Sprintf("%s%s%03d%04d", prefix, timestamp, millisecond, number.Int64()), nil
}
