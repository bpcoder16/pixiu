package aliyun

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/bpcoder16/pixiu/logit"
)

// TeaLogResult 包含 ES 和 STS 写结果日志时使用的公共字段和级别。
type TeaLogResult struct {
	Status    int
	RequestID string
	ErrorCode string
	Level     logit.Level
}

// ClassifyTeaResult 优先使用实际 HTTP 状态，仅从 Tea 错误中提取可记录的字段。
func ClassifyTeaResult(status int, requestID string, err error, duration, slowThreshold time.Duration) TeaLogResult {
	result := TeaLogResult{
		Status:    status,
		RequestID: requestID,
		Level:     logit.InfoLevel,
	}
	if err == nil {
		if duration > slowThreshold {
			result.Level = logit.WarnLevel
		}
		return result
	}
	if sdkErr, ok := errors.AsType[*tea.SDKError](err); ok {
		if result.Status == 0 {
			result.Status = tea.IntValue(sdkErr.StatusCode)
		}
		result.ErrorCode = tea.StringValue(sdkErr.Code)
		// Data 可含敏感信息；沿用 SDK 的 RequestId，解析失败时保留响应提供的 ID。
		var data struct {
			RequestID string `json:"RequestId"`
		}
		if json.Unmarshal([]byte(tea.StringValue(sdkErr.Data)), &data) == nil {
			result.RequestID = data.RequestID
		}
	}
	if result.ErrorCode == "" {
		result.ErrorCode = "client_error"
	}
	result.Level = logit.ErrorLevel
	if result.Status >= 400 && result.Status < 500 {
		result.Level = logit.WarnLevel
	}
	return result
}
