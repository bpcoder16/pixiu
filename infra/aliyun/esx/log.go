package esx

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/bpcoder16/pixiu/logit"
)

const downstreamType = "AliyunES"

func (c *Client) finish(ctx context.Context, operation, requestID string, status int, err error, duration time.Duration) {
	logit.AddDownstreamDurationAuto(ctx, downstreamType+"_"+c.name, duration)
	if !c.logRequests {
		return
	}
	code := ""
	if sdkErr, ok := errors.AsType[*tea.SDKError](err); ok {
		if status == 0 {
			status = tea.IntValue(sdkErr.StatusCode)
		}
		code = tea.StringValue(sdkErr.Code)
		// SDK 把请求 ID 放在 Data 中；只解析必要字段，禁止输出整个错误或 Data。
		var data struct {
			RequestID string `json:"RequestId"`
		}
		if json.Unmarshal([]byte(tea.StringValue(sdkErr.Data)), &data) == nil {
			requestID = data.RequestID
		}
	}
	level := logit.InfoLevel
	if err != nil {
		if code == "" {
			code = "client_error"
		}
		level = logit.ErrorLevel
		if status >= 400 && status < 500 {
			level = logit.WarnLevel
		}
	} else if duration > c.slowThreshold {
		level = logit.WarnLevel
	}
	if !logit.LoggerFromContext(ctx).Enabled(level) {
		return
	}
	details := map[string]any{
		"operation":   operation,
		"instance_id": c.instanceID,
		"status":      status,
		"request_id":  requestID,
		"error_code":  code,
	}
	logit.Output(ctx, level, 1, downstreamType, logit.DownstreamFields(downstreamType, c.name, duration, details)...)
}
