package stsx

import (
	"context"
	"time"

	"github.com/bpcoder16/pixiu/infra/internal/aliyun"
	"github.com/bpcoder16/pixiu/logit"
)

const downstreamType = "AliyunSTS"

func (c *Client) finish(ctx context.Context, operation, requestID string, status int, err error, duration time.Duration) {
	logit.AddDownstreamDurationAuto(ctx, downstreamType+"_"+c.name, duration)
	if !c.logRequests {
		return
	}
	result := aliyun.ClassifyTeaResult(status, requestID, err, duration, c.slowThreshold)
	if !logit.LoggerFromContext(ctx).Enabled(result.Level) {
		return
	}
	details := map[string]any{
		"operation":  operation,
		"status":     result.Status,
		"request_id": result.RequestID,
		"error_code": result.ErrorCode,
	}
	logit.Output(ctx, result.Level, 1, downstreamType, logit.DownstreamFields(downstreamType, c.name, duration, details)...)
}
