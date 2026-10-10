package elasticsearchx

import (
	"errors"
	"net/http"
	"time"

	"github.com/bpcoder16/pixiu/logit"
)

const downstreamElasticsearchMessage = "elasticsearch"

func (c *Client) resultLevel(err error, duration time.Duration) logit.Level {
	if err != nil && err != ErrNotFound {
		// 仅单纯 HTTP 4xx 为 Warn；叠加读取或关闭错误时仍为 Error。
		if httpErr, ok := err.(*HTTPError); ok && httpErr.Status >= 400 && httpErr.Status < 500 {
			return logit.WarnLevel
		}
		return logit.ErrorLevel
	}
	if duration > c.slowThreshold {
		return logit.WarnLevel
	}
	return logit.InfoLevel
}

func (c *Client) detailsEnabled() bool {
	return c.logRequests && c.logDetails
}

func (c *Client) logResult(req *http.Request, op operation, res *http.Response, err error, duration time.Duration, requestBody, responseBody []byte) {
	ctx := req.Context()
	level := c.resultLevel(err, duration)
	failedShards := 0
	if op.failedShards != nil {
		failedShards = *op.failedShards
	}
	// 副本异常提升成功日志的级别，已有的动作或收尾错误仍按 Error 记录。
	if failedShards > 0 && level == logit.InfoLevel {
		level = logit.WarnLevel
	}
	if !c.logRequests || !logit.LoggerFromContext(ctx).Enabled(level) {
		return
	}
	status := 0
	if res != nil {
		status = res.StatusCode
	}
	errorType := ""
	if err != nil {
		errorType = op.errorType
		var httpErr *HTTPError
		var bulkErr *BulkError
		var partialSearchErr *PartialSearchError
		switch {
		case err == ErrNotFound:
			errorType = "document_not_found"
		case errors.As(err, &httpErr):
			errorType = httpErr.Type
			if errorType == "" {
				errorType = "http_error"
			}
		case errors.As(err, &bulkErr):
			errorType = "bulk_error"
		case errors.As(err, &partialSearchErr):
			errorType = "partial_search_error"
		}
	}
	details := map[string]any{
		"method":     req.Method,
		"operation":  op.name,
		"index":      op.index,
		"status":     status,
		"error_type": errorType,
	}
	if op.bulk != nil && status >= 200 && status < 300 {
		details["succeeded"] = op.bulk.Succeeded
		details["failed"] = len(op.bulk.Failures)
	}
	if failedShards > 0 {
		details["failed_shards"] = failedShards
	}
	if c.logDetails {
		proto, statusText := "", ""
		if res != nil {
			proto, statusText = res.Proto, res.Status
		}
		details["request_body"] = string(requestBody)
		details["response_proto"] = proto
		details["response_body"] = string(responseBody)
		details["response_status_text"] = statusText
	}
	logit.Output(ctx, level, 1, downstreamElasticsearchMessage, logit.DownstreamFields(downstreamElasticsearchMessage, c.name, duration, details)...)
}
