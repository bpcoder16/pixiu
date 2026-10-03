package elasticSearchx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/jsonx"
	"github.com/bpcoder16/pixiu/logit"
)

// operation 只在一次同步操作内使用，元数据直接交给日志，不存入 context。
type operation struct {
	name         string
	index        string
	errorType    string
	bulk         *BulkResult
	failedShards *int // 写入操作在解析后填充，仅用于最终日志，不参与错误判定。
}

func (c *Client) jsonRequest(ctx context.Context, method, suffix string, op operation, payload any, decode func(io.Reader) error) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("elasticSearchx: encode %s request: %w", op.name, err)
	}
	return c.request(ctx, method, suffix, op, body, "application/json", decode)
}

// request 独占响应体；先完成解析与收尾，再按最终错误记录一次日志和耗时。
func (c *Client) request(ctx context.Context, method, suffix string, op operation, body []byte, contentType string, decode func(io.Reader) error) (resultErr error) {
	if ctx == nil {
		return errors.New("elasticSearchx: nil context")
	}
	if c.closed.Load() {
		return errClientClosed
	}
	if strings.TrimSpace(op.index) == "" || strings.Contains(op.index, "/") {
		return errors.New("elasticSearchx: invalid index")
	}
	path := "/" + url.PathEscape(op.index) + "/" + suffix
	req, err := http.NewRequestWithContext(ctx, method, path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("elasticSearchx: build %s request: %w", op.name, err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	started := time.Now()
	var res *http.Response
	var responseBody bytes.Buffer
	// 仅在最终返回错误时作为兜底分类，具体错误类型由后续流程覆盖。
	op.errorType = "response_error"
	defer func() {
		duration := time.Since(started)
		logit.AddDownstreamDurationAuto(ctx, c.durationPrefix, duration)
		c.logResult(req, op, res, resultErr, duration, body, responseBody.Bytes())
	}()
	res, err = c.perform(req)
	if err != nil {
		op.errorType = "transport_error"
		return fmt.Errorf("elasticSearchx: %s request: %w", op.name, err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("elasticSearchx: close %s response: %w", op.name, err))
		}
	}()
	var reader io.Reader = res.Body
	if c.detailsEnabled() {
		reader = io.TeeReader(reader, &responseBody)
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return decodeHTTPError(reader, res.StatusCode, op.name == "get")
	}
	if decode != nil {
		return decode(reader)
	}
	// 无需解析的成功响应仍须读完，以复用连接并保留读取错误。
	if _, err := io.Copy(io.Discard, reader); err != nil {
		return fmt.Errorf("elasticSearchx: read %s response: %w", op.name, err)
	}
	return nil
}

func decodeHTTPError(reader io.Reader, status int, get bool) error {
	var payload struct {
		Found *bool `json:"found"`
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	err := jsonx.DecodeOne(io.LimitReader(reader, 64<<10), &payload)
	httpErr := &HTTPError{Status: status, Type: payload.Error.Type}
	// 无错误体也可按状态识别 HTTP 错误；读取或解析失败则同时保留两类错误。
	if err != nil && err != io.EOF {
		return errors.Join(httpErr, fmt.Errorf("elasticSearchx: decode HTTP error response: %w", err))
	}
	if get && status == http.StatusNotFound && err == nil && payload.Found != nil && !*payload.Found && payload.Error.Type == "" {
		return ErrNotFound
	}
	return httpErr
}
