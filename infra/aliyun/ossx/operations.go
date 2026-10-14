package ossx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/bpcoder16/pixiu/logit"
)

const downstreamType = "AliyunOSS"

// PutObjectRequest 上传原始字节流；Body 由调用方关闭，调用完成前不得并发读写。
type PutObjectRequest struct {
	Key         string
	Body        io.Reader
	ContentType string
}

// PutFileRequest 上传本地文件；文件由本包打开并关闭。
type PutFileRequest struct {
	Key         string
	FilePath    string
	ContentType string
}

// PutObjectResult 保留服务端标识；ETag 不保证等于内容 MD5。
type PutObjectResult struct {
	RequestID string
	ETag      string
	VersionID string
}

// PutObject 以普通 PUT 上传流，保留 SDK 的 CRC64 校验，不自动重试。
// 超时或网络错误不证明远端未写入；调用方应自行决定覆盖、版本和重试策略。
func (c *Client) PutObject(ctx context.Context, req PutObjectRequest) (PutObjectResult, error) {
	if err := c.check(ctx); err != nil {
		return PutObjectResult{}, err
	}
	if req.Key == "" {
		return PutObjectResult{}, errors.New("ossx: key is required")
	}
	if req.Body == nil {
		return PutObjectResult{}, errors.New("ossx: body is required")
	}
	return c.put(ctx, "PutObject", req, nil)
}

// PutFile 流式上传文件，不把整个文件读入内存；文件收尾完成后才登记日志和耗时。
func (c *Client) PutFile(ctx context.Context, req PutFileRequest) (PutObjectResult, error) {
	if err := c.check(ctx); err != nil {
		return PutObjectResult{}, err
	}
	if req.Key == "" {
		return PutObjectResult{}, errors.New("ossx: key is required")
	}
	if req.FilePath == "" {
		return PutObjectResult{}, errors.New("ossx: file path is required")
	}
	file, err := os.Open(req.FilePath)
	if err != nil {
		return PutObjectResult{}, fmt.Errorf("ossx: open file: %w", err)
	}
	return c.put(ctx, "PutFile", PutObjectRequest{
		Key:         req.Key,
		Body:        file,
		ContentType: req.ContentType,
	}, file)
}

func (c *Client) put(ctx context.Context, operation string, req PutObjectRequest, file *os.File) (result PutObjectResult, err error) {
	start := time.Now()
	callCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	status := 0
	requestID := ""
	defer func() {
		// 先关闭自有文件，再输出最终结果；上传错误与收尾错误都保留。
		if file != nil {
			if closeErr := file.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("ossx: close file: %w", closeErr))
			}
		}
		c.finish(ctx, operation, req, requestID, status, err, time.Since(start))
	}()
	sdkReq := &oss.PutObjectRequest{
		Bucket: new(c.bucket),
		Key:    new(req.Key),
		Body:   req.Body,
	}
	if req.ContentType != "" {
		sdkReq.ContentType = new(req.ContentType)
	}
	response, err := c.sdk.PutObject(callCtx, sdkReq, func(opts *oss.Options) {
		// CRC 校验失败时 SDK 不返回结果，须在校验前保存日志所需的响应信息。
		opts.ResponseHandlers = append(opts.ResponseHandlers, func(response *http.Response) error {
			status = response.StatusCode
			requestID = response.Header.Get("x-oss-request-id")
			return nil
		})
	})
	if response != nil {
		result = PutObjectResult{
			RequestID: requestID,
			ETag:      oss.ToString(response.ETag),
			VersionID: oss.ToString(response.VersionId),
		}
	}
	if err != nil {
		return result, fmt.Errorf("ossx: %s: %w", operation, err)
	}
	return result, nil
}

func (c *Client) finish(ctx context.Context, operation string, req PutObjectRequest, requestID string, status int, err error, duration time.Duration) {
	logit.AddDownstreamDurationAuto(ctx, downstreamType+"_"+c.name, duration)
	if !c.logRequests {
		return
	}
	code := ""
	if serviceErr, ok := errors.AsType[*oss.ServiceError](err); ok {
		status, code, requestID = serviceErr.StatusCode, serviceErr.Code, serviceErr.RequestID
	}
	level := logit.InfoLevel
	if err != nil {
		code = nonemptyCode(code)
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
	// 原始 SDK 错误可能带签名 URL 和响应正文，只记录结构化元数据。
	details := map[string]any{
		"operation":  operation,
		"bucket":     c.bucket,
		"key":        req.Key,
		"status":     status,
		"request_id": requestID,
		"error_code": code,
	}
	logit.Output(ctx, level, 1, downstreamType, logit.DownstreamFields(downstreamType, c.name, duration, details)...)
}

func nonemptyCode(code string) string {
	if code == "" {
		return "client_error"
	}
	return code
}
