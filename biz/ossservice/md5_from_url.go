package ossservice

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"

	"github.com/bpcoder16/pixiu/infra/aliyun/ossx"
)

// ErrFileRead 表示 URL 请求、HTTP 状态或正文读取失败，错误中不包含 URL 或响应正文。
// HTTP 404 单独返回 ossx.ErrObjectNotFound，取消和超时可通过 errors.Is 判断。
var ErrFileRead = errors.New("ossservice: read file failed")

// MD5FromURL 下载 URL 内容并返回 32 位小写十六进制 MD5，用于内容或版本比对。
// 无需创建 Service 或提供 OSS、STS 客户端；沿用 http.DefaultClient 的连接和重定向行为。
// 仅接受 HTTP 200，流式读取且关闭响应正文；调用方通过 ctx 控制总期限，本方法不另设超时。
func MD5FromURL(ctx context.Context, fileURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return "", ErrFileRead
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fileReadError(ctx, err)
	}
	defer func() {
		_ = res.Body.Close()
	}()
	if res.StatusCode == http.StatusNotFound {
		return "", ossx.ErrObjectNotFound
	}
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%w: HTTP %d", ErrFileRead, res.StatusCode)
	}
	// 摘要来自完整正文，不能用 ETag 代替；流式读取避免把整个文件保留在内存。
	hash := md5.New()
	if _, readErr := io.Copy(hash, res.Body); readErr != nil {
		return "", fileReadError(ctx, readErr)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func fileReadError(ctx context.Context, cause error) error {
	// 调用方的取消或期限优先；只返回稳定标记，避免底层错误链泄露签名 URL。
	if ctxErr := ctx.Err(); ctxErr == context.Canceled || ctxErr == context.DeadlineExceeded {
		return errors.Join(ErrFileRead, ctxErr)
	}
	// url.Error.Timeout 只检查直接原因，传输层用 %w 包装的超时需要继续向内查找。
	if u, ok := cause.(*url.Error); ok && !u.Timeout() {
		cause = u.Err
	}
	timeout, ok := errors.AsType[net.Error](cause)
	if errors.Is(cause, context.DeadlineExceeded) || (ok && timeout.Timeout()) {
		return errors.Join(ErrFileRead, context.DeadlineExceeded)
	}
	return ErrFileRead
}
