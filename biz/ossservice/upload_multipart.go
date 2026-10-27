package ossservice

import (
	"context"
	"errors"
	"mime/multipart"

	"github.com/bpcoder16/pixiu/infra/aliyun/ossx"
)

// UploadMultipart 打开 multipart 文件并流式上传到指定 Key，仅依赖 OSS 客户端。
// 调用方负责文件校验和 Key 生成；不从文件名或文件头设置 ContentType。
// 方法返回前关闭自己打开的文件，MultipartForm.RemoveAll 仍由 HTTP 调用方执行。
// 上传结果及错误链沿用 ossx.PutObject，不重试，不关闭共享 OSS 客户端。
func (s *Service) UploadMultipart(ctx context.Context, key string, file *multipart.FileHeader) (ossx.PutObjectResult, error) {
	if file == nil {
		return ossx.PutObjectResult{}, errors.New("ossservice: multipart file is required")
	}
	body, err := file.Open()
	if err != nil {
		return ossx.PutObjectResult{}, err
	}
	defer func() {
		// 沿用直传流程的关闭语义，不用关闭错误覆盖上传结果。
		_ = body.Close()
	}()
	return s.oss.PutObject(ctx, ossx.PutObjectRequest{
		Key:  key,
		Body: body,
	})
}
