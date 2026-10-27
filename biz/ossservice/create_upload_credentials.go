package ossservice

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/infra/aliyun/ossx"
	"github.com/bpcoder16/pixiu/infra/aliyun/stsx"
)

// ErrSTSNotConfigured 表示当前服务未配置签发上传凭据所需的 STS 客户端。
var ErrSTSNotConfigured = errors.New("ossservice: STS client is not configured")

// UploadCredentialsOptions 指定上传凭据使用的角色与有效期。
type UploadCredentialsOptions struct {
	// RoleARN 为要扮演的 RAM 角色 ARN，必填。
	RoleARN string
	// SessionName 为角色会话名，留空沿用 stsx 的 sts-session 默认值。
	SessionName string
	// Duration 为 15 分钟至 12 小时的整秒时长，仍受角色最大会话时间限制。
	Duration time.Duration
}

// UploadCredentialsResult 返回固定 Bucket 的连接信息和临时上传凭据。
type UploadCredentialsResult struct {
	// Info 为传入 OSS 客户端的非敏感连接信息，授权对象固定属于该 Bucket。
	Info ossx.Info
	// Credentials 为仅允许写入指定对象的临时凭据，禁止记录到日志。
	Credentials stsx.Credentials
}

// CreateUploadCredentials 只授权 keys 中的精确对象执行 oss:PutObject。
// 未通过 WithSTSClient 配置 STS 时返回 ErrSTSNotConfigured。
// keys 必须非空，每个 Key 非空且不含 *、?；完整 Policy 的 2048 字节限制由 stsx 校验。
// 不缓存、不重试、不写入数据库，失败返回零值并保留 STS 错误链和 Context 语义。
func (s *Service) CreateUploadCredentials(ctx context.Context, keys []string, options UploadCredentialsOptions) (UploadCredentialsResult, error) {
	if s.sts == nil {
		return UploadCredentialsResult{}, ErrSTSNotConfigured
	}
	info := s.oss.Info()
	policy, err := uploadPolicy(info.Bucket, keys)
	if err != nil {
		return UploadCredentialsResult{}, err
	}
	credentials, err := s.sts.AssumeRole(ctx, stsx.AssumeRoleRequest{
		RoleARN:     options.RoleARN,
		SessionName: options.SessionName,
		Duration:    options.Duration,
		Policy:      policy,
	})
	if err != nil {
		return UploadCredentialsResult{}, err
	}
	return UploadCredentialsResult{
		Info:        info,
		Credentials: credentials,
	}, nil
}

// uploadPolicy 只允许本次申请的对象写入，不能授权其他对象或读取操作。
// STS 策略不限制实际文件大小，实际对象的校验由调用方负责。
func uploadPolicy(bucket string, keys []string) (string, error) {
	if len(keys) == 0 {
		return "", errors.New("OSS upload keys are required")
	}
	resources := make([]string, 0, len(keys))
	for _, key := range keys {
		// RAM 把 *、? 解释为通配符，禁止扩大授权范围。
		if key == "" || strings.ContainsAny(key, "*?") {
			return "", errors.New("OSS upload requires exact object keys")
		}
		resources = append(resources, "acs:oss:*:*:"+bucket+"/"+key)
	}
	policy, err := json.Marshal(map[string]any{
		"Version": "1",
		"Statement": []any{map[string]any{
			"Effect":   "Allow",
			"Action":   []string{"oss:PutObject"},
			"Resource": resources,
		}},
	})
	return string(policy), err
}
