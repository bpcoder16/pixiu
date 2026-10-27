package stsx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	stssdk "github.com/alibabacloud-go/sts-20150401/v2/client"
	"github.com/alibabacloud-go/tea/dara"
)

var sessionNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_.@-]{2,64}$`)

// AssumeRoleRequest 由业务指定角色与授权范围。
type AssumeRoleRequest struct {
	// RoleARN 为要扮演的 RAM 角色 ARN，必填。
	RoleARN string
	// SessionName 角色会话名，默认 sts-session。
	SessionName string
	// Duration 为 15 分钟至 12 小时的整秒时长，仍受角色最大会话时间限制。
	Duration time.Duration
	// Policy 可选，最多 2048 字节；留空使用角色权限，非空时取与角色权限的交集。
	Policy string
}

// Credentials 是 STS 临时凭据，禁止记录到日志。
type Credentials struct {
	AccessKeyID     string
	AccessKeySecret string
	SecurityToken   string
	Expiration      time.Time
}

// AssumeRole 按请求签发临时凭据，不缓存、不重试，不绑定具体云服务。
func (c *Client) AssumeRole(ctx context.Context, req AssumeRoleRequest) (result Credentials, err error) {
	if err = c.check(ctx); err != nil {
		return
	}
	if !strings.HasPrefix(req.RoleARN, "acs:ram::") || strings.TrimSpace(req.RoleARN) != req.RoleARN {
		return result, errors.New("stsx: RAM role ARN is required")
	}
	if req.SessionName == "" {
		req.SessionName = "sts-session"
	}
	if !sessionNamePattern.MatchString(req.SessionName) {
		return result, errors.New("stsx: invalid session name")
	}
	if req.Duration < 15*time.Minute || req.Duration > 12*time.Hour || req.Duration%time.Second != 0 {
		return result, errors.New("stsx: duration must be whole seconds between 15m and 12h")
	}
	if len(req.Policy) > 2048 || (req.Policy != "" && !json.Valid([]byte(req.Policy))) {
		return result, errors.New("stsx: policy must be valid JSON within 2048 bytes")
	}
	callCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	start := time.Now()
	status := 0
	requestID := ""
	callCtx = context.WithValue(callCtx, stsStatusKey{}, &status)
	defer func() { c.finish(ctx, "AssumeRole", requestID, status, err, time.Since(start)) }()
	request := &stssdk.AssumeRoleRequest{
		RoleArn:         new(req.RoleARN),
		RoleSessionName: new(req.SessionName),
		DurationSeconds: new(int64(req.Duration / time.Second)),
	}
	if req.Policy != "" {
		request.Policy = new(req.Policy)
	}
	response, err := c.sdk.AssumeRoleWithContext(callCtx, request, &dara.RuntimeOptions{Autoretry: new(false)})
	if err != nil {
		if callCtx.Err() != nil {
			err = errors.Join(err, callCtx.Err())
		}
		return result, fmt.Errorf("stsx: AssumeRole: %w", err)
	}
	if response == nil || response.Body == nil {
		return result, errors.New("stsx: empty response")
	}
	requestID = dara.StringValue(response.Body.RequestId)
	if response.Body.Credentials == nil {
		return result, errors.New("stsx: empty credentials")
	}
	value := response.Body.Credentials
	expiration, parseErr := time.Parse(time.RFC3339, dara.StringValue(value.Expiration))
	if parseErr != nil || !expiration.After(time.Now()) || dara.StringValue(value.AccessKeyId) == "" || dara.StringValue(value.AccessKeySecret) == "" || dara.StringValue(value.SecurityToken) == "" {
		return result, errors.New("stsx: invalid credentials")
	}
	return Credentials{
		AccessKeyID:     dara.StringValue(value.AccessKeyId),
		AccessKeySecret: dara.StringValue(value.AccessKeySecret),
		SecurityToken:   dara.StringValue(value.SecurityToken),
		Expiration:      expiration,
	}, nil
}
