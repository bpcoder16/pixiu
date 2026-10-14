package esx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	essdk "github.com/alibabacloud-go/elasticsearch-20170613/v6/client"
	"github.com/alibabacloud-go/tea/dara"
)

// DictInfo 是管理端词典元数据，不能据此推断节点已加载该版本。
type DictInfo struct {
	Name       string
	FileSize   int64
	SourceType string
	Type       string
	// DownloadURL 可能含短期签名，不应写入日志。
	DownloadURL string
}

type ListSynonymsDictsResult struct {
	RequestID string
	Dicts     []DictInfo
}

type UpdateSynonymsDictsResult struct {
	RequestID string
	Dicts     []DictInfo
}

type DescribeInstanceResult struct {
	RequestID  string
	InstanceID string
	Status     string
	Version    string
}

// SynonymsDict 表示完整目标清单中的一个同义词文件，零值无效。
// 使用 KeepSynonymsDict 保留已有文件，使用 SynonymsDictFromOSS 从 OSS 新增或替换文件。
// 构造器仅组装参数，文件名、来源和 OSS 参数在 UpdateSynonymsDicts 调用时校验。
//
// 以下示例假设 client 已创建、ctx 为请求 context，目标清单只有这两个文件；
// 遗漏已有文件会删除它，同一次更新的重试须复用 ClientToken：
//
//	req := esx.UpdateSynonymsDictsRequest{
//		ClientToken: "unique-release-token",
//		Dicts: []esx.SynonymsDict{
//			esx.KeepSynonymsDict("existing.txt"),
//			esx.SynonymsDictFromOSS("synonyms.txt", "dict-bucket", "releases/v1/synonyms.txt"),
//		},
//	}
//	result, err := client.UpdateSynonymsDicts(ctx, req)
//	if err != nil {
//		return err
//	}
//	_ = result.RequestID
type SynonymsDict struct {
	// name 是 ES 管理端词典文件名，不是 OSS 对象 key 或 ES 节点文件路径。
	name string
	// sourceType 为 ORIGIN 时保留已有文件，为 OSS 时从 bucket/key 导入。
	sourceType string
	// bucket 和 key 仅用于 OSS 来源，分别表示 Bucket 名称和完整对象 key。
	bucket string
	key    string
}

// KeepSynonymsDict 在完整目标清单中保留已有同名文件；不重新上传它。
// name 应使用 ListSynonymsDicts 返回的 DictInfo.Name，例如：
//
//	dict := esx.KeepSynonymsDict("existing.txt")
func KeepSynonymsDict(name string) SynonymsDict {
	return SynonymsDict{
		name:       name,
		sourceType: "ORIGIN",
	}
}

// SynonymsDictFromOSS 从 OSS 新增或替换文件；不会修改对象 ACL 或使用上传模块的凭据。
// name 是目标词典文件名，bucket 是 OSS Bucket 名称，key 是完整对象 key；
// name 可以与 OSS 对象的文件名不同，例如：
//
//	dict := esx.SynonymsDictFromOSS("synonyms.txt", "dict-bucket", "releases/v1/source.txt")
func SynonymsDictFromOSS(name, bucket, key string) SynonymsDict {
	return SynonymsDict{
		name:       name,
		sourceType: "OSS",
		bucket:     bucket,
		key:        key,
	}
}

// UpdateSynonymsDictsRequest 中 Dicts 是完整目标清单，遗漏已有文件会删除它。
type UpdateSynonymsDictsRequest struct {
	// ClientToken 同一次更新的重试须复用，不同更新须更换，最多 64 个 ASCII 字符。
	ClientToken string
	Dicts       []SynonymsDict
}

var dictNamePattern = regexp.MustCompile(`^[A-Za-z0-9_]+\.txt$`)

type dictPayload struct {
	Name       string     `json:"name"`
	SourceType string     `json:"sourceType"`
	Type       string     `json:"type"`
	OSSObject  *ossObject `json:"ossObject,omitempty"`
}

type ossObject struct {
	BucketName string `json:"bucketName"`
	Key        string `json:"key"`
}

func (r UpdateSynonymsDictsRequest) encode() (string, error) {
	if strings.TrimSpace(r.ClientToken) == "" || len(r.ClientToken) > 64 {
		return "", errors.New("esx: client token must contain 1 to 64 ASCII characters")
	}
	for _, ch := range r.ClientToken {
		if ch > 127 {
			return "", errors.New("esx: client token must be ASCII")
		}
	}
	if len(r.Dicts) == 0 {
		return "", errors.New("esx: full dictionary inventory must not be empty")
	}
	seen := make(map[string]bool, len(r.Dicts))
	payload := make([]dictPayload, 0, len(r.Dicts))
	for _, dict := range r.Dicts {
		if len(dict.name) > 30 || !dictNamePattern.MatchString(dict.name) {
			return "", errors.New("esx: invalid dictionary filename")
		}
		if seen[dict.name] {
			return "", errors.New("esx: duplicate dictionary filename")
		}
		seen[dict.name] = true
		entry := dictPayload{
			Name:       dict.name,
			SourceType: dict.sourceType,
			Type:       "SYNONYMS",
		}
		switch dict.sourceType {
		case "ORIGIN":
		case "OSS":
			if strings.TrimSpace(dict.bucket) == "" || dict.key == "" {
				return "", errors.New("esx: OSS bucket and key are required")
			}
			entry.OSSObject = &ossObject{
				BucketName: dict.bucket,
				Key:        dict.key,
			}
		default:
			return "", errors.New("esx: invalid dictionary source")
		}
		payload = append(payload, entry)
	}
	// 仅编码自有的字符串结构，不接受任意请求体，避免绕开完整清单校验。
	encoded, err := json.Marshal(payload)
	return string(encoded), err
}

// ListSynonymsDicts 返回全部同义词文件，不做名称过滤或与本地状态合并。
func (c *Client) ListSynonymsDicts(ctx context.Context) (result ListSynonymsDictsResult, err error) {
	if err = c.check(ctx); err != nil {
		return
	}
	start := time.Now()
	callCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	status := 0
	defer func() {
		c.finish(ctx, "ListSynonymsDicts", result.RequestID, status, err, time.Since(start))
	}()
	response, callErr := c.sdk.ListDictsWithContext(callCtx, new(c.instanceID), &essdk.ListDictsRequest{AnalyzerType: new("SYNONYMS")}, nil, &dara.RuntimeOptions{})
	if callErr != nil {
		return result, fmt.Errorf("esx: ListSynonymsDicts: %w", callErr)
	}
	if response != nil {
		status = int(dara.Int32Value(response.StatusCode))
	}
	if response == nil || response.Body == nil || dara.StringValue(response.Body.RequestId) == "" || response.Body.Result == nil {
		return result, errors.New("esx: incomplete ListSynonymsDicts response")
	}
	result.RequestID = dara.StringValue(response.Body.RequestId)
	result.Dicts = make([]DictInfo, 0, len(response.Body.Result))
	for _, dict := range response.Body.Result {
		if dict == nil || dara.StringValue(dict.Name) == "" {
			return result, errors.New("esx: incomplete dictionary metadata")
		}
		result.Dicts = append(result.Dicts, DictInfo{
			Name:        dara.StringValue(dict.Name),
			FileSize:    dara.Int64Value(dict.FileSize),
			SourceType:  dara.StringValue(dict.SourceType),
			Type:        dara.StringValue(dict.Type),
			DownloadURL: dara.StringValue(dict.DownloadUrl),
		})
	}
	return result, nil
}

// UpdateSynonymsDicts 提交完整词典清单；同实例更新须由调用方串行化。
// 返回成功仅表示管理 API 成功，不保证分发、reload 完成或不重启实例。
func (c *Client) UpdateSynonymsDicts(ctx context.Context, req UpdateSynonymsDictsRequest) (result UpdateSynonymsDictsResult, err error) {
	if err = c.check(ctx); err != nil {
		return
	}
	body, err := req.encode()
	if err != nil {
		return result, err
	}
	start := time.Now()
	callCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	status := 0
	defer func() {
		c.finish(ctx, "UpdateSynonymsDicts", result.RequestID, status, err, time.Since(start))
	}()
	response, callErr := c.sdk.UpdateSynonymsDictsWithContext(callCtx, new(c.instanceID), &essdk.UpdateSynonymsDictsRequest{
		Body:        new(body),
		ClientToken: new(req.ClientToken),
	}, nil, &dara.RuntimeOptions{})
	if callErr != nil {
		return result, fmt.Errorf("esx: UpdateSynonymsDicts: %w", callErr)
	}
	if response != nil {
		status = int(dara.Int32Value(response.StatusCode))
	}
	if response == nil || response.Body == nil || dara.StringValue(response.Body.RequestId) == "" || response.Body.Result == nil {
		return result, errors.New("esx: incomplete UpdateSynonymsDicts response")
	}
	result.RequestID = dara.StringValue(response.Body.RequestId)
	result.Dicts = make([]DictInfo, 0, len(response.Body.Result))
	for _, dict := range response.Body.Result {
		if dict == nil || dara.StringValue(dict.Name) == "" {
			return result, errors.New("esx: incomplete dictionary metadata")
		}
		result.Dicts = append(result.Dicts, DictInfo{
			Name:       dara.StringValue(dict.Name),
			FileSize:   dara.Int64Value(dict.FileSize),
			SourceType: dara.StringValue(dict.SourceType),
			Type:       dara.StringValue(dict.Type),
		})
	}
	return result, nil
}

// DescribeInstance 返回服务端原始状态；active 不证明某次词库已分发完成。
func (c *Client) DescribeInstance(ctx context.Context) (result DescribeInstanceResult, err error) {
	if err = c.check(ctx); err != nil {
		return
	}
	start := time.Now()
	callCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	status := 0
	defer func() {
		c.finish(ctx, "DescribeInstance", result.RequestID, status, err, time.Since(start))
	}()
	response, callErr := c.sdk.DescribeInstanceWithContext(callCtx, new(c.instanceID), nil, &dara.RuntimeOptions{})
	if callErr != nil {
		return result, fmt.Errorf("esx: DescribeInstance: %w", callErr)
	}
	if response != nil {
		status = int(dara.Int32Value(response.StatusCode))
	}
	if response == nil || response.Body == nil || response.Body.Result == nil {
		return result, errors.New("esx: incomplete DescribeInstance response")
	}
	body := response.Body
	result = DescribeInstanceResult{
		RequestID:  dara.StringValue(body.RequestId),
		InstanceID: dara.StringValue(body.Result.InstanceId),
		Status:     dara.StringValue(body.Result.Status),
		Version:    dara.StringValue(body.Result.EsVersion),
	}
	if result.RequestID == "" || result.InstanceID == "" || result.Status == "" || result.Version == "" {
		return result, errors.New("esx: incomplete instance metadata")
	}
	return result, nil
}
