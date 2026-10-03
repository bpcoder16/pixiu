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

	"github.com/bpcoder16/pixiu/jsonx"
)

// ErrNotFound 表示 Get 查询的文档不存在。
var ErrNotFound = errors.New("elasticSearchx: document not found")

// HTTPError 表示 Elasticsearch 返回非成功的 HTTP 状态。
type HTTPError struct {
	Status int
	Type   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("elasticSearchx: HTTP %d, type %q", e.Status, e.Type)
}

// PartialSearchError 表示搜索响应已完整解析，但超时或分片失败使结果可能不完整。
// Search 同时返回已有结果；可通过 errors.As 识别，组合错误中的其他失败仍须处理。
type PartialSearchError struct {
	TimedOut     bool
	FailedShards int
}

func (e *PartialSearchError) Error() string {
	return fmt.Sprintf("elasticSearchx: partial search response: timed_out=%t, failed_shards=%d", e.TimedOut, e.FailedShards)
}

// Total 描述搜索命中数量；Relation 为 eq 时数量准确，为 gte 时仅表示下限。
type Total struct {
	Value    uint64 `json:"value"`
	Relation string `json:"relation"`
}

// ShardsInfo 描述搜索分片的执行状态，Failures 保留失败原因的原始 JSON。
type ShardsInfo struct {
	Total      int             `json:"total"`
	Successful int             `json:"successful"`
	Skipped    int             `json:"skipped"`
	Failed     int             `json:"failed"`
	Failures   json.RawMessage `json:"failures"`
}

// SearchResult 是扁平的搜索结果，保留命中与聚合的原始 JSON，避免业务 ID 经 float64 丢失精度。
// 超时或分片失败时与 PartialSearchError 一起返回，调用方按业务要求处理部分结果。
// Took 和 TerminatedEarly 暂不启用，恢复时同步恢复解析、结果映射和测试断言。
type SearchResult struct {
	Total        *Total // 未统计总数时为 nil，与零命中区分。
	Hits         json.RawMessage
	Aggregations json.RawMessage
	// Took int64 // 服务端耗时，单位毫秒。
	TimedOut bool
	Shards   ShardsInfo
	// TerminatedEarly *bool // 使用 terminate_after 时按需启用，未返回标记时为 nil。
}

// Search 在指定索引执行调用方提供的 DSL。
// 超时或分片失败时同时返回已有结果及 *PartialSearchError，不将部分结果当作完整成功。
// 此时若 Body 关闭也失败，结果仍保留，返回的错误链同时包含关闭错误。
func (c *Client) Search(ctx context.Context, index string, dsl any) (SearchResult, error) {
	if dsl == nil {
		return SearchResult{}, errors.New("elasticSearchx: nil search DSL")
	}
	var payload struct {
		Hits struct {
			Total *Total          `json:"total"`
			Hits  json.RawMessage `json:"hits"`
		} `json:"hits"`
		Aggregations json.RawMessage `json:"aggregations"`
		// Took int64 `json:"took"`
		TimedOut bool       `json:"timed_out"`
		Shards   ShardsInfo `json:"_shards"`
		// TerminatedEarly *bool `json:"terminated_early"`
	}
	var partialErr *PartialSearchError
	op := operation{
		name:  "search",
		index: index,
	}
	err := c.jsonRequest(ctx, http.MethodPost, "_search", op, dsl, func(reader io.Reader) error {
		if err := jsonx.DecodeOne(reader, &payload); err != nil {
			return fmt.Errorf("elasticSearchx: decode search response: %w", err)
		}
		// DecodeOne 已确认 JSON 语法完整；检查类型而不重新解码或丢失整数精度。
		hits := bytes.TrimSpace(payload.Hits.Hits)
		if len(hits) == 0 || hits[0] != '[' {
			return errors.New("elasticSearchx: missing or invalid search hits array")
		}
		if payload.TimedOut || payload.Shards.Failed > 0 {
			partialErr = &PartialSearchError{
				TimedOut:     payload.TimedOut,
				FailedShards: payload.Shards.Failed,
			}
			return partialErr
		}
		return nil
	})
	// 仅保留已完整解码并确认的部分结果；读取、解析等失败仍返回零值。
	if err != nil && partialErr == nil {
		return SearchResult{}, err
	}
	return SearchResult{
		Total:        payload.Hits.Total,
		Hits:         payload.Hits.Hits,
		Aggregations: payload.Aggregations,
		// Took: payload.Took,
		TimedOut: payload.TimedOut,
		Shards:   payload.Shards,
		// TerminatedEarly: payload.TerminatedEarly,
	}, err
}

// Count 返回匹配 DSL 的文档数；分片失败时返回零值及错误，不接受部分计数。
func (c *Client) Count(ctx context.Context, index string, dsl any) (int64, error) {
	if dsl == nil {
		return 0, errors.New("elasticSearchx: nil count DSL")
	}
	var payload struct {
		Count  *int64     `json:"count"`
		Shards ShardsInfo `json:"_shards"`
	}
	op := operation{
		name:  "count",
		index: index,
	}
	err := c.jsonRequest(ctx, http.MethodPost, "_count", op, dsl, func(reader io.Reader) error {
		if err := jsonx.DecodeOne(reader, &payload); err != nil {
			return fmt.Errorf("elasticSearchx: decode count response: %w", err)
		}
		if payload.Count == nil {
			return errors.New("elasticSearchx: incomplete count response")
		}
		if payload.Shards.Failed > 0 {
			return fmt.Errorf("elasticSearchx: count response has %d failed shards", payload.Shards.Failed)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return *payload.Count, nil
}

// Get 返回文档的 _source；不存在时返回 ErrNotFound。
// 文档存在但禁用 _source 时返回 (nil, nil)；无效响应返回错误。
func (c *Client) Get(ctx context.Context, index, id string) (json.RawMessage, error) {
	if id == "" {
		return nil, errors.New("elasticSearchx: empty document ID")
	}
	var payload struct {
		Found  *bool           `json:"found"`
		Source json.RawMessage `json:"_source"`
	}
	op := operation{
		name:  "get",
		index: index,
	}
	err := c.request(ctx, http.MethodGet, "_doc/"+url.PathEscape(id), op, nil, "", func(reader io.Reader) error {
		if err := jsonx.DecodeOne(reader, &payload); err != nil {
			return fmt.Errorf("elasticSearchx: decode get response: %w", err)
		}
		if payload.Found == nil || !*payload.Found {
			return errors.New("elasticSearchx: missing or invalid get found flag")
		}
		// 禁用 _source 的索引会省略该字段；存在时必须是完整 JSON 对象。
		source := bytes.TrimSpace(payload.Source)
		if payload.Source != nil && (len(source) == 0 || source[0] != '{') {
			return errors.New("elasticSearchx: invalid get source object")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return payload.Source, nil
}

// Index 按显式文档 ID 创建或整体覆盖文档。
// 主分片写入成功时，副本失败不返回错误；开启请求日志时以 Warn 记录失败分片数。
func (c *Client) Index(ctx context.Context, index, id string, document any) error {
	if id == "" || document == nil {
		return errors.New("elasticSearchx: empty document ID or content")
	}
	var payload struct {
		Result string `json:"result"`
		Shards struct {
			Total      *int `json:"total"`
			Successful *int `json:"successful"`
			Failed     *int `json:"failed"`
		} `json:"_shards"`
	}
	var failedShards int
	op := operation{
		name:         "index",
		index:        index,
		failedShards: &failedShards,
	}
	return c.jsonRequest(ctx, http.MethodPut, "_doc/"+url.PathEscape(id), op, document, func(reader io.Reader) error {
		if err := jsonx.DecodeOne(reader, &payload); err != nil {
			return fmt.Errorf("elasticSearchx: decode index response: %w", err)
		}
		if payload.Result == "" || payload.Shards.Total == nil || payload.Shards.Successful == nil || payload.Shards.Failed == nil {
			return errors.New("elasticSearchx: incomplete index response")
		}
		total, successful, failed := *payload.Shards.Total, *payload.Shards.Successful, *payload.Shards.Failed
		// 未分配副本不计为失败，pipeline 丢弃文档也可能返回零分片。
		// 使用减法检查上界，避免不可信计数相加溢出。
		if total < 0 || successful < 0 || failed < 0 || successful > total || failed > total-successful {
			return errors.New("elasticSearchx: invalid index shard counts")
		}
		// 成功响应中的副本失败只用于观测，不改变主分片写入成功的结果。
		failedShards = failed
		return nil
	})
}
