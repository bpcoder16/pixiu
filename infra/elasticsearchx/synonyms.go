package elasticsearchx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/bpcoder16/pixiu/jsonx"
)

// ReloadDetail 是服务端报告的单个索引重载详情，空分析器数组不代表同义词已更新。
type ReloadDetail struct {
	Index             string   `json:"index"`
	ReloadedAnalyzers []string `json:"reloaded_analyzers"`
	ReloadedNodeIDs   []string `json:"reloaded_node_ids"`
}

// ReloadSearchAnalyzersResult 保留两阶段已完整解析的结果，发生分片或关闭错误时仍可用于诊断。
type ReloadSearchAnalyzersResult struct {
	// Shards 是 reload 的执行计数，按持有分片的节点统计，不等于物理分片数。
	Shards        ShardsInfo
	ReloadDetails []ReloadDetail
	// CacheShards 为 nil 表示尚未取得有效的缓存清理响应，不保证请求未发送。
	CacheShards *ShardsInfo
}

// ReloadSearchAnalyzers 重载单个具体索引的搜索分析器，成功后清理该索引的 request cache。
// 调用方须预先更新所有数据节点的同义词文件，过滤器须设置 updateable: true 且仅用于搜索分析器。
// index 须为实际索引名；不解析别名或数据流。两个请求共用 ctx，任一步失败即停止，不重试或回滚。
// nil 错误只表示两次请求成功，是否重载了预期分析器应检查 ReloadDetails。
func (c *Client) ReloadSearchAnalyzers(ctx context.Context, index string) (ReloadSearchAnalyzersResult, error) {
	if err := validateConcreteIndex(index); err != nil {
		return ReloadSearchAnalyzersResult{}, err
	}
	var result ReloadSearchAnalyzersResult
	op := operation{
		name:  "reload_search_analyzers",
		index: index,
	}
	requestErr := c.request(ctx, http.MethodPost, "_reload_search_analyzers", op, nil, "", func(reader io.Reader) error {
		var payload struct {
			Shards        synonymShards  `json:"_shards"`
			ReloadDetails []ReloadDetail `json:"reload_details"`
		}
		if err := jsonx.DecodeOne(reader, &payload); err != nil {
			return fmt.Errorf("elasticsearchx: decode reload search analyzers response: %w", err)
		}
		shards, err := payload.Shards.info()
		if err != nil {
			return fmt.Errorf("elasticsearchx: reload search analyzers: %w", err)
		}
		if payload.ReloadDetails == nil {
			return errors.New("elasticsearchx: missing or invalid reload details array")
		}
		for _, detail := range payload.ReloadDetails {
			if detail.Index == "" || detail.ReloadedAnalyzers == nil || detail.ReloadedNodeIDs == nil {
				return errors.New("elasticsearchx: incomplete reload detail")
			}
		}
		// 只保存已完整解析的状态；后续关闭或缓存清理失败不能抹掉已经完成的重载结果。
		result.Shards = shards
		result.ReloadDetails = payload.ReloadDetails
		if shards.Failed > 0 {
			return fmt.Errorf("elasticsearchx: reload search analyzers: failed_shards=%d", shards.Failed)
		}
		return nil
	})
	if requestErr != nil {
		return result, requestErr
	}
	op.name = "clear_request_cache"
	requestErr = c.request(ctx, http.MethodPost, "_cache/clear?request=true", op, nil, "", func(reader io.Reader) error {
		var payload struct {
			Shards synonymShards `json:"_shards"`
		}
		if err := jsonx.DecodeOne(reader, &payload); err != nil {
			return fmt.Errorf("elasticsearchx: decode clear request cache response: %w", err)
		}
		shards, err := payload.Shards.info()
		if err != nil {
			return fmt.Errorf("elasticsearchx: clear request cache: %w", err)
		}
		result.CacheShards = &shards
		if shards.Failed > 0 {
			return fmt.Errorf("elasticsearchx: clear request cache: failed_shards=%d", shards.Failed)
		}
		return nil
	})
	return result, requestErr
}

// 指针字段区分缺失与合法零值，两阶段共用广播响应的计数校验。
type synonymShards struct {
	Total      *int            `json:"total"`
	Successful *int            `json:"successful"`
	Failed     *int            `json:"failed"`
	Failures   json.RawMessage `json:"failures"`
}

func (s synonymShards) info() (ShardsInfo, error) {
	if s.Total == nil || s.Successful == nil || s.Failed == nil {
		return ShardsInfo{}, errors.New("incomplete shard counts")
	}
	total, successful, failed := *s.Total, *s.Successful, *s.Failed
	// 使用减法避免计数相加溢出；未分配分片不要求计入 successful 或 failed。
	if total < 0 || successful < 0 || failed < 0 || successful > total || failed > total-successful {
		return ShardsInfo{}, errors.New("invalid shard counts")
	}
	return ShardsInfo{
		Total:      total,
		Successful: successful,
		Failed:     failed,
		Failures:   s.Failures,
	}, nil
}
