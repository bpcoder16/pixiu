package elasticsearchx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bpcoder16/pixiu/jsonx"
)

// CreateIndexResult 是完整解析的创建响应；未确认不等于索引未创建，不应直接重试。
type CreateIndexResult struct {
	// Index 是响应中的具体索引名，与请求创建的索引名一致。
	Index string
	// Acknowledged 表示索引创建是否在超时前获得集群确认；false 不代表索引未创建。
	Acknowledged bool
	// ShardsAcknowledged 表示每个分片所需数量的副本是否在超时前启动，默认只等待主分片。
	// false 时分片仍可能随后就绪，不应据此重试创建索引。
	ShardsAcknowledged bool
}

// CreateIndex 创建具体索引，definition 提供 settings、mappings 等配置。
// nil 错误仅表示响应成功解析，调用方仍须检查两个确认标记。
// 完整结果在 Body 关闭失败时也保留，供调用方识别已创建的资源并收尾。
func (c *Client) CreateIndex(ctx context.Context, index string, definition any) (CreateIndexResult, error) {
	if err := validateConcreteIndex(index); err != nil {
		return CreateIndexResult{}, err
	}
	if definition == nil {
		return CreateIndexResult{}, errors.New("elasticsearchx: nil index definition")
	}
	var result CreateIndexResult
	op := operation{
		name:  "create_index",
		index: index,
	}
	err := c.jsonRequest(ctx, http.MethodPut, "", op, definition, func(reader io.Reader) error {
		var payload struct {
			Index              string `json:"index"`
			Acknowledged       *bool  `json:"acknowledged"`
			ShardsAcknowledged *bool  `json:"shards_acknowledged"`
		}
		if err := jsonx.DecodeOne(reader, &payload); err != nil {
			return fmt.Errorf("elasticsearchx: decode create index response: %w", err)
		}
		if payload.Index != index || payload.Acknowledged == nil || payload.ShardsAcknowledged == nil {
			return errors.New("elasticsearchx: incomplete or mismatched create index response")
		}
		if !*payload.Acknowledged && *payload.ShardsAcknowledged {
			return errors.New("elasticsearchx: inconsistent create index acknowledgement")
		}
		// 只有完整且一致的响应才可作为资源归属的依据。
		result = CreateIndexResult{
			Index:              payload.Index,
			Acknowledged:       *payload.Acknowledged,
			ShardsAcknowledged: *payload.ShardsAcknowledged,
		}
		return nil
	})
	return result, err
}

// DeleteIndex 删除单个具体索引并要求确认；索引不存在仍返回 HTTPError。
func (c *Client) DeleteIndex(ctx context.Context, index string) error {
	if err := validateConcreteIndex(index); err != nil {
		return err
	}
	op := operation{
		name:  "delete_index",
		index: index,
	}
	return c.request(ctx, http.MethodDelete, "", op, nil, "", func(reader io.Reader) error {
		var payload struct {
			Acknowledged *bool `json:"acknowledged"`
		}
		if err := jsonx.DecodeOne(reader, &payload); err != nil {
			return fmt.Errorf("elasticsearchx: decode delete index response: %w", err)
		}
		if payload.Acknowledged == nil || !*payload.Acknowledged {
			return errors.New("elasticsearchx: index deletion not acknowledged")
		}
		return nil
	})
}

// AnalyzeResult 保留普通分析响应的 tokens 数组，允许合法空数组。
type AnalyzeResult struct {
	Tokens json.RawMessage
}

// Analyze 使用索引上的命名分析器处理一段文本，不启用 explain 模式。
func (c *Client) Analyze(ctx context.Context, index, analyzer, text string) (AnalyzeResult, error) {
	if err := validateConcreteIndex(index); err != nil {
		return AnalyzeResult{}, err
	}
	if strings.TrimSpace(analyzer) == "" {
		return AnalyzeResult{}, errors.New("elasticsearchx: empty analyzer")
	}
	request := struct {
		Analyzer string `json:"analyzer"`
		Text     string `json:"text"`
	}{
		Analyzer: analyzer,
		Text:     text,
	}
	var payload struct {
		Tokens json.RawMessage `json:"tokens"`
	}
	op := operation{
		name:  "analyze",
		index: index,
	}
	err := c.jsonRequest(ctx, http.MethodPost, "_analyze", op, request, func(reader io.Reader) error {
		if err := jsonx.DecodeOne(reader, &payload); err != nil {
			return fmt.Errorf("elasticsearchx: decode analyze response: %w", err)
		}
		tokens := bytes.TrimSpace(payload.Tokens)
		if len(tokens) == 0 || tokens[0] != '[' {
			return errors.New("elasticsearchx: missing or invalid analyze tokens array")
		}
		return nil
	})
	if err != nil {
		return AnalyzeResult{}, err
	}
	return AnalyzeResult{Tokens: payload.Tokens}, nil
}

// 索引管理操作拒绝选择器，防止意外扩大删除范围；原有查询仍可使用索引列表等语法。
func validateConcreteIndex(index string) error {
	if index == "" || len(index) > 255 || !utf8.ValidString(index) || index != strings.ToLower(index) || index == "." || index == ".." {
		return errors.New("elasticsearchx: invalid concrete index name")
	}
	if strings.ContainsAny(index, `\/*?"<>| ,#:`) || strings.ContainsRune("-_+", rune(index[0])) {
		return errors.New("elasticsearchx: invalid concrete index name")
	}
	for _, r := range index {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return errors.New("elasticsearchx: invalid concrete index name")
		}
	}
	return nil
}
