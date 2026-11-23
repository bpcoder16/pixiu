package elasticsearchx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	synonymFilter  = "pixiu_validation_synonyms"
	indexAnalyzer  = "pixiu_validation_index"
	searchAnalyzer = "pixiu_validation_search"
)

// SynonymValidationOptions 配置一次通过临时索引执行的同义词验证。
type SynonymValidationOptions struct {
	// SynonymsPath 必填，是 ES 节点上的 Solr 格式词库路径，不是本机文件或 URL。
	SynonymsPath string
	// CleanupTimeout 零值为 5 秒，负值无效；不受主流程 ctx 取消影响。
	CleanupTimeout time.Duration
}

// SynonymValidationStage 表示验证停留的阶段；清理状态单独返回。
type SynonymValidationStage string

const (
	SynonymStagePrepare  SynonymValidationStage = "prepare"
	SynonymStageCreate   SynonymValidationStage = "create"
	SynonymStageComplete SynonymValidationStage = "complete"
)

// SynonymCleanupStatus 表示本次临时索引的清理结果。
type SynonymCleanupStatus string

const (
	SynonymCleanupNotAttempted SynonymCleanupStatus = "not_attempted"
	SynonymCleanupSucceeded    SynonymCleanupStatus = "succeeded"
	SynonymCleanupFailed       SynonymCleanupStatus = "failed"
)

// SynonymValidationResult 即使发生错误也返回；Valid=true 时仍须处理可能的清理错误。
type SynonymValidationResult struct {
	Index string
	// Valid 仅证明词库在固定分析链和执行节点上可加载，不证明集群分发或业务召回效果。
	Valid   bool
	Stage   SynonymValidationStage
	Cleanup SynonymCleanupStatus
	// MayExist 表示本次调用可能遗留索引；不是存在性查询，需结合 Index 人工核查。
	MayExist bool
}

// ValidateSynonyms 通过创建索引严格加载 synonyms_path，确认创建成功后删除索引。
// 分析链固定为 standard → lowercase → synonym_graph。
// 主流程受 ctx 控制；确认创建后总会以独立期限尝试清理，返回前等待清理结束。
// 创建结果不确定时不自动删除；错误保留 ES 原因链及可能的清理失败。
func (c *Client) ValidateSynonyms(ctx context.Context, options SynonymValidationOptions) (result SynonymValidationResult, resultErr error) {
	result.Stage = SynonymStagePrepare
	result.Cleanup = SynonymCleanupNotAttempted
	if c == nil {
		return result, errors.New("elasticsearchx: nil client")
	}
	if ctx == nil {
		return result, errors.New("elasticsearchx: nil context")
	}
	if options.CleanupTimeout < 0 {
		return result, errors.New("elasticsearchx: negative cleanup timeout")
	}
	definition, err := validationIndexDefinition(options.SynonymsPath)
	if err != nil {
		return result, err
	}
	var suffix [16]byte
	// crypto/rand.Read 保证填满切片且不返回错误。
	_, _ = rand.Read(suffix[:])
	index := "pixiu-synonym-validation-" + hex.EncodeToString(suffix[:])
	created, err := c.CreateIndex(ctx, index, definition)
	// request 在派发前发现客户端已关闭时直接返回该错误。传输错误保留“可能遗留”的状态。
	if err == errClientClosed {
		return result, err
	}
	result.Index = index
	result.Stage = SynonymStageCreate
	result.MayExist = true
	// 即使返回响应关闭错误，完整的创建确认仍证明资源归属，须先登记清理再处理 err。
	if created.Acknowledged {
		defer func() {
			timeout := options.CleanupTimeout
			if timeout == 0 {
				timeout = 5 * time.Second
			}
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
			defer cancel()
			cleanupErr := c.DeleteIndex(cleanupCtx, result.Index)
			// 仅单纯的索引缺失可视为已清理；读取、解析和关闭等组合错误不能吞掉。
			if httpErr, ok := cleanupErr.(*HTTPError); ok && httpErr.Status == http.StatusNotFound && httpErr.Type == "index_not_found_exception" {
				cleanupErr = nil
			}
			if cleanupErr != nil {
				result.Cleanup = SynonymCleanupFailed
				resultErr = errors.Join(resultErr, fmt.Errorf("elasticsearchx: cleanup validation index: %w", cleanupErr))
				return
			}
			result.Cleanup = SynonymCleanupSucceeded
			result.MayExist = false
		}()
	}
	if err != nil {
		// 不删除名称冲突的已有索引，亦不通过重试掩盖意外冲突。
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && httpErr.Status == http.StatusBadRequest && httpErr.Type == "resource_already_exists_exception" {
			result.MayExist = false
		}
		return result, fmt.Errorf("elasticsearchx: create validation index: %w", err)
	}
	if !created.Acknowledged {
		return result, errors.New("elasticsearchx: validation index creation not acknowledged")
	}
	if !created.ShardsAcknowledged {
		return result, errors.New("elasticsearchx: validation index shards not ready")
	}
	result.Valid = true
	result.Stage = SynonymStageComplete
	return result, nil
}

func validationIndexDefinition(synonymsPath string) (map[string]any, error) {
	if strings.TrimSpace(synonymsPath) == "" || strings.ContainsRune(synonymsPath, '\x00') {
		return nil, errors.New("elasticsearchx: invalid synonyms path")
	}
	filterDefinitions := map[string]any{
		synonymFilter: map[string]any{
			"type":          "synonym_graph",
			"synonyms_path": synonymsPath,
			"format":        "solr",
			"lenient":       false,
			"updateable":    false,
			"expand":        true,
		},
	}
	analyzers := map[string]any{
		indexAnalyzer: map[string]any{
			"type": "standard",
		},
		searchAnalyzer: map[string]any{
			"type":      "custom",
			"tokenizer": "standard",
			"filter":    []string{"lowercase", synonymFilter},
		},
	}
	analysis := map[string]any{
		"filter":   filterDefinitions,
		"analyzer": analyzers,
	}
	return map[string]any{
		"settings": map[string]any{
			"number_of_shards":                   1,
			"number_of_replicas":                 0,
			"index.write.wait_for_active_shards": "1",
			"analysis":                           analysis,
		},
		"mappings": map[string]any{
			"dynamic": "strict",
			"properties": map[string]any{
				"query_text": map[string]any{
					"type":            "text",
					"analyzer":        indexAnalyzer,
					"search_analyzer": searchAnalyzer,
				},
			},
		},
	}, nil
}
