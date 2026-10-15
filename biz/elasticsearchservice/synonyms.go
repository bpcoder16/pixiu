package elasticsearchservice

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/infra/elasticsearchx"
)

const (
	synonymFilter  = "pixiu_validation_synonyms"
	indexAnalyzer  = "pixiu_validation_index"
	searchAnalyzer = "pixiu_validation_search"
)

// Service 借用 ES 客户端组织通用流程，不创建或关闭客户端，可并发使用。
type Service struct {
	client *elasticsearchx.Client
}

// New 创建 service；应用须在所有调用结束后再关闭传入的客户端。
func New(client *elasticsearchx.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("elasticsearchservice: nil client")
	}
	return &Service{client: client}, nil
}

// ValidationOptions 配置一次通过临时索引执行的同义词验证。
type ValidationOptions struct {
	// SynonymsPath 必填，是 ES 节点上的 Solr 格式词库路径，不是本机文件或 URL。
	SynonymsPath string
	// CleanupTimeout 零值为 5 秒，负值无效；不受主流程 ctx 取消影响。
	CleanupTimeout time.Duration
}

// ValidationStage 表示验证停留的阶段；清理状态单独返回。
type ValidationStage string

const (
	StagePrepare  ValidationStage = "prepare"
	StageCreate   ValidationStage = "create"
	StageComplete ValidationStage = "complete"
)

// CleanupStatus 表示本次临时索引的清理结果。
type CleanupStatus string

const (
	CleanupNotAttempted CleanupStatus = "not_attempted"
	CleanupSucceeded    CleanupStatus = "succeeded"
	CleanupFailed       CleanupStatus = "failed"
)

// ValidationResult 即使发生错误也返回；Valid=true 时仍须处理可能的清理错误。
type ValidationResult struct {
	Index string
	// Valid 仅证明词库在固定分析链和执行节点上可加载，不证明集群分发或业务召回效果。
	Valid   bool
	Stage   ValidationStage
	Cleanup CleanupStatus
	// MayExist 表示本次调用可能遗留索引；不是存在性查询，需结合 Index 人工核查。
	MayExist bool
}

// ValidateSynonyms 通过创建索引严格加载 synonyms_path，确认创建成功后删除索引。
// 分析链固定为 standard → lowercase → synonym_graph。
// 主流程受 ctx 控制；确认创建后总会以独立期限尝试清理，返回前等待清理结束。
// 创建结果不确定时不自动删除；错误保留 ES 原因链及可能的清理失败。
func (s *Service) ValidateSynonyms(ctx context.Context, options ValidationOptions) (result ValidationResult, resultErr error) {
	result.Stage = StagePrepare
	result.Cleanup = CleanupNotAttempted
	if s == nil || s.client == nil {
		return result, errors.New("elasticsearchservice: nil client")
	}
	if ctx == nil {
		return result, errors.New("elasticsearchservice: nil context")
	}
	if options.CleanupTimeout < 0 {
		return result, errors.New("elasticsearchservice: negative cleanup timeout")
	}
	definition, err := validationIndexDefinition(options.SynonymsPath)
	if err != nil {
		return result, err
	}
	var suffix [16]byte
	// crypto/rand.Read 保证填满切片且不返回错误。
	_, _ = rand.Read(suffix[:])
	result.Index = "pixiu-synonym-validation-" + hex.EncodeToString(suffix[:])
	result.Stage = StageCreate
	result.MayExist = true
	created, err := s.client.CreateIndex(ctx, result.Index, definition)
	// 即使返回响应关闭错误，完整的创建确认仍证明资源归属，须先登记清理再处理 err。
	//noinspection GoDfaErrorMayBeNotNil
	if created.Acknowledged {
		defer func() {
			timeout := options.CleanupTimeout
			if timeout == 0 {
				timeout = 5 * time.Second
			}
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
			defer cancel()
			cleanupErr := s.client.DeleteIndex(cleanupCtx, result.Index)
			// 仅单纯的索引缺失可视为已清理；读取、解析和关闭等组合错误不能吞掉。
			if httpErr, ok := cleanupErr.(*elasticsearchx.HTTPError); ok && httpErr.Status == http.StatusNotFound && httpErr.Type == "index_not_found_exception" {
				cleanupErr = nil
			}
			if cleanupErr != nil {
				result.Cleanup = CleanupFailed
				resultErr = errors.Join(resultErr, fmt.Errorf("elasticsearchservice: cleanup validation index: %w", cleanupErr))
				return
			}
			result.Cleanup = CleanupSucceeded
			result.MayExist = false
		}()
	}
	if err != nil {
		// 不删除名称冲突的已有索引，亦不通过重试掩盖意外冲突。
		var httpErr *elasticsearchx.HTTPError
		if errors.As(err, &httpErr) && httpErr.Status == http.StatusBadRequest && httpErr.Type == "resource_already_exists_exception" {
			result.MayExist = false
		}
		return result, fmt.Errorf("elasticsearchservice: create validation index: %w", err)
	}
	if !created.Acknowledged {
		return result, errors.New("elasticsearchservice: validation index creation not acknowledged")
	}
	if !created.ShardsAcknowledged {
		return result, errors.New("elasticsearchservice: validation index shards not ready")
	}
	result.Valid = true
	result.Stage = StageComplete
	return result, nil
}

func validationIndexDefinition(synonymsPath string) (map[string]any, error) {
	if strings.TrimSpace(synonymsPath) == "" || strings.ContainsRune(synonymsPath, '\x00') {
		return nil, errors.New("elasticsearchservice: invalid synonyms path")
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
