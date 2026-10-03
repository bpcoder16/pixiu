package elasticSearchx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/bpcoder16/pixiu/jsonx"
)

// bulkKind 指定服务端的批量操作语义。
type bulkKind string

const (
	bulkIndex  bulkKind = "index"  // 新增或整篇覆盖。
	bulkCreate bulkKind = "create" // 仅新增，ID 已存在时失败。
	bulkUpdate bulkKind = "update" // 部分更新，默认不插入缺失文档。
	bulkDelete bulkKind = "delete" // 删除，文档已不存在也视为完成。
)

// VersionType 指定 index/delete 动作的外部版本比较方式。
type VersionType string

const (
	VersionExternal    VersionType = "external"
	VersionExternalGTE VersionType = "external_gte"
)

// BulkAction 表示一个批量动作，通过 NewBulkIndex、NewBulkCreate、NewBulkDelete
// 或三个 update 构造函数创建；零值不可用，所有动作均要求显式 ID。
// index/delete 可通过 WithExternalVersion 设置外部版本。
// 构造函数只组装动作，必要参数由 Bulk 在发送前统一校验。
type BulkAction struct {
	kind            bulkKind
	id              string
	document        any
	updateMode      bulkUpdateMode
	initialDocument any

	version            int64
	versionType        VersionType
	hasExternalVersion bool
}

type bulkUpdateMode uint8

const (
	bulkUpdateOnly        bulkUpdateMode = iota // 仅部分更新，文档不存在时失败。
	bulkUpdateDocAsUpsert                       // 部分更新，文档不存在时插入同一份 document。
	bulkUpdateWithInitial                       // 部分更新，文档不存在时插入独立的 initialDocument。
)

// NewBulkIndex 构造文档写入动作；文档不存在时新增，存在时整篇覆盖。
func NewBulkIndex(id string, document any) BulkAction {
	return BulkAction{
		kind:     bulkIndex,
		id:       id,
		document: document,
	}
}

// NewBulkCreate 构造仅新增动作；文档 ID 已存在时失败。
func NewBulkCreate(id string, document any) BulkAction {
	return BulkAction{
		kind:     bulkCreate,
		id:       id,
		document: document,
	}
}

// NewBulkDelete 构造删除动作；文档已不存在也视为完成。
func NewBulkDelete(id string) BulkAction {
	return BulkAction{
		kind: bulkDelete,
		id:   id,
	}
}

// NewBulkUpdate 构造部分更新动作；文档不存在时失败。
func NewBulkUpdate(id string, patch any) BulkAction {
	return BulkAction{
		kind:       bulkUpdate,
		id:         id,
		document:   patch,
		updateMode: bulkUpdateOnly,
	}
}

// NewBulkUpsert 构造部分更新动作；文档不存在时插入同一份 document。
func NewBulkUpsert(id string, document any) BulkAction {
	action := NewBulkUpdate(id, document)
	action.updateMode = bulkUpdateDocAsUpsert
	return action
}

// NewBulkUpsertWithInitial 构造部分更新动作；存在时更新 patch，不存在时插入 initialDocument。
// patch 和 initialDocument 均须提供，缺失时由 Bulk 返回错误。
func NewBulkUpsertWithInitial(id string, patch, initialDocument any) BulkAction {
	action := NewBulkUpdate(id, patch)
	// 单独记录模式，确保缺失插入文档时不会静默退化成普通更新。
	action.updateMode = bulkUpdateWithInitial
	action.initialDocument = initialDocument
	return action
}

// WithExternalVersion 返回设置外部版本后的动作副本，不修改原动作，也不深拷贝文档。
// 仅适用于 index/delete；版本号须非负，versionType 须为 VersionExternal 或 VersionExternalGTE。
// 参数由 Bulk 在发送前统一校验。
func (a BulkAction) WithExternalVersion(version int64, versionType VersionType) BulkAction {
	a.version = version
	a.versionType = versionType
	// 区分未设置与显式传入零值参数，避免无效参数被静默忽略。
	a.hasExternalVersion = true
	return a
}

// BulkFailure 保留失败项供调用方判断是否重试或忽略。
type BulkFailure struct {
	ID     string
	Status int
	Type   string
	Reason string
}

// BulkResult 汇总服务端逐项操作结果；Succeeded 包含删除时文档已不存在的动作。
type BulkResult struct {
	Succeeded int
	Failures  []BulkFailure
}

// BulkError 表示 HTTP 成功但至少一个批量动作失败。
type BulkError struct{ Failed int }

func (e *BulkError) Error() string {
	return fmt.Sprintf("elasticSearchx: %d bulk actions failed", e.Failed)
}

// Bulk 执行一批动作；部分成功时同时返回结果和 BulkError。
func (c *Client) Bulk(ctx context.Context, index string, actions []BulkAction) (BulkResult, error) {
	if len(actions) == 0 {
		return BulkResult{}, nil
	}
	body, err := encodeBulk(actions)
	if err != nil {
		return BulkResult{}, err
	}
	var result BulkResult
	op := operation{
		name:  "bulk",
		index: index,
		bulk:  &result,
	}
	err = c.request(ctx, http.MethodPost, "_bulk", op, body, "application/x-ndjson", func(reader io.Reader) error {
		var err error
		result, err = decodeBulk(reader, actions)
		return err
	})
	return result, err
}

func encodeBulk(actions []BulkAction) ([]byte, error) {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, action := range actions {
		if action.id == "" {
			return nil, errors.New("elasticSearchx: bulk action requires document ID")
		}
		switch action.kind {
		case bulkIndex, bulkCreate, bulkUpdate:
			if action.document == nil {
				return nil, errors.New("elasticSearchx: bulk action requires document")
			}
		case bulkDelete:
		default:
			return nil, errors.New("elasticSearchx: invalid bulk action kind")
		}
		if action.updateMode == bulkUpdateWithInitial && action.initialDocument == nil {
			return nil, errors.New("elasticSearchx: bulk upsert requires initial document")
		}
		if action.hasExternalVersion {
			if (action.kind != bulkIndex && action.kind != bulkDelete) || action.version < 0 || (action.versionType != VersionExternal && action.versionType != VersionExternalGTE) {
				return nil, errors.New("elasticSearchx: invalid bulk external version")
			}
		}
		metadata := map[string]any{"_id": action.id}
		if action.hasExternalVersion {
			metadata["version"] = action.version
			metadata["version_type"] = action.versionType
		}
		if err := encoder.Encode(map[bulkKind]any{action.kind: metadata}); err != nil {
			return nil, fmt.Errorf("elasticSearchx: encode bulk metadata: %w", err)
		}
		switch action.kind {
		case bulkDelete:
			// delete 只有元数据行，额外写入文档行会破坏后续动作的 NDJSON 边界。
			continue
		case bulkUpdate:
			update := map[string]any{"doc": action.document}
			switch action.updateMode {
			case bulkUpdateOnly:
				// 普通更新只发送 doc，不附带插入选项。
			case bulkUpdateDocAsUpsert:
				update["doc_as_upsert"] = true
			case bulkUpdateWithInitial:
				update["upsert"] = action.initialDocument
			default:
				return nil, errors.New("elasticSearchx: invalid bulk update mode")
			}
			if err := encoder.Encode(update); err != nil {
				return nil, fmt.Errorf("elasticSearchx: encode bulk update: %w", err)
			}
		default:
			if err := encoder.Encode(action.document); err != nil {
				return nil, fmt.Errorf("elasticSearchx: encode bulk document: %w", err)
			}
		}
	}
	return body.Bytes(), nil
}

func decodeBulk(reader io.Reader, actions []BulkAction) (BulkResult, error) {
	var payload struct {
		Errors *bool `json:"errors"`
		Items  []map[bulkKind]struct {
			ID     string `json:"_id"`
			Status int    `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := jsonx.DecodeOne(reader, &payload); err != nil {
		return BulkResult{}, fmt.Errorf("elasticSearchx: decode bulk response: %w", err)
	}
	if payload.Errors == nil {
		err := errors.New("elasticSearchx: bulk errors flag missing")
		return BulkResult{}, err
	}
	if len(payload.Items) != len(actions) {
		err := errors.New("elasticSearchx: bulk item count mismatch")
		return BulkResult{}, err
	}
	result := BulkResult{}
	for i, item := range payload.Items {
		value, ok := item[actions[i].kind]
		if !ok || value.Status == 0 {
			err := errors.New("elasticSearchx: malformed bulk item")
			return BulkResult{}, err
		}
		succeeded := value.Status >= 200 && value.Status < 300
		// delete 的 404 在无 error 时视为已完成，不依赖 result 的具体值。
		if actions[i].kind == bulkDelete && value.Status == http.StatusNotFound {
			succeeded = true
		}
		if succeeded && value.Error == nil {
			result.Succeeded++
			continue
		}
		failure := BulkFailure{ID: value.ID, Status: value.Status}
		if failure.ID == "" {
			failure.ID = actions[i].id
		}
		if value.Error != nil {
			failure.Type, failure.Reason = value.Error.Type, value.Error.Reason
		}
		result.Failures = append(result.Failures, failure)
	}
	if *payload.Errors != (len(result.Failures) > 0) {
		err := errors.New("elasticSearchx: inconsistent bulk response")
		return result, err
	}
	if len(result.Failures) != 0 {
		err := &BulkError{Failed: len(result.Failures)}
		return result, err
	}
	return result, nil
}
