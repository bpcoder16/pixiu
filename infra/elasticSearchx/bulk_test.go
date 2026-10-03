package elasticSearchx

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestBulkOfficialActionsUseNDJSON(t *testing.T) {
	const wantBody = "{\"index\":{\"_id\":\"1\"}}\n{\"value\":1}\n" +
		"{\"delete\":{\"_id\":\"2\"}}\n" +
		"{\"create\":{\"_id\":\"3\"}}\n{\"value\":3}\n" +
		"{\"update\":{\"_id\":\"1\"}}\n{\"doc\":{\"value\":4}}\n"
	calls := 0
	c := &Client{
		name:           "search",
		durationPrefix: "elasticSearch_search",
		slowThreshold:  time.Hour,
		performer: performerFunc(func(req *http.Request) (*http.Response, error) {
			calls++
			body, err := io.ReadAll(req.Body)
			if err != nil || string(body) != wantBody {
				t.Errorf("混合动作的 NDJSON 错误: body=%q err=%v", body, err)
			}
			return jsonResponse(req, http.StatusOK, `{"errors":false,"items":[{"index":{"_id":"1","status":201}},{"delete":{"_id":"2","status":200,"result":"deleted"}},{"create":{"_id":"3","status":201}},{"update":{"_id":"1","status":200}}]}`), nil
		}),
	}
	result, err := c.Bulk(context.Background(), "products", []BulkAction{
		NewBulkIndex("1", map[string]int{"value": 1}),
		NewBulkDelete("2"),
		NewBulkCreate("3", map[string]int{"value": 3}),
		NewBulkUpdate("1", map[string]int{"value": 4}),
	})
	if err != nil || result.Succeeded != 4 || len(result.Failures) != 0 || calls != 1 {
		t.Fatalf("四种动作未正确执行: result=%+v calls=%d err=%v", result, calls, err)
	}
}

func TestBulkUpdateDoesNotInsertByDefault(t *testing.T) {
	c := &Client{
		name:           "search",
		durationPrefix: "elasticSearch_search",
		slowThreshold:  time.Hour,
		performer: performerFunc(func(req *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(req.Body)
			const want = "{\"update\":{\"_id\":\"1\"}}\n{\"doc\":{\"value\":1}}\n"
			if err != nil || string(body) != want {
				t.Errorf("普通 update 不应隐式插入: body=%q err=%v", body, err)
			}
			return jsonResponse(req, http.StatusOK, `{"errors":true,"items":[{"update":{"_id":"1","status":404,"error":{"type":"document_missing_exception","reason":"missing"}}}]}`), nil
		}),
	}
	result, err := c.Bulk(context.Background(), "products", []BulkAction{
		NewBulkUpdate("1", map[string]int{"value": 1}),
	})
	var bulkErr *BulkError
	if !errors.As(err, &bulkErr) || bulkErr.Failed != 1 || result.Succeeded != 0 || len(result.Failures) != 1 || result.Failures[0].Type != "document_missing_exception" {
		t.Fatalf("文档缺失应保留为 update 失败: result=%+v err=%v", result, err)
	}
}

func TestBulkDeleteNotFound(t *testing.T) {
	for _, tt := range []struct {
		name     string
		response string
		failed   bool
	}{
		{
			name:     "文档已不存在",
			response: `{"errors":false,"items":[{"delete":{"_id":"1","status":404,"result":"not_found"}}]}`,
		},
		{
			name:     "索引不存在",
			response: `{"errors":true,"items":[{"delete":{"_id":"1","status":404,"error":{"type":"index_not_found_exception","reason":"missing index"}}}]}`,
			failed:   true,
		},
		{
			name:     "not_found 但携带错误",
			response: `{"errors":true,"items":[{"delete":{"_id":"1","status":404,"result":"not_found","error":{"type":"failure","reason":"failed"}}}]}`,
			failed:   true,
		},
		{
			name:     "404 无错误且省略 result",
			response: `{"errors":false,"items":[{"delete":{"_id":"1","status":404}}]}`,
		},
		{
			name:     "404 无错误且 result 使用其他值",
			response: `{"errors":false,"items":[{"delete":{"_id":"1","status":404,"result":"unknown_result"}}]}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				name:           "search",
				durationPrefix: "elasticSearch_search",
				slowThreshold:  time.Hour,
				performer: performerFunc(func(req *http.Request) (*http.Response, error) {
					return jsonResponse(req, http.StatusOK, tt.response), nil
				}),
			}
			result, err := c.Bulk(context.Background(), "products", []BulkAction{
				NewBulkDelete("1"),
			})
			if tt.failed {
				var bulkErr *BulkError
				if !errors.As(err, &bulkErr) || bulkErr.Failed != 1 || result.Succeeded != 0 || len(result.Failures) != 1 || result.Failures[0].Status != 404 {
					t.Fatalf("删除失败被误判: result=%+v err=%v", result, err)
				}
			} else if err != nil || result.Succeeded != 1 || len(result.Failures) != 0 {
				t.Fatalf("删除不存在的文档不应作为失败项: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestBulkUpdateUpsertAndDeleteExternalVersion(t *testing.T) {
	const wantBody = "{\"update\":{\"_id\":\"1\"}}\n{\"doc\":{\"value\":2},\"upsert\":{\"name\":\"first\",\"value\":2}}\n" +
		"{\"delete\":{\"_id\":\"2\",\"version\":12,\"version_type\":\"external_gte\"}}\n"
	c := &Client{
		name:           "search",
		durationPrefix: "elasticSearch_search",
		slowThreshold:  time.Hour,
		performer: performerFunc(func(req *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(req.Body)
			if err != nil || string(body) != wantBody {
				t.Errorf("upsert 或外部版本删除编码错误: body=%q err=%v", body, err)
			}
			return jsonResponse(req, http.StatusOK, `{"errors":false,"items":[{"update":{"_id":"1","status":201,"result":"created"}},{"delete":{"_id":"2","status":200,"result":"deleted"}}]}`), nil
		}),
	}
	versioned := NewBulkDelete("2").WithExternalVersion(12, VersionExternalGTE)
	result, err := c.Bulk(context.Background(), "products", []BulkAction{
		NewBulkUpsertWithInitial("1", map[string]int{"value": 2}, map[string]any{"name": "first", "value": 2}),
		versioned,
	})
	if err != nil || result.Succeeded != 2 || len(result.Failures) != 0 {
		t.Fatalf("显式 upsert 或删除失败: result=%+v err=%v", result, err)
	}
}

func TestBulkWithExternalVersionReturnsCopy(t *testing.T) {
	for _, tt := range []struct {
		name     string
		action   BulkAction
		body     string
		response string
	}{
		{
			name:   "index",
			action: NewBulkIndex("1", map[string]int{"value": 1}),
			body: "{\"index\":{\"_id\":\"1\"}}\n{\"value\":1}\n" +
				"{\"index\":{\"_id\":\"1\",\"version\":12,\"version_type\":\"external\"}}\n{\"value\":1}\n" +
				"{\"index\":{\"_id\":\"1\",\"version\":13,\"version_type\":\"external_gte\"}}\n{\"value\":1}\n",
			response: `{"errors":false,"items":[{"index":{"_id":"1","status":201}},{"index":{"_id":"1","status":200}},{"index":{"_id":"1","status":200}}]}`,
		},
		{
			name:   "delete",
			action: NewBulkDelete("1"),
			body: "{\"delete\":{\"_id\":\"1\"}}\n" +
				"{\"delete\":{\"_id\":\"1\",\"version\":12,\"version_type\":\"external\"}}\n" +
				"{\"delete\":{\"_id\":\"1\",\"version\":13,\"version_type\":\"external_gte\"}}\n",
			response: `{"errors":false,"items":[{"delete":{"_id":"1","status":200}},{"delete":{"_id":"1","status":404,"result":"not_found"}},{"delete":{"_id":"1","status":404,"result":"not_found"}}]}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				name:           "search",
				durationPrefix: "elasticSearch_search",
				slowThreshold:  time.Hour,
				performer: performerFunc(func(req *http.Request) (*http.Response, error) {
					body, err := io.ReadAll(req.Body)
					if err != nil || string(body) != tt.body {
						t.Errorf("版本设置污染了原动作或编码错误: body=%q err=%v", body, err)
					}
					return jsonResponse(req, http.StatusOK, tt.response), nil
				}),
			}
			original := tt.action
			versioned := original.WithExternalVersion(12, VersionExternal)
			next := versioned.WithExternalVersion(13, VersionExternalGTE)
			result, err := c.Bulk(context.Background(), "products", []BulkAction{original, versioned, next})
			if err != nil || result.Succeeded != 3 || len(result.Failures) != 0 {
				t.Fatalf("动作副本执行失败: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestBulkRejectsInvalidActionsBeforeSending(t *testing.T) {
	doc := map[string]int{"value": 1}
	unknownUpdateMode := NewBulkUpdate("1", doc)
	unknownUpdateMode.updateMode = bulkUpdateMode(255)

	type testCase struct {
		name   string
		action BulkAction
	}
	tests := []testCase{
		{
			name:   "零值动作",
			action: BulkAction{},
		},
		{
			name:   "未知更新模式不能降级为普通更新",
			action: unknownUpdateMode,
		},
		{
			name:   "delete 缺失 ID",
			action: NewBulkDelete(""),
		},
		{
			name:   "独立插入文档缺失不能降级为普通更新",
			action: NewBulkUpsertWithInitial("1", doc, nil),
		},
		{
			name:   "版本缺失比较方式",
			action: NewBulkDelete("1").WithExternalVersion(1, ""),
		},
		{
			name:   "零版本未提供比较方式也不能静默忽略",
			action: NewBulkIndex("1", doc).WithExternalVersion(0, ""),
		},
		{
			name:   "负外部版本",
			action: NewBulkDelete("1").WithExternalVersion(-1, VersionExternal),
		},
		{
			name:   "未知版本比较方式",
			action: NewBulkIndex("1", doc).WithExternalVersion(1, "unknown"),
		},
		{
			name:   "create 不接受外部版本",
			action: NewBulkCreate("1", doc).WithExternalVersion(1, VersionExternal),
		},
		{
			name:   "update 不接受外部版本",
			action: NewBulkUpdate("1", doc).WithExternalVersion(1, VersionExternal),
		},
		{
			name:   "同内容 upsert 不接受外部版本",
			action: NewBulkUpsert("1", doc).WithExternalVersion(1, VersionExternal),
		},
		{
			name:   "独立文档 upsert 不接受外部版本",
			action: NewBulkUpsertWithInitial("1", doc, doc).WithExternalVersion(1, VersionExternal),
		},
	}
	for _, constructor := range []struct {
		name string
		new  func(string, any) BulkAction
	}{
		{
			name: "覆盖写入",
			new:  NewBulkIndex,
		},
		{
			name: "仅新增",
			new:  NewBulkCreate,
		},
		{
			name: "普通更新",
			new:  NewBulkUpdate,
		},
		{
			name: "同内容插入",
			new:  NewBulkUpsert,
		},
		{
			name: "独立文档插入",
			new: func(id string, patch any) BulkAction {
				return NewBulkUpsertWithInitial(id, patch, doc)
			},
		},
	} {
		tests = append(tests, testCase{
			name:   constructor.name + " 构造后缺失 ID",
			action: constructor.new("", doc),
		}, testCase{
			name:   constructor.name + " 构造后缺失文档",
			action: constructor.new("1", nil),
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				name:           "search",
				durationPrefix: "elasticSearch_search",
				performer: performerFunc(func(req *http.Request) (*http.Response, error) {
					t.Error("无效批次不应发送请求")
					return jsonResponse(req, http.StatusOK, `{}`), nil
				}),
			}
			result, err := c.Bulk(context.Background(), "products", []BulkAction{
				NewBulkIndex("valid", doc),
				tt.action,
			})
			if err == nil || result.Succeeded != 0 || len(result.Failures) != 0 {
				t.Fatalf("参数校验失败: result=%+v err=%v", result, err)
			}
		})
	}
}
