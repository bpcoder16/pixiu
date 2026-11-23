package elasticsearchx_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/elasticsearchx"
	v7 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v7"
)

// 将 testdata/synonyms 只读挂载到测试节点的 config/pixiu-synonym-validation。
func TestValidateSynonymsOnElasticsearch7(t *testing.T) {
	endpoint := strings.TrimRight(os.Getenv("PIXIU_ES7_SYNONYMS_TEST_URL"), "/")
	if endpoint == "" {
		t.Skip("未配置 PIXIU_ES7_SYNONYMS_TEST_URL")
	}
	client, err := v7.New(elasticsearchx.Config{
		Name:      "synonym-validation",
		Addresses: []string{endpoint},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	for _, tc := range []struct {
		file      string
		causeType string
	}{
		{file: "valid.txt"},
		{file: "invalid.txt", causeType: "parse_exception"},
		{file: "missing.txt", causeType: "no_such_file_exception"},
	} {
		t.Run(tc.file, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			result, err := client.ValidateSynonyms(ctx, elasticsearchx.SynonymValidationOptions{
				SynonymsPath: "pixiu-synonym-validation/" + tc.file,
			})
			if tc.causeType == "" {
				if err != nil || !result.Valid || result.Stage != elasticsearchx.SynonymStageComplete || result.Cleanup != elasticsearchx.SynonymCleanupSucceeded || result.MayExist {
					t.Fatalf("合法词库未通过验证或未成功清理: %+v err=%v", result, err)
				}
			} else {
				var httpErr *elasticsearchx.HTTPError
				if !errors.As(err, &httpErr) || httpErr.Status != http.StatusBadRequest || result.Valid || result.Stage != elasticsearchx.SynonymStageCreate {
					t.Fatalf("预期 ES 创建索引时拒绝词库: %+v err=%v", result, err)
				}
				if httpErr.CausedBy == nil || httpErr.CausedBy.Type != tc.causeType {
					t.Fatalf("词库错误原因 = %+v，预期 %s", httpErr.CausedBy, tc.causeType)
				}
			}

			// 通过节点实际状态核对索引是否存在，独立于客户端返回的 MayExist。
			if result.Index == "" {
				t.Fatal("缺少可核查的临时索引名")
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodHead, endpoint+"/"+result.Index, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("HEAD %s = %s，预期 404", result.Index, resp.Status)
			}
		})
	}
}
