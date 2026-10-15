package estextsearch_test

import (
	"encoding/json"
	"fmt"

	"github.com/bpcoder16/pixiu/infra/estextsearch"
)

func ExampleQuery() {
	clause, err := estextsearch.Query("query_text", "apple PR-PV02")
	if err != nil {
		panic(err)
	}
	boolQuery := map[string]any{
		"filter": []any{
			map[string]any{"term": map[string]any{"merchant_user_id": "10001"}},
		},
	}
	if clause != nil {
		boolQuery["must"] = []any{clause}
	}
	body, err := json.Marshal(map[string]any{
		"query": map[string]any{"bool": boolQuery},
		"size":  20,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(json.Valid(body))
	// Output: true
}
