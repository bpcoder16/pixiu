package jsonx_test

import (
	"fmt"
	"strings"

	"github.com/bpcoder16/pixiu/jsonx"
)

func ExampleDecodeOne() {
	var value struct {
		Count int `json:"count"`
	}
	reader := strings.NewReader(`{"count":42}`)
	if err := jsonx.DecodeOne(reader, &value); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(value.Count)
	// Output: 42
}
