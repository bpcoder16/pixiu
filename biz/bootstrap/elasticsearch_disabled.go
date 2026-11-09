//go:build pixiu_bootstrap_no_es

package bootstrap

import (
	"fmt"

	"github.com/bpcoder16/pixiu/lifecycle"
)

// MustRegisterElasticsearch 在禁用 Elasticsearch 装配的构建中立即 panic，避免遗漏显式声明的实例。
func MustRegisterElasticsearch(name string, isDefault bool) {
	panic(fmt.Errorf("bootstrap: cannot register Elasticsearch instance %q with build tag pixiu_bootstrap_no_es", name))
}

func initElasticsearch(*lifecycle.Stack) error {
	return nil
}
