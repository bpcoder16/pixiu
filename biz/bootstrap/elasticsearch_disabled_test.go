//go:build pixiu_bootstrap_no_es

package bootstrap_test

import (
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
)

func TestBootstrapWithoutElasticsearch(t *testing.T) {
	assertBootstrapModuleDisabled(t, "pixiu_bootstrap_no_es", func() {
		bootstrap.MustRegisterElasticsearch("disabled", true)
	},
		"github.com/bpcoder16/pixiu/infra/elasticsearchx",
		"github.com/elastic/go-elasticsearch/",
	)
}
