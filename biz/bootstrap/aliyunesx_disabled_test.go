//go:build pixiu_bootstrap_no_aliyun_es

package bootstrap_test

import (
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
)

func TestBootstrapWithoutAliyunES(t *testing.T) {
	assertBootstrapModuleDisabled(t, "pixiu_bootstrap_no_aliyun_es", func() {
		bootstrap.MustRegisterAliyunES("disabled", true)
	},
		"github.com/bpcoder16/pixiu/infra/aliyun/esx",
		"github.com/alibabacloud-go/elasticsearch-20170613/",
	)
}
