//go:build pixiu_bootstrap_no_aliyun_sts

package bootstrap_test

import (
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
)

func TestBootstrapWithoutAliyunSTS(t *testing.T) {
	assertBootstrapModuleDisabled(t, "pixiu_bootstrap_no_aliyun_sts", func() {
		bootstrap.MustRegisterAliyunSTS("disabled", true)
	},
		"github.com/bpcoder16/pixiu/infra/aliyun/stsx",
		"github.com/alibabacloud-go/sts-20150401/",
	)
}
