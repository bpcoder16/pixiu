//go:build pixiu_bootstrap_no_aliyun_oss

package bootstrap_test

import (
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
)

func TestBootstrapWithoutAliyunOSS(t *testing.T) {
	assertBootstrapModuleDisabled(t, "pixiu_bootstrap_no_aliyun_oss", func() {
		bootstrap.MustRegisterAliyunOSS("disabled", true)
	},
		"github.com/bpcoder16/pixiu/infra/aliyun/ossx",
		"github.com/aliyun/alibabacloud-oss-go-sdk-v2/",
	)
}
