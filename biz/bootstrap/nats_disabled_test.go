//go:build pixiu_bootstrap_no_nats

package bootstrap_test

import (
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
)

func TestBootstrapWithoutNATS(t *testing.T) {
	assertBootstrapModuleDisabled(t, "pixiu_bootstrap_no_nats", func() {
		bootstrap.MustRegisterNATS("disabled", true)
	},
		"github.com/bpcoder16/pixiu/infra/natsx",
		"github.com/nats-io/",
	)
}
