package bootstrap_test

import (
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/internal/baseconfig"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func TestMustBaseInitRejectsMissingArguments(t *testing.T) {
	for _, name := range []string{"config", "resources"} {
		t.Run(name, func(t *testing.T) {
			cfg := &baseconfig.AppConfig{}
			resources := &lifecycle.Stack{}
			switch name {
			case "config":
				cfg = nil
			case "resources":
				resources = nil
			}
			defer func() {
				if value := recover(); value == nil {
					t.Fatal("缺少启动参数应 panic")
				}
			}()
			bootstrap.MustBaseInit(cfg, resources)
		})
	}
}
