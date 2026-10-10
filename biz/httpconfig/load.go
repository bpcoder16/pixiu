package httpconfig

import (
	"fmt"
	"path/filepath"

	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/env"
)

// MustLoadAppConfig 读取并校验启动配置，发布全局环境，返回配置指针，不注册全局配置。
// 相对路径基于工作目录；配置目录取文件绝对路径的父目录。失败或重复加载时 panic。
// 仅用于启动阶段；失败应退出进程，不承诺恢复后重试。不会修改 time.Local 或初始化组件。
func MustLoadAppConfig(filePath string) *AppConfig {
	if filePath == "" {
		panic(fmt.Errorf("httpconfig: empty config file path"))
	}
	absolutePath, err := filepath.Abs(filePath)
	if err != nil {
		panic(fmt.Errorf("httpconfig: resolve config file %q: %w", filePath, err))
	}
	cfg, err := configx.Parse[AppConfig](absolutePath)
	if err != nil {
		panic(fmt.Errorf("httpconfig: load app config %q: %w", absolutePath, err))
	}
	cfg.Env.ConfigDirPath = filepath.Dir(absolutePath)
	if initErr := env.Init(cfg.Env); initErr != nil {
		panic(fmt.Errorf("httpconfig: initialize environment from %q: %w", absolutePath, initErr))
	}
	return cfg
}
