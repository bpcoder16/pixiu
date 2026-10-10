package bootstrap

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/lifecycle"
)

type instanceRegistration struct {
	name      string
	isDefault bool
}

// instanceGroup 复用同类实例的声明、配置加载与生命周期流程。
// 各组件提供文件配置转换和客户端构造函数；实例身份始终来自声明。
type instanceGroup[FileConfig, Config, Client any] struct {
	kind          string
	registrations []instanceRegistration
	configure     func(*FileConfig, string) (Config, error)
	newDefault    func(Config) (Client, error)
	newNamed      func(Config) (Client, error)
	closeAll      func() error
}

func (g *instanceGroup[FileConfig, Config, Client]) mustRegister(name string, isDefault bool) {
	if baseInitStarted {
		panic(fmt.Errorf("bootstrap: initialization has started; cannot register %s %q", g.kind, name))
	}
	if name == "" {
		panic(fmt.Errorf("bootstrap: invalid %s name %q", g.kind, name))
	}
	for _, ch := range name {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-') {
			panic(fmt.Errorf("bootstrap: invalid %s name %q: only ASCII letters, digits, '_' and '-' are allowed", g.kind, name))
		}
	}
	for _, registered := range g.registrations {
		if registered.name == name {
			panic(fmt.Errorf("bootstrap: %s %q is already registered", g.kind, name))
		}
		if isDefault && registered.isDefault {
			panic(fmt.Errorf("bootstrap: default %s %q is already registered; cannot register %q as default", g.kind, registered.name, name))
		}
	}
	g.registrations = append(g.registrations, instanceRegistration{
		name:      name,
		isDefault: isDefault,
	})
}

func (g *instanceGroup[FileConfig, Config, Client]) configPath(name string) string {
	return filepath.Join(env.ConfigDirPath(), strings.ToLower(g.kind)+"."+name+".yaml")
}

func (g *instanceGroup[FileConfig, Config, Client]) initialize(resources *lifecycle.Stack) error {
	if len(g.registrations) == 0 {
		return nil
	}
	// 同类实例的全部文件先解析并转换，避免后续配置错误时已创建连接。
	// 转换阶段的语义校验由各组件决定，构造阶段仍由底层客户端完成自身校验。
	configs := make([]Config, len(g.registrations))
	for i, registration := range g.registrations {
		path := g.configPath(registration.name)
		cfg, err := configx.Parse[FileConfig](path)
		if err != nil {
			return fmt.Errorf("load instance %q from %q: %w", registration.name, path, err)
		}
		configs[i], err = g.configure(cfg, registration.name)
		if err != nil {
			return fmt.Errorf("configure instance %q from %q: %w", registration.name, path, err)
		}
	}
	// 每类组件只登记一次关闭，部分初始化失败时由应用栈回收此前成功的实例。
	if err := resources.Register(g.closeAll); err != nil {
		return fmt.Errorf("register CloseAll: %w", err)
	}
	for i, cfg := range configs {
		registration := g.registrations[i]
		var err error
		if registration.isDefault {
			_, err = g.newDefault(cfg)
		} else {
			_, err = g.newNamed(cfg)
		}
		if err != nil {
			return fmt.Errorf("initialize instance %q from %q: %w", registration.name, g.configPath(registration.name), err)
		}
	}
	return nil
}
