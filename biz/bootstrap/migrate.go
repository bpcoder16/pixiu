package bootstrap

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/mysqlx"
	"github.com/bpcoder16/pixiu/infra/schemamigrate"
	"gorm.io/gorm"
)

type migrationConfig struct {
	Enabled bool          `mapstructure:"enabled"`
	Timeout time.Duration `mapstructure:"timeout"`
}

// RunMySQLMigration 从配置目录严格读取 migrate.<name>.yaml，在命名 MySQL 的主库执行迁移策略。
// enabled 默认 false；关闭或空清单不获取客户端。timeout 默认 5 分钟，负数非法。
// 调用方在 MustBaseInit 后、业务服务启动前显式调用，错误和部分执行报告交回项目。
// 本入口内部创建启动期 Context，并通过配置限制整体迁移时间。
func RunMySQLMigration(name string, tables []schemamigrate.Table) (schemamigrate.Report, error) {
	if name == "" || strings.ContainsAny(name, `/\.`) {
		return schemamigrate.Report{}, fmt.Errorf("bootstrap: invalid MySQL migration name %q", name)
	}
	for _, ch := range name {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-') {
			return schemamigrate.Report{}, fmt.Errorf("bootstrap: invalid MySQL migration name %q", name)
		}
	}
	file := filepath.Join(env.ConfigDirPath(), "migrate."+name+".yaml")
	cfg, err := configx.Parse[migrationConfig](file)
	if err != nil {
		return schemamigrate.Report{}, fmt.Errorf("bootstrap: load migration config: %w", err)
	}
	if cfg.Timeout < 0 {
		return schemamigrate.Report{}, fmt.Errorf("bootstrap: negative migration timeout in %q", file)
	}
	if !cfg.Enabled {
		return schemamigrate.Report{}, nil
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	report, err := schemamigrate.Apply(ctx, func(ctx context.Context) *gorm.DB {
		return mysqlx.Named(name).MasterDB(ctx)
	}, tables)
	if err != nil {
		return report, fmt.Errorf("bootstrap: MySQL migration %q: %w", name, err)
	}
	return report, nil
}
