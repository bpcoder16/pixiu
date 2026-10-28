package bootstrap

import (
	"context"
	"fmt"
	"io/fs"
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

// SeedOptions 指定启动时的种子声明及独立执行期限。
type SeedOptions struct {
	// Files 提供可信种子 SQL，通常使用项目的 embed.FS。
	Files fs.FS
	// Seeds 按顺序执行；为空时跳过种子阶段。
	Seeds []schemamigrate.Seed
	// Timeout 限制整批种子执行时间；零值默认 2 分钟，负数非法。
	Timeout time.Duration
}

// RunMySQLMigration 从配置目录严格读取 migrate.<name>.yaml，在命名 MySQL 的主库执行迁移策略。
// enabled 默认 false；关闭或空清单不获取客户端。timeout 默认 5 分钟，负数非法。
// 调用方在 MustBaseInit 后、业务服务启动前显式调用，错误和部分执行报告交回项目。
// 本入口内部创建启动期 Context，并通过配置限制整体迁移时间。
func RunMySQLMigration(name string, tables []schemamigrate.Table) (schemamigrate.Report, error) {
	return RunMySQLInitialization(name, tables, SeedOptions{})
}

// RunMySQLInitialization 先执行表结构迁移，再在同一主库校准种子数据。
// 沿用 migrate.<name>.yaml 的 enabled 同时控制两阶段；关闭时不获取数据库或读取 SQL。
// 配置 timeout 只限制结构迁移；种子使用 SeedOptions.Timeout，从种子阶段开始独立计时。
// tables 为空时仍可对已有表执行种子；任一阶段失败均立即返回，不应继续启动业务。
// 返回的 Report 仅记录表结构迁移结果；种子错误保留文件、行号（适用时）及底层错误链。
func RunMySQLInitialization(name string, tables []schemamigrate.Table, seeds SeedOptions) (schemamigrate.Report, error) {
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
	if seeds.Timeout < 0 {
		return schemamigrate.Report{}, fmt.Errorf("bootstrap: negative seed timeout")
	}
	if !cfg.Enabled {
		return schemamigrate.Report{}, nil
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	masterDB := func(ctx context.Context) *gorm.DB {
		return mysqlx.Named(name).MasterDB(ctx)
	}
	report, err := schemamigrate.Apply(ctx, masterDB, tables)
	cancel()
	if err != nil {
		return report, fmt.Errorf("bootstrap: MySQL migration %q: %w", name, err)
	}
	if len(seeds.Seeds) == 0 {
		return report, nil
	}
	if seeds.Timeout == 0 {
		seeds.Timeout = 2 * time.Minute
	}
	seedCtx, seedCancel := context.WithTimeout(context.Background(), seeds.Timeout)
	defer seedCancel()
	if seedErr := schemamigrate.ApplySeeds(seedCtx, masterDB, seeds.Files, seeds.Seeds); seedErr != nil {
		return report, fmt.Errorf("bootstrap: MySQL seeds %q: %w", name, seedErr)
	}
	return report, nil
}
