//go:build !pixiu_bootstrap_no_mysql || !pixiu_bootstrap_no_pgsql

package bootstrap

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"time"

	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/env"
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

// 两个数据库入口共用配置、期限和迁移顺序，仅由调用方提供对应主库。
func runSQLInitialization(kind, name string, tables []schemamigrate.Table, seeds SeedOptions, masterDB func(context.Context) *gorm.DB) (schemamigrate.Report, error) {
	if err := validateInstanceName(kind+" migration", name); err != nil {
		return schemamigrate.Report{}, err
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
	report, err := schemamigrate.Apply(ctx, masterDB, tables)
	cancel()
	if err != nil {
		return report, fmt.Errorf("bootstrap: %s migration %q: %w", kind, name, err)
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
		return report, fmt.Errorf("bootstrap: %s seeds %q: %w", kind, name, seedErr)
	}
	return report, nil
}
