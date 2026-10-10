package baseconfig

import "time"

// LogConfig 定义默认及命名日志配置，由 bootstrap 校验并应用默认值。
// 固定使用 rotatefile；Format 必须显式设置，Dir 零值使用启动工作目录下的 log 目录。
type LogConfig struct {
	Format string `mapstructure:"format"`
	// Caller 控制调用位置输出；默认关闭。
	Caller bool `mapstructure:"caller"`
	// Dir 是日志目录；相对路径基于 env.RootDirPath()，不存在时由 bootstrap 创建。
	Dir string `mapstructure:"dir"`
	// Names 是额外命名 Logger 的名称；默认 Logger 始终创建。
	Names  []string        `mapstructure:"names"`
	Rotate LogRotateConfig `mapstructure:"rotate"`
}

// LogRotateConfig 定义日志文件的轮转配置。
// Every 为 0 时默认 1h，支持 1h/24h；MaxFiles 为 0 时默认 48，否则至少为 3。
type LogRotateConfig struct {
	Every    time.Duration `mapstructure:"every"`
	MaxFiles int           `mapstructure:"maxFiles"`
}
