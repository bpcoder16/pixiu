package baseconfig

import "time"

// LogConfig 定义默认及命名日志配置，由 bootstrap 校验并应用默认值。
// 固定使用 rotatefile；Format 必须显式设置，Dir 零值使用启动工作目录下的 log 目录。
type LogConfig struct {
	Format string `mapstructure:"format"`
	// Caller 控制调用位置输出；默认关闭。
	Caller bool `mapstructure:"caller"`
	// HookStd 控制 stdout/stderr 劫持；nil 默认开启，显式 false 关闭。
	HookStd *bool `mapstructure:"hookStd"`
	// Dir 是日志根目录；相对路径基于 env.RootDirPath()，不存在时由 bootstrap 创建。
	// 业务日志位于 <appName>/biz，标准流日志位于 <appName>/std。
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
