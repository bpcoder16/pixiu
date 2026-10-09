package env

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/netx"
)

// 应用支持的运行模式，由具体启动流程决定各模式的行为。
const (
	RunModeDebug   = "debug"
	RunModeTest    = "test"
	RunModeRelease = "release"
)

// Config 定义必填的应用名称、运行模式和时区，以及可选的 LocalIP。
// LocalIP 非空时校验 IP 格式，空值通过 netx 先查询本机 IPv4，失败后查询 IPv6，均失败时保持为空。
// ConfigDirPath 由调用方提供，必须是已存在且当前进程可列举、可遍历的绝对目录，原样保存。
// 该字段不从配置文件解析，不要求目录可写，不递归检查子目录或文件内容读取权限。
type Config struct {
	AppName       string `mapstructure:"appName"`
	RunMode       string `mapstructure:"runMode"`
	TimeLocation  string `mapstructure:"timeLocation"`
	ConfigDirPath string `mapstructure:"-"`
	LocalIP       string `mapstructure:"localIP"`
}

func (c Config) validate() (location *time.Location, localIP string, err error) {
	if strings.TrimSpace(c.AppName) == "" {
		return nil, "", fmt.Errorf("env.appName is required")
	}
	switch c.RunMode {
	case RunModeDebug, RunModeTest, RunModeRelease:
	default:
		return nil, "", fmt.Errorf("env.runMode must be debug, test or release, got %q", c.RunMode)
	}
	// LoadLocation 接受空字符串并返回 UTC，此处显式要求填写时区。
	if strings.TrimSpace(c.TimeLocation) == "" {
		return nil, "", fmt.Errorf("env.timeLocation is required")
	}
	location, err = time.LoadLocation(c.TimeLocation)
	if err != nil {
		return nil, "", fmt.Errorf("invalid env.timeLocation %q: %w", c.TimeLocation, err)
	}
	if !filepath.IsAbs(c.ConfigDirPath) {
		return nil, "", fmt.Errorf("env.configDirPath must be absolute, got %q", c.ConfigDirPath)
	}
	// 末尾的分隔符和 . 要求经过目录自身，验证遍历权限；不能用 Join 清理掉它。
	dir, err := os.Open(c.ConfigDirPath + string(os.PathSeparator) + ".")
	if err != nil {
		return nil, "", fmt.Errorf("invalid env.configDirPath %q: %w", c.ConfigDirPath, err)
	}
	defer func() {
		if closeErr := dir.Close(); closeErr != nil {
			// 保留已有校验错误，关闭失败也不能发布环境。
			err = errors.Join(err, fmt.Errorf("close env.configDirPath %q: %w", c.ConfigDirPath, closeErr))
			location = nil
			localIP = ""
		}
	}()
	// 只试读一个条目验证列举权限，空目录同样有效。
	if _, readErr := dir.Readdirnames(1); readErr != nil && readErr != io.EOF {
		return nil, "", fmt.Errorf("cannot read env.configDirPath %q: %w", c.ConfigDirPath, readErr)
	}
	localIP = c.LocalIP
	if localIP == "" {
		// IP 可选；netx 查询失败返回空字符串，不让网络环境阻止应用启动。
		localIP, err = netx.LocalIPv4()
		if err != nil {
			localIP, _ = netx.LocalIPv6()
		}
	} else if net.ParseIP(localIP) == nil {
		return nil, "", fmt.Errorf("invalid env.localIP %q: must be an IPv4 or IPv6 address", localIP)
	}
	return location, localIP, nil
}
