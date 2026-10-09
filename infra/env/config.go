package env

import (
	"errors"
	"fmt"
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
// LocalIP 非空时校验 IP 格式，空值通过 netx 查询本机 IPv4。
// ConfigDirPath 由调用方提供，必须是已存在且可读、可遍历的绝对目录，原样保存。
// 该字段不从配置文件解析，文件自身权限在实际读取时检查。
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
	root, err := os.OpenRoot(c.ConfigDirPath)
	if err != nil {
		return nil, "", fmt.Errorf("invalid env.configDirPath %q: %w", c.ConfigDirPath, err)
	}
	defer func() {
		if closeErr := root.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close env.configDirPath root %q: %w", c.ConfigDirPath, closeErr))
			location = nil
			localIP = ""
		}
	}()
	// 在目录内打开自身，验证当前进程的读取及遍历权限，而非仅检查权限位。
	dir, err := root.Open(".")
	if err != nil {
		return nil, "", fmt.Errorf("cannot access env.configDirPath %q: %w", c.ConfigDirPath, err)
	}
	defer func() {
		if closeErr := dir.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close env.configDirPath directory %q: %w", c.ConfigDirPath, closeErr))
			location = nil
			localIP = ""
		}
	}()
	localIP = c.LocalIP
	if localIP == "" {
		localIP, err = netx.LocalIPv4()
		if err != nil {
			return nil, "", fmt.Errorf("detect env.localIP: %w", err)
		}
	} else if net.ParseIP(localIP) == nil {
		return nil, "", fmt.Errorf("invalid env.localIP %q: must be an IPv4 or IPv6 address", localIP)
	}
	return location, localIP, nil
}
