package env

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

type environment struct {
	appName       string
	runMode       string
	location      *time.Location
	configDirPath string
	localIP       string
	rootDirPath   string
}

var defaultEnv atomic.Pointer[environment]

// ErrAlreadyInitialized 表示进程环境已由某个启动入口初始化，不能重复发布。
var ErrAlreadyInitialized = errors.New("env: environment is already initialized")

// Init 校验配置并一次性发布进程环境，供 HTTP、命令行等启动入口复用。
// ConfigDirPath 必须为已存在且当前进程可列举、可遍历的绝对目录，校验后原样保存。
// 不清理路径，不要求目录可写，不递归检查子目录或文件内容读取权限。失败不改变已有环境。
// LocalIP 非空时校验格式，空值通过 netx 先查询本机 IPv4，失败后查询 IPv6；均失败时保持为空。
// RootDirPath 自动保存 Init 时的工作目录绝对路径；获取失败返回错误。
// 并发调用仅一次成功；成功后再次调用返回 ErrAlreadyInitialized。不修改 time.Local。
func Init(cfg Config) error {
	if defaultEnv.Load() != nil {
		return ErrAlreadyInitialized
	}
	location, localIP, err := cfg.validate()
	if err != nil {
		return err
	}
	// 固定启动入口的工作目录，后续 os.Chdir 不改变环境快照。
	rootDirPath, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("env: resolve rootDirPath: %w", err)
	}
	// 按值保存配置字段；不同启动入口也必须竞争同一个发布点，禁止覆盖。
	next := &environment{
		appName:       cfg.AppName,
		runMode:       cfg.RunMode,
		location:      location,
		configDirPath: cfg.ConfigDirPath,
		localIP:       localIP,
		rootDirPath:   rootDirPath,
	}
	if !defaultEnv.CompareAndSwap(nil, next) {
		return ErrAlreadyInitialized
	}
	return nil
}

func currentEnv() *environment {
	env := defaultEnv.Load()
	if env == nil {
		panic("env: environment is not initialized; call Init first")
	}
	return env
}

// AppName 返回启动时的应用名称；环境尚未初始化时 panic。
func AppName() string { return currentEnv().appName }

// RunMode 返回启动时的运行模式；环境尚未初始化时 panic。
func RunMode() string { return currentEnv().runMode }

// TimeLocation 返回解析后的应用时区，不修改进程时区；环境尚未初始化时 panic。
func TimeLocation() *time.Location { return currentEnv().location }

// ConfigDirPath 返回初始化时传入的配置目录原值；环境尚未初始化时 panic。
func ConfigDirPath() string { return currentEnv().configDirPath }

// LocalIP 返回初始化时指定或查询得到的 IP 快照；IPv4、IPv6 查询均失败时为空，环境尚未初始化时 panic。
func LocalIP() string { return currentEnv().localIP }

// RootDirPath 返回 Init 时的工作目录绝对路径快照；环境尚未初始化时 panic。
func RootDirPath() string { return currentEnv().rootDirPath }
