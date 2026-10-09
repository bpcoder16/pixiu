// Package configx 提供 YAML、TOML、JSON 文件的结构体解析与全局命名配置。
//
// 依赖 Viper，统一使用 mapstructure 标签；未知字段通过 UnmarshalExact 报错，
// 缺失字段保留零值，禁用弱类型转换；字符串转 int/bool、bool 转 int 等会报错。
// 保留 Viper 默认的时长和逗号分隔切片解码 hook。JSON 保留数字精度再映射。
// JSON 数字不经过字符串 hook，不隐式转为字符串；整数时长按纳秒解析。
// Viper 的键名不区分大小写；不隐式执行业务校验或填充默认值。
// 结构体映射保留源数据中的 null、空对象和字面键名，不经点号路径展开。
//
// 以下示例需导入 github.com/bpcoder16/pixiu/infra/configx：
//
//	type ServerConfig struct {
//	    Addr string `mapstructure:"addr"`
//	    Port int    `mapstructure:"port"`
//	}
//	if err := configx.Load[ServerConfig]("server", "./conf/server.yaml"); err != nil {
//	    return err
//	}
//	cfg, err := configx.Get[ServerConfig]("server")
//	if err != nil {
//	    return err
//	}
//	_ = cfg.Port
//	optional := configx.GetOrZero[ServerConfig]("optional")
//	_ = optional.Port
//
// Load 的 T 必须是结构体。文件后缀忽略大小写，支持 .yaml、.yml、.toml、.json；
// 相对路径基于工作目录。加载失败不注册，成功后同名配置不得覆盖或重新加载。
// Get 不重新读取文件，每次返回同一共享指针；GetOrZero 获取失败时返回 new(T)，
// 不占用名称，不与其他兜底对象共享数据，map、slice 和指针字段保持 nil。
//
// 注册和查询并发安全，配置内容不保证不可变；共享指针的并发读写由调用方协调。
// 建议启动阶段完成加载和必要调整，业务组件获取一次后保存指针。
// 不提供配置合并、环境变量覆盖、热更新、文件写回或 JSON5 支持。
package configx
