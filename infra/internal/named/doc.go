// Package named 提供 infra 内部共用的可关闭客户端命名与默认注册表。
// 模块保留自己的 NewNamed、Named、NewDefault、Default 和 CloseAll 对外入口，
// 并负责名称校验。
// 例如模块内部可按下列方式登记实现 io.Closer 的客户端：
//
//	registry := named.New[io.Closer]("example")
//	client, err := registry.Create("input", func() (io.Closer, error) {
//	    return io.NopCloser(strings.NewReader("data")), nil
//	})
//	if err != nil {
//	    return err
//	}
//	_ = client
//	return registry.CloseAll()
//
// 上例需导入 io、strings 和 github.com/bpcoder16/pixiu/infra/internal/named。
// 注册表在启动阶段由调用方串行初始化，不并发调用 Create 或 CreateDefault。
// 所有创建返回后再开始按名称查询；关闭前须先停止使用客户端的任务。
// 创建不得与 CloseAll 并发。
//
// 默认客户端通过 CreateDefault 显式初始化；同一实例也可按名称查询：
//
//	registry := named.New[io.Closer]("example")
//	_, err := registry.CreateDefault("input", func() (io.Closer, error) {
//	    return io.NopCloser(strings.NewReader("data")), nil
//	})
//	if err != nil {
//	    return err
//	}
//	_ = registry.MustDefault()
//	_ = registry.MustGet("input")
//	return registry.CloseAll()
//
// 默认创建由调用方串行执行，重复初始化在构造前报错；失败可重试。
// 默认实例仅是命名实例的别名，CloseAll 不重复关闭。默认未初始化或关闭后
// 查询会 panic；Create 不自动设置默认实例，关闭后不得重新初始化。
package named
