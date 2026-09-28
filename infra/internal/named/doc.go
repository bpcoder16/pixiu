// Package named 提供 infra 内部共用的可关闭客户端命名注册表。
// 模块保留自己的 NewNamed、Named 和 CloseAll 对外入口，并负责名称校验。
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
// 注册表在启动阶段可并发创建；同名并发构造只登记一个，未登记的新客户端会关闭。
// 所有创建返回后再开始按名称查询；关闭前须先停止使用客户端的任务。
// 创建不得与 CloseAll 并发。
package named
