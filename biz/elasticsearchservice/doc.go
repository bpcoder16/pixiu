// Package elasticsearchservice 组织 Elasticsearch 的通用业务流程。
// 当前通过创建并清理临时索引，验证 ES 节点上的 Solr 格式同义词库。
// 使用标准库和 infra/elasticsearchx，不负责文件上传、分发或业务索引 reload。
//
// 下例需导入 context、time 和本包；
// client 为应用已经创建的 *elasticsearchx.Client，service 不关闭它：
//
//	service, err := elasticsearchservice.New(client)
//	if err != nil {
//	    return err
//	}
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	result, err := service.ValidateSynonyms(ctx, elasticsearchservice.ValidationOptions{
//	    SynonymsPath:   "analysis/synonyms.txt",
//	    CleanupTimeout: 5 * time.Second,
//	})
//	if err != nil {
//	    if result.MayExist {
//	        // 保留 result.Index，核查可能的遗留索引。
//	    }
//	    // 即使 result.Valid=true，也必须处理清理失败。
//	    return err
//	}
//	return nil
//
// 分析链固定为 standard → lowercase → synonym_graph，固定 lenient=false、expand=true。
// 创建响应中的 acknowledged 和 shards_acknowledged 均为 true 才验证通过，不额外调用 _analyze。
// SynonymsPath 是 ES 节点上的路径，库不在 Go 进程本机读取文件。
// 验证只证明固定分析链和执行节点可加载，不保证与业务的其他分析链兼容，
// 也不证明词库全节点一致或业务召回符合预期。
//
// 返回的 Stage 表示验证阶段，Cleanup 单独表示清理状态。创建未确认或请求结果未知时，
// 不重试、不自动删除；MayExist=true 时由调用方核查 Index。
// 确认创建后总会尝试清理，清理 context 保留作用域、脱离主流程取消并单独限时。
// 调用方应等待整个方法返回后再关闭客户端和日志。
// 可通过 errors.As(err, &httpErr) 取得 *elasticsearchx.HTTPError，显式读取
// Reason、RootCause、CausedBy 诊断规则行号；错误文本和概要日志不包含这些详情。
package elasticsearchservice
