package logit

import "time"

// 下游日志的标准字段名，调用方也可直接用普通字段构造器写入。
const (
	DownstreamTypeKey       = "downstream_type"
	DownstreamDurationMSKey = "downstream_duration_ms"
	DownstreamIDKey         = "downstream_id"
	DownstreamDetailsKey    = "downstream_details"
)

// DownstreamFields 构造统一的下游调用字段。
// kind 为技术类型，id 为稳定的业务目标标识，duration 以毫秒数值输出，
// details 为自定义详情；nil details 输出空对象。
func DownstreamFields(kind, id string, duration time.Duration, details map[string]any) []Field {
	if details == nil {
		details = map[string]any{}
	}
	return []Field{
		Str(DownstreamTypeKey, kind),
		Dur(DownstreamDurationMSKey, duration),
		Str(DownstreamIDKey, id),
		Any(DownstreamDetailsKey, details),
	}
}
