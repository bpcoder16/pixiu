package natsx

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// namespaceValue 仅用于已校验的原始值；空 queue、Name 和过滤条件保持其原有语义。
// 不猜测调用方是否已加前缀，原始名称或主题本身也可能以 namespace 开头。
func namespaceValue(prefix, value string) string {
	if prefix == "" || value == "" {
		return value
	}
	return prefix + value
}

func namespaceSubjects(prefix string, subjects []string) []string {
	if prefix == "" || len(subjects) == 0 {
		return subjects
	}
	// 注册声明可跨客户端复用，不能在调用方的底层切片上追加前缀。
	result := slices.Clone(subjects)
	for i, subject := range result {
		result[i] = prefix + subject
	}
	return result
}

// 下划线可以出现在 namespace 和逻辑名内；绑定整条流前须核对服务端主题，不能仅信任物理名称。
func (c *Client) checkUnfilteredStreams(ctx context.Context, specs []JetStreamConsumer) error {
	checked := make(map[string]bool)
	namespace := strings.TrimSuffix(c.subjectPrefix, ".")
	for _, spec := range specs {
		if spec.Config.FilterSubject != "" || len(spec.Config.FilterSubjects) > 0 || checked[spec.Stream] {
			continue
		}
		stream, err := c.js.Stream(ctx, spec.Stream)
		if err != nil {
			return fmt.Errorf("natsx: inspect stream %q for namespace %q: %w", spec.Stream, namespace, err)
		}
		cfg := stream.CachedInfo().Config
		if len(cfg.Subjects) == 0 || cfg.Mirror != nil || len(cfg.Sources) > 0 || cfg.SubjectTransform != nil {
			return fmt.Errorf("natsx: stream %q requires an explicit consumer filter for namespace %q", spec.Stream, namespace)
		}
		for _, subject := range cfg.Subjects {
			if !strings.HasPrefix(subject, c.subjectPrefix) {
				return fmt.Errorf("natsx: stream %q subject %q is outside namespace %q; specify a consumer filter", spec.Stream, subject, namespace)
			}
		}
		checked[spec.Stream] = true
	}
	return nil
}
