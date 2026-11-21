package natsx

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ValidateRegistrations 在建立订阅或操作远端资源之前，校验三类声明的本地约束。
// 可在配置装配时重复调用，不修改声明或占用 Client 注册次数。资源冲突和服务端限制由实际注册返回。
func ValidateRegistrations(streams []StreamRegistration, jetStream []JetStreamConsumer, core []CoreConsumer) error {
	if err := validateStreamRegistrations(streams); err != nil {
		return fmt.Errorf("validate streams: %w", err)
	}
	if err := validateJetStreamConsumers(jetStream); err != nil {
		return fmt.Errorf("validate JetStream consumers: %w", err)
	}
	if err := validateCoreConsumers(core); err != nil {
		return fmt.Errorf("validate Core consumers: %w", err)
	}
	// 直接调用 Core 注册入口时，SDK 在订阅时检查主题和组名。整批预检须提前发现这些错误。
	for _, spec := range core {
		if !validSubscriptionSubject(spec.Subject) {
			return fmt.Errorf("natsx: Core consumer %q subject %q: %w", spec.Name, spec.Subject, nats.ErrBadSubject)
		}
		if strings.ContainsAny(spec.Queue, " \t\r\n") {
			return fmt.Errorf("natsx: Core consumer %q queue %q: %w", spec.Name, spec.Queue, nats.ErrBadQueueName)
		}
	}
	return nil
}

// Stream 和 Durable 使用 NATS SDK 的资源命名约束。
const invalidResourceNameChars = ">*. /\\\t\r\n"

func validSubscriptionSubject(subject string) bool {
	// > 匹配后续全部层级，作为独立 token 时必须位于末尾。
	if strings.TrimSpace(subject) != subject {
		return false
	}
	var tailWildcard bool
	for token := range strings.SplitSeq(subject, ".") {
		if tailWildcard || token == "" || strings.ContainsAny(token, " \t\r\n\f\x00") {
			return false
		}
		tailWildcard = token == ">"
	}
	return true
}

func validateStreamRegistrations(specs []StreamRegistration) error {
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		cfg := spec.Config
		switch spec.Mode {
		case "", StreamModeCreate, StreamModeUpsert:
		case StreamModeExisting:
			// 原生配置字段较多，统一检查零值，避免未来新增字段被静默忽略。
			options := cfg
			options.Name = ""
			if !reflect.ValueOf(options).IsZero() {
				return fmt.Errorf("natsx: stream %q existing mode only accepts Config.Name", cfg.Name)
			}
		default:
			return fmt.Errorf("natsx: stream %q mode %q must be one of create, upsert, existing", cfg.Name, spec.Mode)
		}
		if cfg.Name == "" || strings.TrimSpace(cfg.Name) != cfg.Name {
			return fmt.Errorf("natsx: stream name %q must be nonempty and without surrounding whitespace", cfg.Name)
		}
		if strings.ContainsAny(cfg.Name, invalidResourceNameChars) {
			return fmt.Errorf("natsx: stream %q: %w", cfg.Name, jetstream.ErrInvalidStreamName)
		}
		for i, subject := range cfg.Subjects {
			if !validSubscriptionSubject(subject) {
				return fmt.Errorf("natsx: stream %q subjects[%d] %q: %w", cfg.Name, i, subject, jetstream.ErrInvalidSubject)
			}
		}
		if _, exists := seen[cfg.Name]; exists {
			return fmt.Errorf("natsx: duplicate stream %q", cfg.Name)
		}
		seen[cfg.Name] = struct{}{}
	}
	return nil
}

func validateJetStreamConsumers(specs []JetStreamConsumer) error {
	seen := make(map[[2]string]struct{}, len(specs))
	for i, spec := range specs {
		switch spec.Mode {
		case "", ConsumerModeCreate, ConsumerModeUpsert:
		default:
			return fmt.Errorf("natsx: consumer %q/%q mode %q must be one of create, upsert", spec.Stream, spec.Config.Durable, spec.Mode)
		}
		if spec.Stream == "" || strings.TrimSpace(spec.Stream) != spec.Stream ||
			spec.Config.Durable == "" || strings.TrimSpace(spec.Config.Durable) != spec.Config.Durable ||
			spec.Handler == nil {
			return fmt.Errorf("natsx: consumer %d requires nonempty stream and durable without surrounding whitespace, and handler", i)
		}
		if strings.ContainsAny(spec.Stream, invalidResourceNameChars) {
			return fmt.Errorf("natsx: stream %q: %w", spec.Stream, jetstream.ErrInvalidStreamName)
		}
		if strings.ContainsAny(spec.Config.Durable, invalidResourceNameChars) {
			return fmt.Errorf("natsx: consumer %q: %w", spec.Config.Durable, jetstream.ErrInvalidConsumerName)
		}
		if spec.Config.FilterSubject != "" && !validSubscriptionSubject(spec.Config.FilterSubject) {
			return fmt.Errorf("natsx: consumer %q filterSubject: %w", spec.Config.Durable, jetstream.ErrInvalidSubject)
		}
		for j, subject := range spec.Config.FilterSubjects {
			if !validSubscriptionSubject(subject) {
				return fmt.Errorf("natsx: consumer %q filterSubjects[%d] %q: %w", spec.Config.Durable, j, subject, jetstream.ErrInvalidSubject)
			}
		}
		if spec.Config.Name != "" && spec.Config.Name != spec.Config.Durable {
			return fmt.Errorf("natsx: consumer %q name must equal durable", spec.Config.Durable)
		}
		if spec.Config.DeliverSubject != "" {
			return fmt.Errorf("natsx: consumer %q requires pull mode; DeliverSubject must be empty", spec.Config.Durable)
		}
		if spec.Config.AckPolicy != jetstream.AckExplicitPolicy {
			return fmt.Errorf("natsx: consumer %q requires explicit ack", spec.Config.Durable)
		}
		if spec.Config.AckWait <= 0 && len(spec.Config.BackOff) == 0 {
			return fmt.Errorf("natsx: consumer %q requires AckWait or BackOff", spec.Config.Durable)
		}
		if limit := spec.Config.MaxDeliver; limit > 0 && len(spec.Config.BackOff) > limit {
			return fmt.Errorf("natsx: consumer %q/%q BackOff length %d exceeds MaxDeliver %d", spec.Stream, spec.Config.Durable, len(spec.Config.BackOff), limit)
		}
		if spec.Consume.Workers < 0 || spec.Consume.MaxMessages < 0 || spec.RetryDelay < 0 {
			return fmt.Errorf("natsx: consumer %q has negative consumption parameter", spec.Config.Durable)
		}
		if len(spec.Config.BackOff) > 0 && spec.RetryDelay > 0 {
			return fmt.Errorf("natsx: consumer %q cannot combine BackOff with positive RetryDelay", spec.Config.Durable)
		}
		if err := validatePullLimits(spec.Config, registrationConsumeConfig(spec.Consume).MaxMessages); err != nil {
			return err
		}
		key := [2]string{spec.Stream, spec.Config.Durable}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("natsx: duplicate consumer %q/%q", key[0], key[1])
		}
		seen[key] = struct{}{}
	}
	return nil
}

func registrationConsumeConfig(cfg ConsumeConfig) ConsumeConfig {
	if cfg.Workers == 0 {
		cfg.Workers = 1
	}
	if cfg.MaxMessages == 0 {
		cfg.MaxMessages = 2 * cfg.Workers
	}
	return cfg
}

func validateCoreConsumers(consumers []CoreConsumer) error {
	seen := make(map[string]struct{}, len(consumers))
	for i, spec := range consumers {
		if spec.Name == "" || strings.TrimSpace(spec.Name) != spec.Name ||
			spec.Subject == "" || strings.TrimSpace(spec.Subject) != spec.Subject ||
			spec.Handler == nil ||
			strings.TrimSpace(spec.Queue) != spec.Queue ||
			spec.Subscribers < 0 ||
			spec.PendingMessages < 0 ||
			spec.PendingBytes < 0 {
			return fmt.Errorf("natsx: invalid Core consumer %d", i)
		}
		if _, exists := seen[spec.Name]; exists {
			return fmt.Errorf("natsx: duplicate Core consumer %q", spec.Name)
		}
		seen[spec.Name] = struct{}{}
	}
	return nil
}
