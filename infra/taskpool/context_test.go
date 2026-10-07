package taskpool_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/taskpool"
	"github.com/bpcoder16/pixiu/logit"
)

func TestTaskContextPreservesLogIDAcrossQueueAndRetry(t *testing.T) {
	p, err := taskpool.New(taskpool.Config{
		MinWorkers: 1,
		MaxWorkers: 1,
		QueueSize:  2,
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Shutdown()
	var buf bytes.Buffer
	l := logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(&buf)))
	defer logit.Close(l)
	type key struct{}
	parent, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	first := logit.WithContextLogID(context.WithValue(parent, key{}, "first"))
	second := logit.WithContextLogID(context.WithValue(context.Background(), key{}, "second"))
	firstID, _ := logit.LogIDFromContext(first)
	secondID, _ := logit.LogIDFromContext(second)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	for _, source := range []context.Context{first, second} {
		name := source.Value(key{}).(string)
		wantID, _ := logit.LogIDFromContext(source)
		attempt := 0
		if err := p.Submit(source, name, func(ctx context.Context) error {
			<-release
			attempt++
			if id, _ := logit.LogIDFromContext(ctx); id != wantID || ctx.Value(key{}) != name {
				t.Errorf("任务 %s 的 context 值丢失或串任务: id=%q, value=%v", name, id, ctx.Value(key{}))
			}
			if _, ok := ctx.Deadline(); ok || ctx.Err() != nil || context.Cause(ctx) != nil {
				t.Error("提交方的截止时间或取消影响了已接收任务")
			}
			l.Info(ctx, name, logit.Int("attempt", attempt))
			if name == "first" && attempt == 1 {
				return errors.New("retry")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	close(release)
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte{'\n'})
	if len(lines) != 3 {
		t.Fatalf("预期两次执行及一次重试，日志为 %s", buf.String())
	}
	for i, line := range lines {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		want := firstID
		if i == 2 {
			want = secondID
		}
		if record[logit.LogId] != want {
			t.Fatalf("生产与任务日志 ID 不一致: %s", line)
		}
	}
}

func TestTaskContextAndChildIgnorePoolCancellation(t *testing.T) {
	p, err := taskpool.New(taskpool.Config{
		MinWorkers:   1,
		MaxWorkers:   1,
		QueueSize:    1,
		DrainTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Shutdown()
	source, cancel := context.WithCancelCause(logit.WithContextLogID(context.Background()))
	defer cancel(nil)
	wantID, _ := logit.LogIDFromContext(source)
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	defer func() {
		close(release)
		_ = p.Shutdown()
		_ = p.Wait()
	}()
	if err := p.Submit(source, "cancel", func(ctx context.Context) error {
		if ctx.Done() != nil {
			t.Error("回调 context 不应带有取消信号")
		}
		child, stop := context.WithCancel(ctx)
		defer stop()
		started <- child
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var child context.Context
	select {
	case child = <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("任务未启动")
	}
	cancel(errors.New("request ended"))
	if child.Err() != nil || context.Cause(child) != nil {
		t.Fatal("提交方取消影响了任务子 context")
	}
	if err := p.Shutdown(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("预期排空超时: %v", err)
	}
	if child.Err() != nil || context.Cause(child) != nil {
		t.Fatalf("池取消影响了回调子 context: %v, %v", child.Err(), context.Cause(child))
	}
	if id, _ := logit.LogIDFromContext(child); id != wantID {
		t.Fatalf("取消后日志 ID 丢失: %q", id)
	}
}

func TestTaskContextsIsolateLogFieldsAndGenerateIDs(t *testing.T) {
	var buf bytes.Buffer
	l := logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(&buf)))
	defer logit.Close(l)
	p, err := taskpool.New(taskpool.Config{
		MinWorkers: 2,
		MaxWorkers: 2,
		QueueSize:  4,
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	source := logit.WithContextLogID(context.Background())
	logit.AddMeta(source, logit.Str("parent_meta", "parent"))
	logit.AddField(source, logit.Str("parent_field", "parent"))
	wantID, _ := logit.LogIDFromContext(source)
	empty := logit.WithContext(context.Background())
	logit.AddMeta(empty, logit.Str(logit.LogId, ""))
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		_ = p.Shutdown()
		_ = p.Wait()
	}()
	for _, tc := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "first", ctx: source},
		{name: "second", ctx: source},
		{name: "missing", ctx: context.Background()},
		{name: "empty", ctx: empty},
	} {
		attempt := 0
		if err := p.Submit(tc.ctx, tc.name, func(ctx context.Context) error {
			<-release
			attempt++
			if attempt == 1 {
				logit.AddMeta(ctx, logit.Str("meta_"+tc.name, "own"))
				logit.AddField(ctx, logit.Str("field_"+tc.name, "own"))
			}
			l.Info(ctx, tc.name)
			if tc.name == "missing" && attempt == 1 {
				return errors.New("retry")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	logit.AddMeta(source, logit.Str(logit.LogId, "changed-after-submit"))
	logit.AddField(source, logit.Str("parent_after", "parent"))
	close(release)
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
	l.Info(source, "parent")
	ids := make(map[string]string)
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte{'\n'})
	if len(lines) != 6 {
		t.Fatalf("日志数量错误: %s", buf.String())
	}
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		name := record["msg"].(string)
		id, _ := record[logit.LogId].(string)
		if name == "parent" {
			if id != "changed-after-submit" || record["parent_meta"] != "parent" || record["parent_field"] != "parent" {
				t.Fatalf("父级日志被修改: %s", line)
			}
		} else {
			if id == "" || record["parent_meta"] != nil || record["parent_field"] != nil || record["parent_after"] != nil {
				t.Fatalf("任务 ID 缺失或继承了父级日志字段: %s", line)
			}
			if (name == "first" || name == "second") && id != wantID {
				t.Fatalf("未保存提交时的 ID: %s", line)
			}
			if previous := ids[name]; previous != "" && previous != id {
				t.Fatalf("重试时 ID 改变: %s", line)
			}
			ids[name] = id
			if record["meta_"+name] != "own" || record["field_"+name] != "own" {
				t.Fatalf("任务字段未保留: %s", line)
			}
		}
		for _, other := range []string{"first", "second", "missing", "empty"} {
			if other != name && (record["meta_"+other] != nil || record["field_"+other] != nil) {
				t.Fatalf("日志字段跨任务或污染父级: %s", line)
			}
		}
	}
	if ids["missing"] == ids["empty"] || len(ids["missing"]) != 36 || len(ids["empty"]) != 36 {
		t.Fatalf("缺失或空 ID 未各自生成: %v", ids)
	}
}
