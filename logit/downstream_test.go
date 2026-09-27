package logit

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestDownstreamFieldsJSONAndText(t *testing.T) {
	details := map[string]any{"method": "POST", "status": 200}
	fields := DownstreamFields("httpcall", "wechat", 12*time.Millisecond+345*time.Microsecond, details)
	wantKeys := []string{DownstreamTypeKey, DownstreamDurationMSKey, DownstreamIDKey, DownstreamDetailsKey}
	if len(fields) != len(wantKeys) {
		t.Fatalf("下游字段数量 = %d, want %d", len(fields), len(wantKeys))
	}
	for i, want := range wantKeys {
		if fields[i].Key != want {
			t.Errorf("下游字段 %d = %q, want %q", i, fields[i].Key, want)
		}
	}

	jsonLogger, jsonBuf := newTestLogger(t, OptEncoder(DefaultJSONEncoder))
	jsonLogger.Info(context.Background(), "downstream call", fields...)
	var record map[string]any
	if err := json.Unmarshal(jsonBuf.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record[DownstreamTypeKey] != "httpcall" || record[DownstreamDurationMSKey] != 12.345 || record[DownstreamIDKey] != "wechat" {
		t.Fatalf("下游字段值: %v", record)
	}
	gotDetails, ok := record[DownstreamDetailsKey].(map[string]any)
	if !ok || gotDetails["method"] != "POST" || gotDetails["status"] != float64(200) {
		t.Fatalf("扩展信息不是预期对象: %v", record[DownstreamDetailsKey])
	}

	textLogger, textBuf := newTestLogger(t)
	textLogger.Info(context.Background(), "downstream call", fields...)
	for _, want := range []string{
		"downstream_type=[httpcall]",
		"downstream_duration_ms=[12.345]",
		"downstream_id=[wechat]",
		"downstream_details=[",
	} {
		if !strings.Contains(textBuf.String(), want) {
			t.Errorf("文本日志缺少 %q: %q", want, textBuf.String())
		}
	}
}

func TestDownstreamFieldsNormalizeNilDetails(t *testing.T) {
	fields := DownstreamFields("mysql", "orders", 0, nil)
	l, buf := newTestLogger(t, OptEncoder(DefaultJSONEncoder))
	l.Info(context.Background(), "db call", fields...)
	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if details, ok := record[DownstreamDetailsKey].(map[string]any); !ok || len(details) != 0 {
		t.Fatalf("nil details 应输出空对象: %v", record[DownstreamDetailsKey])
	}
}

func TestDownstreamKeysAreRegularFields(t *testing.T) {
	l, buf := newTestLogger(t, OptEncoder(DefaultJSONEncoder))
	l = l.With(Str(DownstreamTypeKey, "httpcall"))
	ctx := WithContext(context.Background())
	AddMeta(ctx, Dur(DownstreamDurationMSKey, time.Millisecond))
	AddField(ctx, Str(DownstreamIDKey, "wechat"))
	AddDebugField(ctx, Str(DownstreamTypeKey, "debug-only"))
	l.Info(ctx, "downstream call", Any(DownstreamDetailsKey, map[string]any{"method": "GET"}))

	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record[DownstreamTypeKey] != "httpcall" ||
		record[DownstreamDurationMSKey] != float64(1) ||
		record[DownstreamIDKey] != "wechat" {
		t.Fatalf("标准字段未按普通字段输出: %v", record)
	}
	details, ok := record[DownstreamDetailsKey].(map[string]any)
	if !ok || details["method"] != "GET" {
		t.Fatalf("调用点详情字段未输出: %v", record[DownstreamDetailsKey])
	}

	other, otherBuf := newTestLogger(t, OptEncoder(DefaultJSONEncoder))
	other.Info(context.Background(), "custom downstream field", Str(DownstreamDurationMSKey, "legacy"))
	if err := json.Unmarshal(otherBuf.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record[DownstreamDurationMSKey] != "legacy" {
		t.Fatalf("标准字段不应强制类型: %v", record)
	}
}
