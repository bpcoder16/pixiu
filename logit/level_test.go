package logit

import (
	"errors"
	"testing"
)

func TestLevelString(t *testing.T) {
	tests := []struct {
		level Level
		want  string
	}{
		{DebugLevel, "DEBUG"},
		{InfoLevel, "INFO"},
		{WarnLevel, "WARN"},
		{ErrorLevel, "ERROR"},
		{FatalLevel, "FATAL"},
		{AllLevels, "ALL"},
		{UnknownLevel, "UNKNOWN"},
		{Level(1 << 7), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.level.String(); got != tt.want {
			t.Errorf("Level(%d).String() = %q, want %q", tt.level, got, tt.want)
		}
	}
}

func TestLevelOrdering(t *testing.T) {
	// 单比特级别必须数值有序,MinLevel 过滤依赖这一点。
	levels := []Level{DebugLevel, InfoLevel, WarnLevel, ErrorLevel, FatalLevel}
	for i := 1; i < len(levels); i++ {
		if levels[i-1] >= levels[i] {
			t.Errorf("level ordering broken: %d >= %d", levels[i-1], levels[i])
		}
	}
}

func TestLevelIs(t *testing.T) {
	if !DebugLevel.Is(DebugLevel) {
		t.Error("DebugLevel should be visible in DebugLevel line")
	}
	if DebugLevel.Is(InfoLevel) {
		t.Error("DebugLevel field should not be visible in InfoLevel line")
	}
	if !AllLevels.Is(DebugLevel) || !AllLevels.Is(FatalLevel) {
		t.Error("AllLevels field should be visible in any line")
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		in      string
		want    Level
		wantErr bool
	}{
		{"debug", DebugLevel, false},
		{"INFO", InfoLevel, false},
		{"Warn", WarnLevel, false},
		{"warning", WarnLevel, false},
		{"ERROR", ErrorLevel, false},
		{"fatal", FatalLevel, false},
		{"notice", UnknownLevel, true},
		{"", UnknownLevel, true},
	}
	for _, tt := range tests {
		got, err := ParseLevel(tt.in)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseLevel(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseLevel(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
	if _, err := ParseLevel("bogus"); err == nil {
		t.Error("bogus should not parse")
	}
	var target *levelParseError
	if !errors.As(error(&levelParseError{}), &target) {
		t.Error("levelParseError should implement error")
	}
}
