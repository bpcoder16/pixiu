package common_test

import (
	"errors"
	"math"
	"testing"

	"github.com/bpcoder16/pixiu/biz/common"
)

func TestFormatPercent(t *testing.T) {
	for _, tc := range []struct {
		value int64
		want  string
	}{
		{0, "0%"},
		{1, "0.01%"},
		{10, "0.1%"},
		{100, "1%"},
		{1000, "10%"},
		{1001, "10.01%"},
		{1010, "10.1%"},
		{1021, "10.21%"},
		{10000, "100%"},
		{math.MaxUint32, "42949672.95%"},
		{-1, "-0.01%"},
		{-10, "-0.1%"},
		{-100, "-1%"},
		{-1000, "-10%"},
		{-1001, "-10.01%"},
		{-1010, "-10.1%"},
		{math.MaxInt64, "92233720368547758.07%"},
		{math.MinInt64, "-92233720368547758.08%"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			if got := common.FormatPercent(tc.value); got != tc.want {
				t.Fatalf("FormatPercent(%d) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}

func TestParsePercent(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int64
	}{
		{"0%", 0},
		{"0.01%", 1},
		{"0.1%", 10},
		{"1%", 100},
		{"10%", 1000},
		{"10.00%", 1000},
		{"10.01%", 1001},
		{"10.1%", 1010},
		{"10.21%", 1021},
		{"100%", 10000},
		{"100.01%", 10001},
		{"00010.01%", 1001},
		{"-0.01%", -1},
		{"-0.1%", -10},
		{"-10%", -1000},
		{"-10.01%", -1001},
		{"-00010.10%", -1010},
		{"-0%", 0},
		{"-0.00%", 0},
		{"92233720368547758.07%", math.MaxInt64},
		{"-92233720368547758.08%", math.MinInt64},
	} {
		t.Run(tc.value, func(t *testing.T) {
			got, err := common.ParsePercent(tc.value)
			if err != nil || got != tc.want {
				t.Fatalf("ParsePercent(%q) = %d, %v; want %d, nil", tc.value, got, err, tc.want)
			}
		})
	}
}

func TestParsePercentInvalid(t *testing.T) {
	for _, value := range []string{
		"", "%", "10", "10.00", "-%", ".1%", "-.1%", "1.%", "1.001%", "1.000%",
		" 10%", "10% ", "10 %", "10\n%", "+10%", "--1%", "1e2%", "1,000%",
		"１０%", "10％", "NaN%", "Inf%", "1.2.3%", "1%%", "%1%",
	} {
		t.Run(value, func(t *testing.T) {
			got, err := common.ParsePercent(value)
			if got != 0 || !errors.Is(err, common.ErrInvalidPercent) {
				t.Fatalf("ParsePercent(%q) = %d, %v; want 0, ErrInvalidPercent", value, got, err)
			}
		})
	}

	for _, value := range []string{
		"92233720368547758.08%", "-92233720368547758.09%",
		"92233720368547759%", "-92233720368547759%", "999999999999999999999999%",
	} {
		t.Run(value, func(t *testing.T) {
			got, err := common.ParsePercent(value)
			if got != 0 || !errors.Is(err, common.ErrPercentOverflow) {
				t.Fatalf("ParsePercent(%q) = %d, %v; want 0, ErrPercentOverflow", value, got, err)
			}
		})
	}
}

func FuzzPercentRoundTrip(f *testing.F) {
	for _, value := range []int64{0, 1, -1, 10, -10, 1000, -1001, math.MinInt64, math.MaxInt64} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value int64) {
		formatted := common.FormatPercent(value)
		got, err := common.ParsePercent(formatted)
		if err != nil || got != value {
			t.Fatalf("round trip %d through %q = %d, %v", value, formatted, got, err)
		}
	})
}
