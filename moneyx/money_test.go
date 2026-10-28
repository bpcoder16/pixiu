package moneyx_test

import (
	"errors"
	"math"
	"testing"

	"github.com/bpcoder16/pixiu/moneyx"
)

func TestYuanToCent(t *testing.T) {
	for _, tc := range []struct {
		yuan string
		cent int64
	}{
		{"0", 0},
		{"0.00", 0},
		{"0.01", 1},
		{"0.1", 10},
		{"1", 100},
		{"1.2", 120},
		{"12.34", 1234},
		{"00012.30", 1230},
		{"0000000000000000000000000000000000000000.01", 1},
		{"-0", 0},
		{"-0.00", 0},
		{"-000.0", 0},
		{"-0.01", -1},
		{"-0.1", -10},
		{"-1", -100},
		{"-1.2", -120},
		{"-12.34", -1234},
		{"-00012.30", -1230},
		{"-0000000000000000000000000000000000000000.01", -1},
		{"92233720368547758", 9223372036854775800},
		{"92233720368547758.0", 9223372036854775800},
		{"92233720368547758.07", math.MaxInt64},
		{"-92233720368547758", -9223372036854775800},
		{"-92233720368547758.0", -9223372036854775800},
		{"-92233720368547758.08", math.MinInt64},
	} {
		t.Run(tc.yuan, func(t *testing.T) {
			got, err := moneyx.YuanToCent(tc.yuan)
			if err != nil || got != tc.cent {
				t.Fatalf("YuanToCent(%q) = (%d, %v), want (%d, nil)", tc.yuan, got, err, tc.cent)
			}
		})
	}
}

func TestYuanToCentErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		err    error
	}{
		{
			name: "format",
			values: []string{
				"", ".", ".5", "1.", "1.001", "1.000", "1.2.3", "１.００",
				"-", "-.", "-.5", "-1.", "-1.001", "-1.000", "-1.2.3",
			},
			err: moneyx.ErrInvalidFormat,
		},
		{
			name: "character",
			values: []string{
				" ", " 1", "1 ", "+1", "1e2", "NaN", "1,000", "１", "1.a", "1..", "1\n", "1\x00", "\xff",
				"--1", "-+1", "+-1", "1-1", "1.-1", "- 1", " -1", "-1 ", "−1", "-１", "-1e2",
			},
			err: moneyx.ErrInvalidCharacter,
		},
		{
			name: "overflow",
			values: []string{
				"92233720368547758.08", "92233720368547758.1", "92233720368547759", "9223372036854775808",
				"-92233720368547758.09", "-92233720368547758.1", "-92233720368547759", "-9223372036854775808",
				"184467440737095516.15", "184467440737095516.16", "18446744073709551616",
			},
			err: moneyx.ErrOverflow,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, value := range tc.values {
				got, err := moneyx.YuanToCent(value)
				if got != 0 || !errors.Is(err, tc.err) {
					t.Errorf("YuanToCent(%q) = (%d, %v), want (0, %v)", value, got, err, tc.err)
				}
			}
		})
	}
}

func TestCentToYuan(t *testing.T) {
	for _, tc := range []struct {
		cent int64
		yuan string
	}{
		{0, "0.00"},
		{1, "0.01"},
		{10, "0.10"},
		{99, "0.99"},
		{100, "1.00"},
		{101, "1.01"},
		{1234, "12.34"},
		{-1, "-0.01"},
		{-10, "-0.10"},
		{-99, "-0.99"},
		{-100, "-1.00"},
		{-101, "-1.01"},
		{-1234, "-12.34"},
		{math.MaxInt64, "92233720368547758.07"},
		{math.MinInt64, "-92233720368547758.08"},
	} {
		t.Run(tc.yuan, func(t *testing.T) {
			if got := moneyx.CentToYuan(tc.cent); got != tc.yuan {
				t.Errorf("CentToYuan(%d) = %q, want %q", tc.cent, got, tc.yuan)
			}
		})
	}
}

func FuzzCentToYuanRoundTrip(f *testing.F) {
	for _, cent := range []int64{0, 1, -1, 99, -99, 100, -100, 101, -101, 1234, -1234, math.MaxInt64, math.MinInt64} {
		f.Add(cent)
	}
	f.Fuzz(func(t *testing.T, cent int64) {
		yuan := moneyx.CentToYuan(cent)
		got, err := moneyx.YuanToCent(yuan)
		if err != nil || got != cent {
			t.Fatalf("round trip %d -> %q -> (%d, %v)", cent, yuan, got, err)
		}
	})
}
