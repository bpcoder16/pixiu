package common_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/bpcoder16/pixiu/biz/common"
)

func TestMaskName(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  string
	}{
		{"空值", "", ""},
		{"单个汉字", "张", "张"},
		{"单个英文字母", "A", "A"},
		{"单个表情", "😀", "😀"},
		{"双字符", "张三", "张*"},
		{"双英文字符", "AB", "A*"},
		{"三字符", "张小三", "张*三"},
		{"四字符", "欧阳娜娜", "欧**娜"},
		{"五字符", "卡牌收藏家", "卡***家"},
		{"六字符", "快乐卡牌收藏", "快***藏"},
		{"超长名称", strings.Repeat("名", 10000), "名***名"},
		{"四字节汉字", "𠮷野家", "𠮷*家"},
		{"汉字和表情", "😀收藏家🃏", "😀***🃏"},
		{"不裁剪空白", " 张三 ", " ** "},
		{"单空白", " ", " "},
		{"组合音标按码点计数", "e\u0301", "e*"},
		{"复合表情按码点计数", "👩\u200d💻", "👩*💻"},
		{"合法替换字符", "\uFFFD", "\uFFFD"},
		{"非法首字节", "\xff张三", "*"},
		{"非法中间字节", "张\xff三", "*"},
		{"非法尾字节", "张三\xff", "*"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := common.MaskName(tc.value); got != tc.want {
				t.Fatalf("MaskName(%q) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}

func TestMaskNameCustomSymbol(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		symbols []string
		want    string
	}{
		{"空值保持不变", "", []string{"#"}, ""},
		{"单字符保持不变", "张", []string{"#"}, "张"},
		{"双字符替换尾部", "张三", []string{"#"}, "张#"},
		{"三字符", "张小三", []string{"•"}, "张•三"},
		{"四字符", "欧阳娜娜", []string{"🔒"}, "欧🔒🔒娜"},
		{"五字符及以上", "快乐卡牌收藏", []string{"#"}, "快###藏"},
		{"字符串作为完整符号", "欧阳娜娜", []string{"[隐藏]"}, "欧[隐藏][隐藏]娜"},
		{"空符号使用默认值", "张三", []string{""}, "张*"},
		{"非法符号使用默认值", "张三", []string{"\xff"}, "张*"},
		{"仅取首个符号", "张三", []string{"#", "!"}, "张#"},
		{"非法名称完全隐藏", "张\xff三", []string{"#"}, "#"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := common.MaskName(tc.value, tc.symbols...); got != tc.want {
				t.Fatalf("MaskName(%q, %q) = %q, want %q", tc.value, tc.symbols, got, tc.want)
			}
		})
	}
}

func FuzzMaskName(f *testing.F) {
	for _, seed := range []struct {
		value  string
		symbol string
	}{
		{"", ""},
		{"张", "#"},
		{"张三", "*"},
		{"欧阳娜娜", "🔒"},
		{"快乐卡牌收藏", "[隐藏]"},
		{"👩\u200d💻", "•"},
		{"张\xff三", "#"},
		{"张三", "\xff"},
	} {
		f.Add(seed.value, seed.symbol)
	}
	f.Fuzz(func(t *testing.T, value, symbol string) {
		got := common.MaskName(value, symbol)
		if !utf8.ValidString(got) {
			t.Fatalf("输出必须为合法UTF-8: %q", got)
		}
		if symbol == "" || !utf8.ValidString(symbol) {
			symbol = "*"
		}
		if !utf8.ValidString(value) {
			if got != symbol {
				t.Fatalf("非法UTF-8名称必须整体隐藏: %q", got)
			}
			return
		}
		chars := []rune(value)
		if len(chars) <= 1 {
			if got != value {
				t.Fatalf("空值或单字符必须保持不变: %q -> %q", value, got)
			}
			return
		}
		if !strings.HasPrefix(got, string(chars[0])) {
			t.Fatalf("名称必须保留首码点: %q -> %q", value, got)
		}
		if len(chars) >= 3 && !strings.HasSuffix(got, string(chars[len(chars)-1])) {
			t.Fatalf("三字符及以上名称必须保留尾码点: %q -> %q", value, got)
		}
		if len(got) > 2*utf8.UTFMax+3*len(symbol) {
			t.Fatalf("长名称不能使脱敏结果持续增长: %q", got)
		}
	})
}
