package websocketx

import "testing"

func TestBudgetLimitsSnapshot(t *testing.T) {
	var b Budget
	limits := b.Limits()
	if limits.ReceiveBytes != 128<<20 || limits.SendBytes != 1<<30 || limits.SendMessages != 0 {
		t.Fatal(limits)
	}
	configured, err := NewBudget(BudgetConfig{SendBytes: 4096, SendMessages: 3})
	if err != nil {
		t.Fatal(err)
	}
	limits = configured.Limits()
	limits.SendBytes = 1
	if configured.Limits().SendBytes != 4096 || configured.Limits().SendMessages != 3 {
		t.Fatal("limits alias")
	}
}

func TestDefaultBudgetSupportsTenThousandRecipients(t *testing.T) {
	const (
		recipients   = 10_000
		messageBytes = 64 << 10
	)
	var b Budget
	// 即使网络尚未释放任何额度，默认预算也应能接纳一轮万连接广播。
	for i := 0; i < recipients; i++ {
		if !b.reserve(false, 1, messageBytes) {
			t.Fatalf("默认发送预算在第 %d 个目标耗尽: %+v", i+1, b.Usage())
		}
	}
	want := Usage{
		SendMessages: recipients,
		SendBytes:    recipients * messageBytes,
	}
	if got := b.Usage(); got != want {
		t.Fatalf("广播逻辑占用 = %+v，期望 %+v", got, want)
	}
	remaining := b.Limits().SendBytes - want.SendBytes
	if b.reserve(false, 1, remaining+1) {
		t.Fatal("超过默认发送字节预算的积压被接纳")
	}
	b.release(false, recipients, want.SendBytes)
	if got := b.Usage(); got != (Usage{}) {
		t.Fatalf("广播释放后预算未归零: %+v", got)
	}
}
