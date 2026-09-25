package logit

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bpcoder16/pixiu/rotatefile"
)

// readAll 只读取实际文件，避免通过稳定软链重复读取活动文件。
func readAll(t *testing.T, dir, prefix string) (string, int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	files := 0
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), prefix+".") || !e.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		sb.Write(data)
		files++
	}
	return sb.String(), files
}

func assertLink(t *testing.T, path, target string) {
	t.Helper()
	got, err := os.Readlink(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Base(target) {
		t.Fatalf("link %s = %q, want %q", path, got, filepath.Base(target))
	}
}

func TestE2ERotateOnWriteAfterIdle(t *testing.T) {
	for _, every := range []time.Duration{time.Hour, 24 * time.Hour} {
		t.Run(every.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				dir := t.TempDir()
				path := filepath.Join(dir, "app.log")
				file, err := rotatefile.New(path, rotatefile.OptEvery(every))
				if err != nil {
					t.Fatal(err)
				}
				w := NewWriter(file)
				l := MustNew(OptWriter(w))
				defer func() {
					if err := Close(l); err != nil {
						t.Error(err)
					}
				}()
				ctx := newTestContextWithLogID()
				l.Info(ctx, "before idle")
				previous, err := os.Readlink(path)
				if err != nil {
					t.Fatal(err)
				}
				previousPath := filepath.Join(dir, previous)
				before, err := os.ReadFile(previousPath)
				if err != nil || !strings.Contains(string(before), "before idle") {
					t.Fatalf("空闲前日志异常: %q, %v", before, err)
				}

				// 测试时钟直接跨过多个时段，验证无日志时不轮转或补建空文件。
				time.Sleep(3 * every)
				synctest.Wait()
				assertLink(t, path, previous)
				if entries, err := os.ReadDir(dir); err != nil || len(entries) != 2 {
					t.Fatalf("空闲期间不应新增文件: %v, %v", entries, err)
				}

				l.Info(ctx, "after idle")
				layout := "2006010215"
				if every == 24*time.Hour {
					layout = "20060102"
				}
				currentPath := path + "." + time.Now().Format(layout)
				assertLink(t, path, currentPath)
				if data, err := os.ReadFile(currentPath); err != nil || !strings.Contains(string(data), "after idle") || strings.Contains(string(data), "before idle") {
					t.Fatalf("恢复写入后应只写入当前时段: %q, %v", data, err)
				}
				if data, err := os.ReadFile(previousPath); err != nil || string(data) != string(before) {
					t.Fatalf("旧时段日志被修改: %q, %v", data, err)
				}
				if entries, err := os.ReadDir(dir); err != nil || len(entries) != 3 {
					t.Fatalf("不应补建空闲时段文件: %v, %v", entries, err)
				}
			})
		})
	}
}

// TestE2EDispatchSyncRotateWF 覆盖典型的生产日志链路:
// 主文件收 Info、.wf 文件收 Warn 以上；同步落盘；
// 关闭后完整保留；logId 全链路串联。
func TestE2EDispatchSyncRotateWF(t *testing.T) {
	dir := t.TempDir()

	mainFile, err := rotatefile.New(filepath.Join(dir, "app.log"), rotatefile.OptMaxFiles(3))
	if err != nil {
		t.Fatal(err)
	}
	wfFile, err := rotatefile.New(filepath.Join(dir, "app.wf.log"),
		rotatefile.OptMaxFiles(5))
	if err != nil {
		t.Fatal(err)
	}
	l := MustNew(OptDispatch(
		Target{Levels: []Level{DebugLevel, InfoLevel}, Writer: NewWriter(mainFile)},
		Target{Levels: []Level{WarnLevel, ErrorLevel, FatalLevel}, Writer: NewWriter(wfFile)},
	), OptFilterKeys("token"))

	ctx := newTestContextWithLogID()
	AddField(ctx, Str("svc", "e2e"))

	const (
		infoCnt = 60
		warnCnt = 10
		errCnt  = 5
	)
	for i := 0; i < infoCnt; i++ {
		l.Info(ctx, fmt.Sprintf("info event %02d", i), Int("i", i), Str("token", "secret"))
	}
	for i := 0; i < warnCnt; i++ {
		l.Warn(ctx, fmt.Sprintf("warn event %02d", i), Int("i", i))
	}
	for i := 0; i < errCnt; i++ {
		l.Error(ctx, fmt.Sprintf("error event %02d", i), Err(errBoom))
	}

	// Close(logger) 关闭全部目标
	if err := Close(l); err != nil {
		t.Fatal(err)
	}

	mainAll, mainRotated := readAll(t, dir, "app.log")
	wfAll, wfRotated := readAll(t, dir, "app.wf.log")

	// 分级正确
	if got := strings.Count(mainAll, "info event"); got != infoCnt {
		t.Errorf("main info lines = %d, want %d", got, infoCnt)
	}
	if strings.Contains(mainAll, "WARN") || strings.Contains(mainAll, "ERROR") {
		t.Error("wf levels leaked into main file")
	}
	if got := strings.Count(wfAll, "warn event"); got != warnCnt {
		t.Errorf("wf warn lines = %d, want %d", got, warnCnt)
	}
	if got := strings.Count(wfAll, "error event"); got != errCnt {
		t.Errorf("wf error lines = %d, want %d", got, errCnt)
	}
	if strings.Contains(wfAll, "info event") {
		t.Error("info leaked into wf file")
	}

	// 此用例不跨整点，两个目标都应各有一个实际文件。
	if mainRotated != 1 {
		t.Errorf("main physical files = %d, want 1", mainRotated)
	}
	if wfRotated != 1 {
		t.Errorf("wf physical files = %d, want 1", wfRotated)
	}

	// 脱敏
	if strings.Contains(mainAll, "secret") {
		t.Error("filtered token leaked")
	}
	if !strings.Contains(mainAll, "token=[***]") {
		t.Error("masked token missing")
	}

	// logId 全链路串联:两类文件中的 logId 一致且非空
	mainID := extractFirstLogID(mainAll)
	wfID := extractFirstLogID(wfAll)
	if mainID == "" || wfID == "" || mainID != wfID {
		t.Errorf("logId chain broken: main=%q wf=%q", mainID, wfID)
	}

	// 顺序:主文件(合并轮转件)内 i 递增
	first := strings.Index(mainAll, "info event")
	last := strings.LastIndex(mainAll, "info event")
	if first < 0 || last < first {
		t.Errorf("ordering corrupted")
	}
}

// TestE2EPanicDedicatedFile 验证 panic 走独立轮转文件且堆栈保留多行。
func TestE2EPanicDedicatedFile(t *testing.T) {
	dir := t.TempDir()
	panicFile, err := rotatefile.New(filepath.Join(dir, "panic.log"), rotatefile.OptMaxFiles(3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = panicFile.Close() })
	SetPanicLogger(NewWriter(panicFile))
	t.Cleanup(func() {
		panicLoggerPtr.Store(nil)
	})

	ctx := newTestContextWithLogID()
	func() {
		defer RecoverAndReport(ctx)
		panic(fmt.Errorf("db connection refused: %w", errBoom))
	}()

	all, _ := readAll(t, dir, "panic.log")
	if !strings.Contains(all, "db connection refused") {
		t.Errorf("panic message missing: %q", all)
	}
	if strings.Count(all, "\n") < 3 {
		t.Errorf("panic stack should contain physical newlines: %q", all)
	}
}

func extractFirstLogID(s string) string {
	i := strings.Index(s, "logId=[")
	if i < 0 {
		return ""
	}
	rest := s[i+len("logId=["):]
	j := strings.IndexByte(rest, ']')
	if j < 0 {
		return ""
	}
	return rest[:j]
}
