package logit

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultRotateEvery = time.Hour
	defaultRotateFiles = 48 // 当前轮转维度的实际文件数，包含当前时段，不包含稳定软链
	hourlyLayout       = "2006010215"
	dailyLayout        = "20060102"
)

// rotateConfig 是 rotateFile 的全部可调参数。
type rotateConfig struct {
	every    time.Duration
	maxFiles int
}

func defaultRotateConfig() rotateConfig {
	return rotateConfig{
		every:    defaultRotateEvery,
		maxFiles: defaultRotateFiles,
	}
}

// RotateOption 定制 NewRotateFile 的行为。
type RotateOption func(*rotateConfig)

// OptRotateEvery 仅接受 time.Hour 或 24*time.Hour，按本地整小时、整天划分时段。
// 默认每小时；进入更晚时段后的首次 Write 触发轮转，回拨时继续写当前文件。
func OptRotateEvery(d time.Duration) RotateOption {
	return func(c *rotateConfig) { c.every = d }
}

// OptRotateMaxFiles 设置当前轮转维度的实际文件数上限（包含当前时段），默认 48，最小为 3。
// 另一轮转维度的历史文件不计数、不清理。
// 每小时整点清理完成前，文件数可能暂时超过上限。
func OptRotateMaxFiles(n int) RotateOption {
	return func(c *rotateConfig) { c.maxFiles = n }
}

// rotateFile 直接写入带时段后缀的实际文件；path 是指向当前文件的稳定软链。
// Write 在写入前核对时段并按需切换；无日志时不轮转，也不补建空闲时段文件。
type rotateFile struct {
	mu          sync.Mutex
	path        string
	dir         string
	baseName    string
	f           *os.File
	boundary    time.Time
	cfg         rotateConfig
	closed      bool
	closing     int           // 正在异步同步、关闭的旧文件数，由 mu 保护
	closeCond   *sync.Cond    // 等待 closing 归零，等待期间释放 mu
	cleanupStop chan struct{} // Close 通知定时循环退出
	cleanupDone chan struct{} // 定时循环完成后关闭
	key         WriterKey
}

var _ Writer = (*rotateFile)(nil)

func (r *rotateFile) WriterKey() WriterKey { return r.key }

// NewRotateFile 要求绝对路径，打开当前时段的实际文件并建立稳定软链，之后仅在 Write 进入更晚时段时轮转。
// 空闲或停服期间不补建文件；每小时整点清理，使用结束后必须 Close 停止定时循环。
func NewRotateFile(path string, opts ...RotateOption) (Writer, error) {
	cfg := defaultRotateConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	r, err := openRotateFile(path, cfg, time.Now())
	if err != nil {
		return nil, err
	}
	return r, nil
}

func openRotateFile(path string, cfg rotateConfig, now time.Time) (*rotateFile, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("logit: rotate path must be absolute: %q", path)
	}
	if cfg.every != time.Hour && cfg.every != 24*time.Hour {
		return nil, errors.New("logit: rotate period must be time.Hour or 24*time.Hour")
	}
	if cfg.maxFiles < 3 {
		return nil, errors.New("logit: rotate max files must be at least 3")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	r := &rotateFile{
		key:      NewWriterKey(),
		path:     path,
		dir:      dir,
		baseName: filepath.Base(path),
		boundary: periodStart(now, cfg.every),
		cfg:      cfg,
	}
	target := r.periodPath(r.boundary)
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	if err := replaceSymlink(path, filepath.Base(target)); err != nil {
		_ = f.Close()
		return nil, err
	}
	r.f = f
	if err := r.cleanup(); err != nil {
		_ = f.Close()
		return nil, err
	}
	r.closeCond = sync.NewCond(&r.mu)
	r.cleanupStop = make(chan struct{})
	r.cleanupDone = make(chan struct{})
	// 在构造时安排下一个整点，避免依赖 goroutine 的启动时刻。
	go r.runCleanup(time.NewTimer(nextCleanupDelay(time.Now())))
	return r, nil
}

// Write 实现 Writer（并发安全）。进入更晚时段时先切换文件，再写入本条日志。
func (r *rotateFile) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, os.ErrClosed
	}
	if ready, rotateErr := r.advanceLocked(time.Now()); !ready {
		return 0, rotateErr
	}
	n, err := r.f.Write(p)
	return n, normalizeWriteError(n, len(p), err)
}

// Sync 等待旧文件同步、关闭完成，再同步当前文件，不触发轮转或清理。
func (r *rotateFile) Sync() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.closing > 0 && !r.closed {
		r.closeCond.Wait()
	}
	if r.closed {
		return os.ErrClosed
	}
	return r.f.Sync()
}

// Fd 返回当前底层文件描述符（供 HookStdout/HookStderr 劫持）。
// fd 劫持仍绑定打开时的文件，软链切换不会改变已有 fd 的目标。
func (r *rotateFile) Fd() uintptr {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.f.Fd()
}

// Close 停止写入及定时清理，等待后台工作完成，再同步、关闭当前文件。
func (r *rotateFile) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return os.ErrClosed
	}
	r.closed = true
	close(r.cleanupStop)
	r.closeCond.Broadcast()
	for r.closing > 0 {
		r.closeCond.Wait()
	}
	r.mu.Unlock()
	// 等待定时循环完成本轮清理并退出。
	<-r.cleanupDone
	return errors.Join(r.f.Sync(), r.f.Close())
}

// advanceLocked 在持有 mu 时调用。先打开新文件并原子替换软链；
// 两步都成功后才切换 Writer，失败时禁止把新时段日志写回旧文件。
// ready 表示当前时段已就绪；旧文件独立异步关闭，不影响本条日志写入。
func (r *rotateFile) advanceLocked(now time.Time) (ready bool, err error) {
	boundary := periodStart(now, r.cfg.every)
	// 同时段或时钟回拨时继续写当前文件，避免重新打开清理可能删除的旧路径。
	if boundary.After(r.boundary) {
		target := r.periodPath(boundary)
		nextFile, openErr := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if openErr != nil {
			return false, openErr
		}
		if linkErr := replaceSymlink(r.path, filepath.Base(target)); linkErr != nil {
			_ = nextFile.Close()
			return false, linkErr
		}
		oldFile := r.f
		r.f = nextFile
		r.boundary = boundary
		r.closing++
		go r.closeOldFile(oldFile)
	}
	return true, nil
}

// closeOldFile 每个旧句柄只执行一次；Sync 失败后仍关闭文件，不等待其他旧文件。
func (r *rotateFile) closeOldFile(f *os.File) {
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "logit: close old file %q: %v\n", f.Name(), err)
	}
	r.mu.Lock()
	r.closing--
	if r.closing == 0 {
		r.closeCond.Broadcast()
	}
	r.mu.Unlock()
}

// runCleanup 在同一个 goroutine 中定期清理，慢清理不会启动重叠任务。
func (r *rotateFile) runCleanup(timer *time.Timer) {
	defer close(r.cleanupDone)
	defer timer.Stop()
	for {
		select {
		case <-r.cleanupStop:
			return
		case <-timer.C:
			if err := r.cleanup(); err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "logit: cleanup %q: %v\n", r.path, err)
			}
			// 清理耗时不累积到下次调度，错过的整点不补跑。
			timer.Reset(nextCleanupDelay(time.Now()))
		}
	}
}

func nextCleanupDelay(now time.Time) time.Duration {
	// 按本地分秒对齐，兼容非整小时的 UTC 偏移。
	return time.Hour - time.Duration(now.Minute())*time.Minute - time.Duration(now.Second())*time.Second - time.Duration(now.Nanosecond())
}

func (r *rotateFile) periodPath(boundary time.Time) string {
	layout := hourlyLayout
	if r.cfg.every == 24*time.Hour {
		layout = dailyLayout
	}
	return r.path + "." + boundary.Format(layout)
}

// replaceSymlink 先在同目录生成新软链，再用 rename 替换稳定路径。
// 软链目标只写文件名，移动整个日志目录时仍能正确解析。
func replaceSymlink(path, target string) error {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink == 0 {
			return fmt.Errorf("logit: rotate path %q exists and is not a symlink", path)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if current, err := os.Readlink(path); err == nil && current == target {
		return nil
	}
	dir := filepath.Dir(path)
	for {
		// 固定前缀避免长日志文件名叠加 UUID 后超限；由 Symlink 检查名称冲突。
		name := filepath.Join(dir, ".logit-link-"+NewLogID())
		if err := os.Symlink(target, name); err != nil {
			if os.IsExist(err) {
				continue
			}
			return err
		}
		if err := os.Rename(name, path); err != nil {
			cleanupErr := os.Remove(name)
			if os.IsNotExist(cleanupErr) {
				cleanupErr = nil
			}
			return errors.Join(err, cleanupErr)
		}
		return nil
	}
}

// cleanup 只读取不变配置，按时段保留最新文件，不访问 Writer 状态或获取写锁。
// 最少保留 3 个文件覆盖正常轮转中的当前、上一轮及正在准备的新文件。
// 仅管理当前轮转维度的后缀；连续失败留下大量时段文件的情况不额外防护。
func (r *rotateFile) cleanup() error {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return err
	}
	type periodFile struct {
		name string
		time time.Time
	}
	files := make([]periodFile, 0, len(entries))
	prefix := r.baseName + "."
	for _, e := range entries {
		if !e.Type().IsRegular() || !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		ts, ok := parsePeriodSuffix(strings.TrimPrefix(e.Name(), prefix), r.cfg.every)
		if ok {
			files = append(files, periodFile{name: e.Name(), time: ts})
		}
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].time.Equal(files[j].time) {
			return files[i].name > files[j].name
		}
		return files[i].time.After(files[j].time)
	})
	var removeErr error
	for i := r.cfg.maxFiles; i < len(files); i++ {
		err := os.Remove(filepath.Join(r.dir, files[i].name))
		if !os.IsNotExist(err) {
			removeErr = errors.Join(removeErr, err)
		}
	}
	return removeErr
}

func parsePeriodSuffix(suffix string, every time.Duration) (time.Time, bool) {
	layout := hourlyLayout
	if every == 24*time.Hour {
		layout = dailyLayout
	}
	if len(suffix) != len(layout) {
		return time.Time{}, false
	}
	ts, err := time.ParseInLocation(layout, suffix, time.Local)
	return ts, err == nil
}

func periodStart(t time.Time, every time.Duration) time.Time {
	hour := 0
	if every == time.Hour {
		hour = t.Hour()
	}
	return time.Date(t.Year(), t.Month(), t.Day(), hour, 0, 0, 0, t.Location())
}
