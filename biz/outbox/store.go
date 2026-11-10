package outbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"gorm.io/gorm"
)

// Store 借用配置中的主库，不创建连接、迁移表或启动后台服务。
type Store struct {
	cfg StoreConfig
}

func NewStore(cfg StoreConfig) (*Store, error) {
	supported := cfg.Backend == MySQL && mysqlBackendEnabled || cfg.Backend == PostgreSQL && postgresqlBackendEnabled
	if !supported || cfg.MasterDB == nil {
		return nil, fmt.Errorf("%w: enabled backend and master provider required", ErrInvalidArgument)
	}
	if cfg.Location == nil {
		cfg.Location = time.UTC
	}
	return &Store{cfg: cfg}, nil
}

// validateDB 检查 Outbox 的会话边界，调用方须提供由 GORM 初始化的会话。
func (s *Store) validateDB(db *gorm.DB, transaction bool) error {
	if db == nil || db.DryRun {
		return fmt.Errorf("%w: unavailable or DryRun database", ErrInvalidArgument)
	}
	if db.Error != nil {
		return db.Error
	}
	name := string(s.cfg.Backend)
	if s.cfg.Backend == PostgreSQL {
		name = "postgres"
	}
	if db.Dialector.Name() != name {
		return ErrBackendMismatch
	}
	_, isTx := db.Statement.ConnPool.(gorm.TxCommitter)
	if transaction && !isTx {
		return ErrNotTransaction
	}
	if !transaction && isTx {
		return fmt.Errorf("%w: master provider returned a transaction", ErrInvalidArgument)
	}
	return nil
}

func clean(db *gorm.DB) *gorm.DB {
	return db.Session(&gorm.Session{NewDB: true, SkipDefaultTransaction: true}).Model(&Record{})
}

func (s *Store) master(ctx context.Context) (*gorm.DB, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db := s.cfg.MasterDB(ctx)
	if err := s.validateDB(db, false); err != nil {
		return nil, err
	}
	// 时区只附加到本次查询，沿事务和 NewDB 会话传递，不修改共享连接。
	return clean(db).WithContext(context.WithValue(ctx, recordLocationKey{}, s.cfg.Location)), nil
}

func (s *Store) nowSQL() string {
	if s.cfg.Backend == MySQL {
		return "CURRENT_TIMESTAMP(6)"
	}
	return "clock_timestamp()"
}

// EnqueueTx 在调用方事务内登记并返回自增 ID，不发布、不提交、不自动唤醒。
// ID 只在最外层事务提交成功后可用于发送；回滚可能留下序列间隙，重复登记会生成新任务。
func (s *Store) EnqueueTx(tx *gorm.DB, task Task) (int64, error) {
	if err := s.validateDB(tx, true); err != nil {
		return 0, err
	}
	if !validKey(task.Kind, 64) || task.Version <= 0 || task.Version > math.MaxInt32 || len(task.Payload) > 1<<20 {
		return 0, fmt.Errorf("%w: task kind, version or payload", ErrInvalidArgument)
	}
	var expiresAt any
	if !task.ExpiresAt.IsZero() {
		value := task.ExpiresAt.In(s.cfg.Location).Truncate(time.Microsecond)
		if s.cfg.Backend == MySQL && (value.Unix() < 1 || value.Unix() > math.MaxInt32) {
			return 0, fmt.Errorf("%w: expiration exceeds MySQL TIMESTAMP range", ErrInvalidArgument)
		}
		expiresAt = value
	}
	payload := bytes.Clone(task.Payload)
	if payload == nil {
		payload = []byte{}
	}
	logID, _ := logit.LogIDFromContext(tx.Statement.Context)
	values := map[string]any{
		"kind":       task.Kind,
		"version":    task.Version,
		"payload":    payload,
		"expires_at": expiresAt,
		"status":     Pending,
		"attempts":   int64(0),
		"last_error": "",
		"log_id":     logID,
		"created_at": gorm.Expr(s.nowSQL()),
		"updated_at": gorm.Expr(s.nowSQL()),
	}
	// Model 提供自增字段元信息，GORM 通过 LastInsertId / RETURNING 将 ID 回填到 map。
	if err := clean(tx).Create(values).Error; err != nil {
		return 0, err
	}
	id, ok := values["id"].(int64)
	if !ok || id <= 0 {
		return 0, errors.New("outbox: missing generated task ID")
	}
	return id, nil
}

func (s *Store) Get(ctx context.Context, id int64) (Record, error) {
	var record Record
	if id <= 0 {
		return record, ErrInvalidArgument
	}
	db, err := s.master(ctx)
	if err != nil {
		return record, err
	}
	err = db.Where("id = ?", id).Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = ErrNotFound
	}
	return record, err
}
