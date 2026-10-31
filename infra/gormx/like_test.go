package gormx_test

import (
	"reflect"
	"testing"

	"github.com/bpcoder16/pixiu/infra/gormx"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type likeRecord struct {
	ID          string
	OwnerID     uint64
	Title       string
	Description string
}

func TestWhereLikeContains(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := pool.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := db.AutoMigrate(&likeRecord{}); err != nil {
		t.Fatal(err)
	}
	rows := []likeRecord{
		{
			ID:          "TITLE",
			OwnerID:     42,
			Title:       "前100%_!\\后",
			Description: "描述",
		},
		{
			ID:          "DESCRIPTION",
			OwnerID:     42,
			Title:       "前100XYZ!后",
			Description: "前100%_!后",
		},
		{
			ID:          "OTHER-OWNER",
			OwnerID:     43,
			Title:       "前100%_!后",
			Description: "前100%_!后",
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		queryKey string
		keyword  string
		want     []string
	}{
		{"标题列及特殊字符", "title", "100%_!", []string{"TITLE"}},
		{"描述列及特殊字符", "description", "100%_!", []string{"DESCRIPTION"}},
		{"空关键词保留原筛选", "title", "", []string{"DESCRIPTION", "TITLE"}},
		{"关键词按原文绑定", "title", "' OR 1=1 --", []string{}},
		{"反斜杠按原文匹配", "title", "!\\", []string{"TITLE"}},
		{"不清理关键词空白", "title", " 100%_! ", []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := db.Model(&likeRecord{}).
				Where("owner_id = ?", 42)
			ids := make([]string, 0)
			err := gormx.WhereLikeContains(query, tc.queryKey, tc.keyword).
				Order("id ASC").
				Pluck("id", &ids).Error
			if err != nil || !reflect.DeepEqual(ids, tc.want) {
				t.Fatalf("列名及原有筛选应保留，关键词按原文匹配: ids=%v want=%v err=%v", ids, tc.want, err)
			}
		})
	}
}
