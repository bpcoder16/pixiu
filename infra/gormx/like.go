package gormx

import (
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var likeReplacer = strings.NewReplacer("!", "!!", "%", "!%", "_", "!_")

// escapeLike 将关键词中的 LIKE 特殊字符按原文转义，查询须配合 ESCAPE '!' 使用。
// 不添加两侧的 %，由调用方决定匹配范围。
func escapeLike(keyword string) string {
	return likeReplacer.Replace(keyword)
}

// WhereLikeContains 为指定列添加按关键词原文包含匹配的条件，关键词为空时保留原查询。
// queryKey 为代码指定的数据库列名，通过标识符引用，不能传入 SQL 表达式。
func WhereLikeContains(tx *gorm.DB, queryKey, keyword string) *gorm.DB {
	if keyword == "" {
		return tx
	}
	return tx.Where("? LIKE ? ESCAPE '!'", clause.Column{Name: queryKey}, "%"+escapeLike(keyword)+"%")
}
