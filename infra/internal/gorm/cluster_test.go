package gorm

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	gormlib "gorm.io/gorm"
)

func TestBuildClusterRegistersEndpointsInOrder(t *testing.T) {
	var cluster Cluster
	var opened []*sql.DB
	var endpoints []*gormlib.DB
	master := &gormlib.DB{}
	slaveA := &gormlib.DB{}
	slaveB := &gormlib.DB{}
	err := BuildCluster(context.Background(), &cluster, master, []*gormlib.DB{slaveA, slaveB}, func(_ context.Context, db *gormlib.DB) (*gormlib.DB, *sql.DB, error) {
		endpoints = append(endpoints, db)
		pool, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return nil, nil, err
		}
		opened = append(opened, pool)
		return db, pool, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(endpoints, []*gormlib.DB{master, slaveA, slaveB}) || cluster.master != master ||
		len(cluster.slaves) != 2 || cluster.slaves[0] != slaveA || cluster.slaves[1] != slaveB {
		t.Fatalf("主从登记顺序错误: endpoints=%v cluster=%+v", endpoints, cluster)
	}
	if err := cluster.Close(); err != nil {
		t.Fatal(err)
	}
	for _, pool := range opened {
		if err := pool.PingContext(context.Background()); err == nil || !strings.Contains(err.Error(), "closed") {
			t.Fatalf("连接池未关闭: %v", err)
		}
	}
}

func TestBuildClusterClosesOpenedPoolsOnFailure(t *testing.T) {
	var cluster Cluster
	var opened []*sql.DB
	var endpoints []int
	failure := errors.New("slave open failed")
	err := BuildCluster(context.Background(), &cluster, 0, []int{1, 2}, func(_ context.Context, endpoint int) (*gormlib.DB, *sql.DB, error) {
		endpoints = append(endpoints, endpoint)
		pool, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return nil, nil, err
		}
		opened = append(opened, pool)
		if endpoint == 2 {
			return nil, pool, failure
		}
		return &gormlib.DB{}, pool, nil
	})
	if !errors.Is(err, failure) {
		t.Fatalf("构建错误=%v, want %v", err, failure)
	}
	if !slices.Equal(endpoints, []int{0, 1, 2}) {
		t.Fatalf("构建顺序=%v", endpoints)
	}
	for _, pool := range opened {
		if err := pool.PingContext(context.Background()); err == nil || !strings.Contains(err.Error(), "closed") {
			t.Fatalf("已创建的连接池未关闭: %v", err)
		}
	}
	if err := cluster.Close(); err != nil {
		t.Fatalf("重复关闭失败: %v", err)
	}
}
