package gorm

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	gormlib "gorm.io/gorm"
)

func TestBuildClusterRegistersEndpointsInOrder(t *testing.T) {
	var cluster Cluster
	var opened []*sql.DB
	var roles []string
	master := &gormlib.DB{}
	slaveA := &gormlib.DB{}
	slaveB := &gormlib.DB{}
	err := BuildCluster(context.Background(), &cluster, master, []*gormlib.DB{slaveA, slaveB}, func(_ context.Context, db *gormlib.DB, role string) (*gormlib.DB, *sql.DB, error) {
		roles = append(roles, role)
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
	if strings.Join(roles, ",") != "master,slave,slave" || cluster.master != master ||
		len(cluster.slaves) != 2 || cluster.slaves[0] != slaveA || cluster.slaves[1] != slaveB {
		t.Fatalf("主从登记顺序错误: roles=%v cluster=%+v", roles, cluster)
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
	var roles []string
	failure := errors.New("slave open failed")
	err := BuildCluster(context.Background(), &cluster, 0, []int{1, 2}, func(_ context.Context, endpoint int, role string) (*gormlib.DB, *sql.DB, error) {
		roles = append(roles, role)
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
	if strings.Join(roles, ",") != "master,slave,slave" {
		t.Fatalf("构建顺序=%v", roles)
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
