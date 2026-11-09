//go:build pixiu_bootstrap_no_mysql && pixiu_bootstrap_no_postgresql

package bootstrap_test

import (
	"os/exec"
	"strings"
	"testing"
)

func TestBootstrapWithoutSQLDependencies(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "go", "list", "-deps", "-tags=pixiu_bootstrap_no_mysql,pixiu_bootstrap_no_postgresql", ".")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	for dep := range strings.SplitSeq(string(out), "\n") {
		if strings.HasPrefix(dep, "gorm.io/") || dep == "github.com/bpcoder16/pixiu/infra/schemamigrate" {
			t.Errorf("禁用全部 SQL 装配后仍依赖 %s", dep)
		}
	}
}
