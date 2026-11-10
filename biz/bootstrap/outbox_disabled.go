//go:build pixiu_bootstrap_no_mysql && pixiu_bootstrap_no_postgresql

package bootstrap

func initOutbox() error {
	return nil
}
