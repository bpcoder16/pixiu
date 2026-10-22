package bootstrap_test

import (
	"context"
	"errors"
	"time"
)

// stopAndWait 仅用于测试清理；生产调用方显式传递统一的排空期限。
func stopAndWait(stop func() error, wait func(context.Context) error, cancel func()) error {
	err := stop()
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	waitErr := wait(ctx)
	if waitErr != nil {
		cancel()
	}
	return errors.Join(err, waitErr)
}
