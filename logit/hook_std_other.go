//go:build !darwin && !linux

package logit

import "errors"

var errUnsupportedPlatform = errors.New("logit: std hook unsupported on this platform")

func dupToFd(Writer, int) error { return errUnsupportedPlatform }

func saveFd(int) (int, error) { return -1, errUnsupportedPlatform }

func restoreFd(int, int) error { return errUnsupportedPlatform }

func closeFd(int) error { return errUnsupportedPlatform }
