GO ?= go

.PHONY: test test-race

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...
