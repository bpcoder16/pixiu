GO ?= go

.PHONY: test test-race

# 两轮顺序执行，避免 NATS 服务端的后台采样与 bootstrap 时区初始化测试混跑。
test:
	$(GO) test ./...
	$(GO) test -tags=natsintegration ./biz/bootstrap -run '^TestNATSFromConfig$$'

test-race:
	$(GO) test -race ./...
	$(GO) test -race -tags=natsintegration ./biz/bootstrap -run '^TestNATSFromConfig$$'
