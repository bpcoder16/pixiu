GO ?= go

BOOTSTRAP_DISABLE_TAGS := pixiu_bootstrap_no_es pixiu_bootstrap_no_aliyun_oss \
	pixiu_bootstrap_no_aliyun_sts pixiu_bootstrap_no_aliyun_es \
	pixiu_bootstrap_no_mysql pixiu_bootstrap_no_postgresql pixiu_bootstrap_no_redis pixiu_bootstrap_no_nats

.PHONY: test test-race test-bootstrap-tags

# NATS 集成测试单独执行，避免服务端的后台采样与 bootstrap 时区初始化测试混跑。
test:
	$(GO) test ./...
	$(GO) test -tags=natsintegration ./biz/bootstrap -run '^TestNATSFromConfig$$'
	$(MAKE) test-bootstrap-tags

test-race:
	$(GO) test -race ./...
	$(GO) test -race -tags=natsintegration ./biz/bootstrap -run '^TestNATSFromConfig$$'
	$(MAKE) test-bootstrap-tags BOOTSTRAP_TEST_FLAGS=-race

# 各标签独立验证，再覆盖全部禁用、Redis/NATS 组合及仅保留 PostgreSQL 的数据库装配。
test-bootstrap-tags:
	@set -e; \
	bootstrap_all_tags=$$(printf '%s' "$(BOOTSTRAP_DISABLE_TAGS)" | tr ' ' ','); \
	for bootstrap_tags in $(BOOTSTRAP_DISABLE_TAGS) "$$bootstrap_all_tags" pixiu_bootstrap_no_redis,pixiu_bootstrap_no_nats; do \
		printf 'bootstrap tags: %s\n' "$$bootstrap_tags"; \
		$(GO) test $(BOOTSTRAP_TEST_FLAGS) -tags="$$bootstrap_tags" ./biz/bootstrap \
			-run '^(TestBootstrapWithout|TestCronWithoutRedis|TestCronStandaloneDoesNotNeedHTTPAndShutdownBeforeRun)'; \
	done
	$(GO) test $(BOOTSTRAP_TEST_FLAGS) -tags=pixiu_bootstrap_no_mysql ./biz/bootstrap -run '^TestPostgreSQL'
