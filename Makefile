GO ?= go

BOOTSTRAP_DISABLE_TAGS := pixiu_bootstrap_no_es pixiu_bootstrap_no_aliyun_oss \
	pixiu_bootstrap_no_aliyun_sts pixiu_bootstrap_no_aliyun_es \
	pixiu_bootstrap_no_mysql pixiu_bootstrap_no_postgresql pixiu_bootstrap_no_redis pixiu_bootstrap_no_nats

.PHONY: test test-race test-bootstrap-tags test-outbox-integration

# NATS 集成测试单独执行，避免服务端的后台采样与 bootstrap 时区初始化测试混跑。
test:
	$(GO) test ./...
	$(GO) test -tags=natsintegration ./biz/bootstrap -run '^TestNATSFromConfig$$'
	$(MAKE) test-bootstrap-tags

test-race:
	$(GO) test -race ./...
	$(GO) test -race -tags=natsintegration ./biz/bootstrap -run '^TestNATSFromConfig$$'
	$(MAKE) test-bootstrap-tags BOOTSTRAP_TEST_FLAGS=-race

# 显式验收入口必须同时提供两种数据库；测试仅创建随机命名的临时数据库。
test-outbox-integration:
	@test -n "$$PIXIU_OUTBOX_MYSQL_DSN" || (echo "需要 PIXIU_OUTBOX_MYSQL_DSN" >&2; exit 1)
	@test -n "$$PIXIU_OUTBOX_POSTGRESQL_DSN" || (echo "需要 PIXIU_OUTBOX_POSTGRESQL_DSN" >&2; exit 1)
	$(GO) test $(OUTBOX_TEST_FLAGS) -count=1 ./biz/outbox/...

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
	$(GO) test $(BOOTSTRAP_TEST_FLAGS) -tags=pixiu_bootstrap_no_nats ./biz/bootstrap -run '^TestOutbox'
	$(GO) test $(BOOTSTRAP_TEST_FLAGS) -tags=pixiu_bootstrap_no_postgresql ./biz/bootstrap -run '^TestOutbox'
	@set -e; \
	for outbox_tags in pixiu_bootstrap_no_mysql pixiu_bootstrap_no_postgresql pixiu_bootstrap_no_mysql,pixiu_bootstrap_no_postgresql; do \
		$(GO) test $(BOOTSTRAP_TEST_FLAGS) -tags="$$outbox_tags" ./biz/outbox/...; \
	done
