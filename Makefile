# ============================================================
# 以图搜商品系统 · Makefile
# ============================================================
.PHONY: all build test clean deploy-dev lint

SERVICES = search-service write-service bitmap-filter-service flink-pipeline cron-scheduler

# ── 构建 ──
# FIX-AD: Proto 编译 (Python + Java gRPC stubs)
proto:
	@echo "Compiling proto → Python stubs..."
	python -m grpc_tools.protoc \
		-I proto/ \
		--python_out=services/search-service/app/infra/pb/ \
		--grpc_python_out=services/search-service/app/infra/pb/ \
		proto/bitmap_filter.proto
	@echo "Compiling proto → Java stubs..."
	protoc \
		-I proto/ \
		--java_out=services/bitmap-filter-service/src/main/java/ \
		--grpc-java_out=services/bitmap-filter-service/src/main/java/ \
		proto/bitmap_filter.proto
	@echo "Proto compilation complete."

build: proto
	docker-compose build

build-search:
	docker build -t imgsrch/search-service:latest services/search-service

build-write:
	docker build -t imgsrch/write-service:latest services/write-service

build-bitmap:
	cd services/bitmap-filter-service && mvn clean package -DskipTests

build-flink:
	cd services/flink-pipeline && mvn clean package -DskipTests

# ── 测试 ──
test: test-python test-java

test-python:
	cd services/search-service && python -m pytest tests/ -v --cov=app --cov-report=term-missing
	cd services/write-service && python -m pytest tests/ -v

test-java:
	cd services/flink-pipeline && mvn test
	cd services/bitmap-filter-service && mvn test

test-unit:
	cd services/search-service && python -m pytest tests/unit/ -v

test-integration:
	docker-compose -f docker-compose.yml up -d postgres redis kafka milvus-standalone etcd minio
	sleep 10
	cd services/search-service && python -m pytest tests/integration/ -v
	docker-compose down

# ── 按业务部分测试 ──
test-write:
	cd services/write-service && python -m pytest tests/unit/ -v

test-write-integ:
	cd services/write-service && INTEGRATION_TEST=true python -m pytest tests/integration/ -v

test-search:
	cd services/search-service && python -m pytest tests/unit/ -v

test-search-integ:
	cd services/search-service && INTEGRATION_TEST=true python -m pytest tests/integration/ -v

test-lifecycle:
	cd services/cron-scheduler && python -m pytest tests/unit/ -v

test-lifecycle-integ:
	cd services/cron-scheduler && INTEGRATION_TEST=true python -m pytest tests/integration/ -v

test-flink:
	cd services/flink-pipeline && mvn test -Dgroups=unit

test-flink-integ:
	cd services/flink-pipeline && mvn verify -Pintegration

test-bitmap:
	cd services/bitmap-filter-service && mvn test -Dgroups=unit

test-bitmap-integ:
	cd services/bitmap-filter-service && mvn verify -Pintegration

test-e2e:
	E2E_TEST=true python -m pytest tests/e2e/ -v

test-all: test-write test-search test-lifecycle test-flink test-bitmap

test-all-integ: test-write-integ test-search-integ test-lifecycle-integ test-flink-integ test-bitmap-integ test-e2e

# ── 开发环境 ──
deploy-dev:
	docker-compose up -d

deploy-down:
	docker-compose down -v

# ── 代码质量 ──
lint:
	cd services/search-service && python -m ruff check app/
	cd services/write-service && python -m ruff check app/

format:
	cd services/search-service && python -m ruff format app/
	cd services/write-service && python -m ruff format app/

# ── 数据库 ──
db-init:
	docker-compose exec postgres psql -U imgsrch -d image_search -f /docker-entrypoint-initdb.d/01_init.sql

db-migrate:
	docker-compose exec postgres psql -U imgsrch -d image_search -f /docker-entrypoint-initdb.d/01_init.sql

# ── 清理 ──
clean:
	docker-compose down -v --remove-orphans
	cd services/flink-pipeline && mvn clean
	cd services/bitmap-filter-service && mvn clean
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
