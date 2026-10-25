.DEFAULT_GOAL := help
REDIS_ADDR ?= localhost:6379

.PHONY: help
help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

.PHONY: build
build: ## Build all binaries into ./bin
	CGO_ENABLED=0 go build -trimpath -o bin/ ./cmd/...

.PHONY: test
test: ## Run unit, conformance and end-to-end tests with the race detector
	go test -race ./...

.PHONY: test-integration
test-integration: ## Run the store conformance suite and live e2e tests against a real Redis (REDIS_ADDR)
	REDIS_ADDR=$(REDIS_ADDR) go test -race -count=1 -p 1 ./internal/store/redisstore/ ./internal/e2e/

.PHONY: cover
cover: ## Test coverage report
	go test -coverprofile=coverage.out ./internal/...
	go tool cover -func=coverage.out | tail -1

.PHONY: bench
bench: ## Benchmark the hot path (ShouldCall, Record)
	go test -run '^$$' -bench . -benchmem ./internal/throttler/

.PHONY: fuzz
fuzz: ## Fuzz the policy for 30s (property tests run as part of make test)
	go test -run '^$$' -fuzz FuzzEvaluate -fuzztime 30s ./internal/policy/

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

.PHONY: demo
demo: ## Start the full demo (Redis, 2 evaluators, 3 auction pods, simulator)
	docker compose up --build -d
	@echo "Follow decisions with: make demo-logs"

.PHONY: demo-logs
demo-logs: ## Follow throttling decisions from the evaluators
	docker compose logs -f --no-log-prefix evaluator-1 evaluator-2 \
	  | grep --line-buffered -E '"buyer|leadership' \
	  | jq --unbuffered -r -f deploy/demo-logs.jq

.PHONY: demo-verify
demo-verify: ## Run the demo as an acceptance test: start it, check every buyer's expectations, tear it down
	docker compose up --build -d
	go run ./cmd/demoverify; status=$$?; \
	  if [ $$status -ne 0 ]; then docker compose logs --no-log-prefix evaluator-1 evaluator-2 | tail -100; fi; \
	  docker compose down -v; exit $$status

.PHONY: demo-down
demo-down: ## Stop the demo and remove its containers
	docker compose --profile observability down -v
