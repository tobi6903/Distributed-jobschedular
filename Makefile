# Distributed Job Scheduler: developer entry points.
#   make up          start the whole stack (Postgres, API, workers, scheduler, Prometheus,
#                    Alertmanager, Grafana, Loki, Tempo) with Docker Compose
#   make test        unit + integration tests with the race detector
#   make load-test   load-test the running stack (k6 if installed, else the Go load generator)
#   make chaos       SIGKILL workers and schedulers mid-run and verify nothing is lost
# pipefail matters: without it a failing `helm template` piped into kubeconform "validates" zero
# resources and the target still passes.
SHELL     := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
GO        ?= go
BIN       := bin
CMDS      := api worker scheduler jobctl
COMPOSE   := docker compose -f deploy/docker/docker-compose.yml
IMAGE_TAG ?= dev
REGISTRY  ?= ghcr.io/tobi6903/distributed-jobschedular
API_URL   ?= http://localhost:8080
# Integration tests need Postgres: either set TEST_DATABASE_URL (admin connection string) or
# have Docker running so testcontainers can start one. `make test-db` starts a local container.
TEST_DATABASE_URL ?=

.PHONY: help build test test-short lint fmt vet tidy test-db up down logs ps key demo migrate \
        load-test bench chaos chaos-docker smoke kind-up k8s-verify argocd-verify kind-down docker-build helm-lint k8s-validate rules-test validate dev-up dev-down clean

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-14s %s\n", $$1, $$2}'

build: ## build all binaries into ./bin
	@mkdir -p $(BIN)
	@for c in $(CMDS); do echo "building $$c"; CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-s -w" -o $(BIN)/$$c ./cmd/$$c || exit 1; done

test: ## all tests with -race (includes the 50-worker x 100k-job correctness test)
	REQUIRE_TEST_DB=1 TEST_DATABASE_URL=$(TEST_DATABASE_URL) $(GO) test -race -count=1 -timeout 20m ./...

test-short: ## fast tests (smaller scale tests)
	REQUIRE_TEST_DB=1 TEST_DATABASE_URL=$(TEST_DATABASE_URL) $(GO) test -race -short -count=1 ./...

test-db: ## start a throwaway Postgres 16 on :5433 for tests (needs Docker)
	docker run -d --rm --name jobs-test-pg -p 5433:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16-alpine -c fsync=off -c max_connections=300
	@echo 'export TEST_DATABASE_URL="postgres://postgres@127.0.0.1:5433/postgres?sslmode=disable"'

lint: vet ## gofmt check + go vet (+ golangci-lint when installed)
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed on:"; gofmt -l .; exit 1; }
	@command -v golangci-lint >/dev/null && golangci-lint run ./... || echo "golangci-lint not installed; skipped (CI runs v2.15.0)"

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .

tidy:
	$(GO) mod tidy

up: ## start the full local stack
	$(COMPOSE) up --build -d
	@echo "API: $(API_URL)  Grafana: http://localhost:3000 (admin/admin)  Prometheus: http://localhost:9090  Postgres: localhost:55432 (jobs/jobs)"
	@echo "Create an API key with: make key"

down: ## stop the stack and delete volumes
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

key: ## mint an API key for tenant "demo" (stack must be up)
	@$(COMPOSE) run --rm --no-deps jobctl create-key -tenant demo -name cli

demo: ## submit a flaky job and watch it retry to success (JOBCTL_API_KEY must be set)
	@id=$$($(BIN)/jobctl submit -type flaky -payload '{"succeed_on_attempt":3}' | sed -n 's/.*"id": "\(.*\)".*/\1/p'); \
	 echo "submitted $$id"; $(BIN)/jobctl get -wait $$id

migrate: build ## apply migrations (DATABASE_URL required)
	$(BIN)/jobctl migrate up

load-test: build ## load test the running stack (JOBCTL_API_KEY required)
	@if command -v k6 >/dev/null; then k6 run -e API_URL=$(API_URL) -e API_KEY=$$JOBCTL_API_KEY test/load/k6.js; \
	 else $(GO) run ./test/load/loadgen -api $(API_URL) -key $$JOBCTL_API_KEY -n 20000 -c 64; fi

bench: build ## reproduce the README benchmark numbers (needs ADMIN_DATABASE_URL)
	./scripts/bench.sh

chaos: build ## kill -9 workers and schedulers mid-run; fail if any job is lost or cron double-fires
	@test -n "$$ADMIN_DATABASE_URL" || { echo 'set ADMIN_DATABASE_URL, e.g. (compose stack) postgres://jobs:jobs@localhost:55432/postgres?sslmode=disable'; exit 1; }
	$(GO) run ./test/chaos -bin $(BIN)

smoke: ## end-to-end check of the running compose stack (make up first)
	./scripts/smoke.sh

chaos-docker: ## docker kill workers and schedulers mid-run on the compose stack (make up first)
	./scripts/chaos-docker.sh

kind-up: ## kind cluster + KEDA + metrics-server + the chart (needs `make docker-build IMAGE_TAG=dev REGISTRY=jobscheduler` first)
	./scripts/kind-up.sh

k8s-verify: ## verify KEDA, crashes, rolling deploys, node drains and NetworkPolicies on the kind cluster (FULL=1 adds a 2.5 min test)
	./scripts/k8s-verify.sh

argocd-verify: ## GitOps check: Argo CD deploys the chart, heals drift, rolls out and rolls back
	./scripts/argocd-verify.sh

kind-down: ## delete the kind cluster
	kind delete cluster --name js

docker-build: ## build the four distroless images (needs the docker buildx plugin: the Dockerfile uses BuildKit)
	@for c in $(CMDS); do docker build -f deploy/docker/Dockerfile --build-arg APP=$$c -t $(REGISTRY)/$$c:$(IMAGE_TAG) . || exit 1; done

helm-lint: ## lint the Helm chart (default values and with every optional feature on)
	helm lint deploy/helm/jobscheduler --set database.existingSecret=x
	helm lint deploy/helm/jobscheduler --set database.existingSecret=x --set keda.enabled=true \
	  --set networkPolicy.enabled=true --set networkPolicy.postgres.cidr=10.0.0.0/16 \
	  --set serviceMonitor.enabled=true --set api.ingress.enabled=true

k8s-validate: ## render the chart with the prod values and validate every manifest (incl. KEDA/ServiceMonitor CRDs)
	helm template js deploy/helm/jobscheduler -f deploy/argocd/values-prod.yaml \
	  | kubeconform -strict -summary -ignore-missing-schemas -schema-location default \
	    -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
	  | tee /dev/stderr | grep -qE 'Valid: [1-9]' # also fail if nothing at all was validated

rules-test: ## check the Prometheus rules and run their unit tests (promtool)
	promtool check rules deploy/prometheus/alerts.yml deploy/prometheus/rules.yml
	promtool test rules deploy/prometheus/tests.yml

validate: lint helm-lint k8s-validate rules-test ## everything that can be checked without a container runtime
	amtool check-config deploy/alertmanager/alertmanager.yml
	hadolint deploy/docker/Dockerfile
	actionlint .github/workflows/*.yml
	$(GO) test -race -count=1 ./test/helm/

dev-up: build ## run api+worker+scheduler as local processes (no Docker); needs DATABASE_URL
	./scripts/dev.sh up

dev-down:
	./scripts/dev.sh down

clean:
	rm -rf $(BIN) .dev
