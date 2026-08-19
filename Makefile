SHELL := /bin/bash
.DEFAULT_GOAL := help

RELEASE   ?= pit
NAMESPACE ?= pit
CHART     ?= charts/pit
STS       ?= $(RELEASE)-source-pg
PGUSER    ?= pit
PGDATABASE?= pit
LOCAL_PORT?= 5432

.PHONY: help
help: ## List targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## ---- cluster ----

.PHONY: cluster
cluster: ## Start the local minikube cluster
	minikube status >/dev/null 2>&1 || minikube start

.PHONY: deps
deps: ## Resolve chart dependencies
	helm dependency build $(CHART)

.PHONY: lint
lint: ## helm lint the umbrella chart
	helm lint $(CHART)

.PHONY: template
template: ## Render manifests to stdout
	helm template $(RELEASE) $(CHART) --namespace $(NAMESPACE)

.PHONY: install
install: ## Install or upgrade the release and wait for readiness
	helm upgrade --install $(RELEASE) $(CHART) \
		--namespace $(NAMESPACE) --create-namespace \
		--wait --timeout 5m

.PHONY: uninstall
uninstall: ## Remove the release (PVCs survive; see clean)
	helm uninstall $(RELEASE) --namespace $(NAMESPACE)

.PHONY: clean
clean: ## Remove the release and its PVCs
	-helm uninstall $(RELEASE) --namespace $(NAMESPACE)
	-kubectl delete pvc -n $(NAMESPACE) -l app.kubernetes.io/instance=$(RELEASE)

## ---- verification ----

.PHONY: verify
verify: ## DATA-699 acceptance check: pod ready and wal_level=logical
	@set -euo pipefail; \
	echo "==> waiting for $(STS) to be ready"; \
	kubectl rollout status sts/$(STS) -n $(NAMESPACE) --timeout=5m; \
	echo "==> settings"; \
	kubectl exec -n $(NAMESPACE) sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -c \
		"select name, setting from pg_settings where name in ('wal_level','max_replication_slots','max_wal_senders','wal_keep_size');"; \
	level=$$(kubectl exec -n $(NAMESPACE) sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -Atc 'show wal_level;' | tr -d '\r'); \
	if [[ "$$level" != "logical" ]]; then \
		echo "FAIL: wal_level is '$$level', expected 'logical'"; exit 1; \
	fi; \
	echo "==> proving a logical slot can actually be created"; \
	kubectl exec -n $(NAMESPACE) sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -Atc \
		"select pg_create_logical_replication_slot('pit_verify','pgoutput');" >/dev/null; \
	kubectl exec -n $(NAMESPACE) sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -Atc \
		"select pg_drop_replication_slot('pit_verify');" >/dev/null; \
	echo "PASS: wal_level=logical and logical slot creation works"

.PHONY: verify-schema
verify-schema: ## DATA-700 acceptance check: tables, REPLICA IDENTITY FULL, history triggers
	@set -euo pipefail; \
	echo "==> captured tables"; \
	kubectl exec -n $(NAMESPACE) sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -c 'table pit_captured_tables;'; \
	echo "==> running scripts/verify-schema.sql"; \
	kubectl exec -i -n $(NAMESPACE) sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) --no-psqlrc -f - < scripts/verify-schema.sql

.PHONY: verify-all
verify-all: verify verify-schema ## Both acceptance checks against the cluster

.PHONY: verify-docker
verify-docker: ## Same checks without a cluster: run the rendered chart under plain docker
	@./scripts/verify-conf-docker.sh

## ---- day-to-day ----

.PHONY: psql
psql: ## Open a psql shell in the pod
	kubectl exec -it -n $(NAMESPACE) sts/$(STS) -- psql -U $(PGUSER) -d $(PGDATABASE)

.PHONY: port-forward
port-forward: ## Forward Postgres to localhost:$(LOCAL_PORT)
	kubectl port-forward -n $(NAMESPACE) svc/$(STS) $(LOCAL_PORT):5432

.PHONY: logs
logs: ## Tail Postgres logs
	kubectl logs -n $(NAMESPACE) sts/$(STS) -f
