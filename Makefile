# Point-in-Time De-Identified Database Replica (PoC)
#
# The inner loop is a set of readable commands. When something breaks you can
# copy any recipe out of here and run it by hand. All Python runs through
# `uv run`, never bare python3/pytest.

# ---- config (override on the CLI, e.g. `make up MEMORY=12288`) -------------
PROFILE    ?= pit
NAMESPACE  ?= pit-poc
CPUS       ?= 4
MEMORY     ?= 8192
DRIVER     ?= docker
RELEASE    ?= pit
CHART      ?= charts/pit
IMG_PREFIX ?= pit
TAG        ?= dev

# source-pg's fullname helper resolves to <release>-source-pg, which names both
# the StatefulSet and the ClusterIP Service.
STS        ?= $(RELEASE)-source-pg
PGUSER     ?= pit
PGDATABASE ?= pit
# Local port `forward` maps source-pg onto; the loadgen DSN defaults here too.
LOCAL_PORT ?= 5432

KUBECTL := kubectl --context $(PROFILE) -n $(NAMESPACE)
HELM    := helm
UV      := uv run

# component -> docker build context. Dockerfiles arrive in later milestones
# (connect=M3, deid=M4, pitctl=M5); build steps skip cleanly until then.
CONNECT_CTX := connect
DEID_CTX    := deid
PITCTL_CTX  := pit

# svc:localport:remoteport for `forward`. Services not yet deployed are skipped.
FORWARDS := pit-console:8080:8080 pit-redpanda:8081:8081 connect:8083:8083 $(STS):$(LOCAL_PORT):5432 sink-pg:5433:5432

.DEFAULT_GOAL := help

# ---- meta ------------------------------------------------------------------
.PHONY: help
help: ## Show this help
	@echo "Point-in-Time De-Identified Database Replica (PoC)"
	@echo "Usage: make <target>"
	@echo
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---- cluster lifecycle -----------------------------------------------------
.PHONY: start
start: ## Start the pit minikube profile (idempotent)
	@if minikube status -p $(PROFILE) >/dev/null 2>&1; then \
	  echo "minikube profile '$(PROFILE)' already running"; \
	else \
	  minikube start -p $(PROFILE) --driver=$(DRIVER) --cpus=$(CPUS) --memory=$(MEMORY); \
	fi

.PHONY: namespace
namespace: ## Create the pit-poc namespace (idempotent)
	@kubectl --context $(PROFILE) create namespace $(NAMESPACE) --dry-run=client -o yaml | kubectl --context $(PROFILE) apply -f -

.PHONY: repos
repos: ## Register the Redpanda chart repo (idempotent)
	@$(HELM) repo add redpanda https://charts.redpanda.com --force-update >/dev/null

.PHONY: deps
deps: repos ## Fetch chart dependencies (Redpanda + the in-tree subcharts)
	$(HELM) dependency build $(CHART)

.PHONY: lint
lint: deps ## helm lint the umbrella chart
	$(HELM) lint $(CHART) -f $(CHART)/values-local.yaml

.PHONY: install
install: deps namespace ## Install/upgrade the pit umbrella chart and wait for readiness
	$(HELM) upgrade --install $(RELEASE) $(CHART) -n $(NAMESPACE) --create-namespace \
	  -f $(CHART)/values-local.yaml \
	  --wait --timeout 10m

.PHONY: up
up: start namespace install ## Bring up the cluster and deploy the stack
	@echo "Up. Next: 'make verify-all', or 'make forward' then curl localhost:8081/subjects"

.PHONY: uninstall
uninstall: ## Remove the release (PVCs survive; see clean)
	$(HELM) uninstall $(RELEASE) -n $(NAMESPACE)

.PHONY: clean
clean: ## Remove the release and its PVCs (init SQL re-runs on next install)
	-$(HELM) uninstall $(RELEASE) -n $(NAMESPACE)
	-$(KUBECTL) delete pvc -l app.kubernetes.io/instance=$(RELEASE)

.PHONY: nuke
nuke: ## Delete the profile and ALL volumes (destroys PVCs so init SQL re-runs)
	minikube delete -p $(PROFILE)

# ---- verification ----------------------------------------------------------
.PHONY: verify
verify: ## DATA-699 acceptance check: pod ready and wal_level=logical
	@set -euo pipefail; \
	echo "==> waiting for $(STS) to be ready"; \
	$(KUBECTL) rollout status sts/$(STS) --timeout=5m; \
	echo "==> settings"; \
	$(KUBECTL) exec sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -c \
		"select name, setting from pg_settings where name in ('wal_level','max_replication_slots','max_wal_senders','wal_keep_size');"; \
	level=$$($(KUBECTL) exec sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -Atc 'show wal_level;' | tr -d '\r'); \
	if [[ "$$level" != "logical" ]]; then \
		echo "FAIL: wal_level is '$$level', expected 'logical'"; exit 1; \
	fi; \
	echo "==> proving a logical slot can actually be created"; \
	$(KUBECTL) exec sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -Atc \
		"select pg_create_logical_replication_slot('pit_verify','pgoutput');" >/dev/null; \
	$(KUBECTL) exec sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -Atc \
		"select pg_drop_replication_slot('pit_verify');" >/dev/null; \
	echo "PASS: wal_level=logical and logical slot creation works"

.PHONY: verify-schema
verify-schema: ## DATA-700 acceptance check: tables, REPLICA IDENTITY FULL, history triggers
	@set -euo pipefail; \
	echo "==> captured tables"; \
	$(KUBECTL) exec sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -c 'table pit_captured_tables;'; \
	echo "==> running scripts/verify-schema.sql"; \
	$(KUBECTL) exec -i sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) --no-psqlrc -f - < scripts/verify-schema.sql

.PHONY: verify-all
verify-all: verify verify-schema ## Both acceptance checks against the cluster

.PHONY: verify-docker
verify-docker: ## Same checks without a cluster: run the rendered chart under plain docker
	@./scripts/verify-conf-docker.sh

# ---- images ----------------------------------------------------------------
.PHONY: build
build: build-connect build-deid build-pitctl ## Build the connect, deid and pitctl images

.PHONY: build-connect build-deid build-pitctl
build-connect:
	@if [ -f $(CONNECT_CTX)/Dockerfile ]; then \
	  docker build -t $(IMG_PREFIX)/connect:$(TAG) $(CONNECT_CTX); \
	else echo "skip connect image: $(CONNECT_CTX)/Dockerfile not present yet (M3)"; fi
build-deid:
	@if [ -f $(DEID_CTX)/Dockerfile ]; then \
	  docker build -t $(IMG_PREFIX)/deid:$(TAG) $(DEID_CTX); \
	else echo "skip deid image: $(DEID_CTX)/Dockerfile not present yet (M4)"; fi
build-pitctl:
	@if [ -f $(PITCTL_CTX)/Dockerfile ]; then \
	  docker build -t $(IMG_PREFIX)/pitctl:$(TAG) $(PITCTL_CTX); \
	else echo "skip pitctl image: $(PITCTL_CTX)/Dockerfile not present yet (M5)"; fi

.PHONY: load
load: ## Load built images into the minikube profile
	@for img in connect deid pitctl; do \
	  if docker image inspect $(IMG_PREFIX)/$$img:$(TAG) >/dev/null 2>&1; then \
	    echo "loading $(IMG_PREFIX)/$$img:$(TAG)"; \
	    minikube image load -p $(PROFILE) $(IMG_PREFIX)/$$img:$(TAG); \
	  else echo "skip $$img: image $(IMG_PREFIX)/$$img:$(TAG) not built yet"; fi; \
	done

.PHONY: reload
reload: build load install ## Rebuild images, load them, and roll the affected deployments
	@for d in connect deid pit-tail; do \
	  if $(KUBECTL) get deploy $$d >/dev/null 2>&1; then \
	    $(KUBECTL) rollout restart deploy/$$d; \
	  fi; \
	done

# ---- access ----------------------------------------------------------------
.PHONY: forward
forward: ## Port-forward console/registry/connect/postgres (skips undeployed services)
	@echo "Forwarding (Ctrl-C to stop):"; \
	pids=""; \
	for f in $(FORWARDS); do \
	  svc=$${f%%:*}; rest=$${f#*:}; lp=$${rest%%:*}; rp=$${rest##*:}; \
	  if $(KUBECTL) get svc $$svc >/dev/null 2>&1; then \
	    echo "  $$svc  localhost:$$lp -> $$rp"; \
	    $(KUBECTL) port-forward svc/$$svc $$lp:$$rp >/dev/null 2>&1 & \
	    pids="$$pids $$!"; \
	  else \
	    echo "  $$svc  (not deployed yet — pending)"; \
	  fi; \
	done; \
	trap "kill $$pids 2>/dev/null" INT TERM EXIT; \
	echo "  ready — Ctrl-C to stop"; \
	wait

.PHONY: psql
psql: ## Open a psql shell in the source Postgres pod
	$(KUBECTL) exec -it sts/$(STS) -- psql -U $(PGUSER) -d $(PGDATABASE)

# ---- load generation -------------------------------------------------------
.PHONY: loadgen-deps
loadgen-deps: ## Create the loadgen venv and install pinned dependencies
	cd loadgen && $(UV) sync

.PHONY: seed
seed: ## Populate the clinic schema with synthetic data (needs `make forward`)
	cd loadgen && $(UV) python -m loadgen.seed --reset

.PHONY: seed-fingerprint
seed-fingerprint: ## Digest whatever is currently in the clinic tables
	@cd loadgen && $(UV) python -m loadgen.seed --fingerprint

.PHONY: seed-test
seed-test: ## Unit tests for the generator; no database required
	cd loadgen && $(UV) pytest

.PHONY: seed-verify
seed-verify: ## DATA-701 acceptance check: same seed, same rows, twice over
	@set -euo pipefail; \
	cd loadgen; \
	echo "==> generator tests (no database)"; \
	PIT_TEST_DSN="$${PIT_DSN:-host=127.0.0.1 port=$(LOCAL_PORT) user=$(PGUSER) password=pit-dev-password dbname=$(PGDATABASE)}" \
		$(UV) pytest; \
	echo "==> seeding twice and comparing what landed"; \
	$(UV) python -m loadgen.seed --reset --quiet >/dev/null; \
	first=$$($(UV) python -m loadgen.seed --fingerprint --quiet); \
	$(UV) python -m loadgen.seed --reset --quiet >/dev/null; \
	second=$$($(UV) python -m loadgen.seed --fingerprint --quiet); \
	generated=$$($(UV) python -m loadgen.seed --dry-run --quiet); \
	echo "    run 1     $$first"; \
	echo "    run 2     $$second"; \
	echo "    generated $$generated"; \
	if [[ "$$first" != "$$second" ]]; then \
		echo "FAIL: two runs with the same seed produced different rows"; exit 1; \
	fi; \
	if [[ "$$first" != "$$generated" ]]; then \
		echo "FAIL: the stored rows differ from the generated ones"; exit 1; \
	fi; \
	$(UV) python -m loadgen.seed --fingerprint 2>&1 >/dev/null; \
	echo "PASS: the seed is reproducible and the row counts match the config"

# ---- logs ------------------------------------------------------------------
.PHONY: logs-source-pg logs-deid logs-connect logs-tail
logs-source-pg: ## Tail the source Postgres logs
	$(KUBECTL) logs sts/$(STS) -f --tail=100
logs-deid: ## Tail the de-id transformer logs
	$(KUBECTL) logs -l app.kubernetes.io/name=deid -f --tail=100
logs-connect: ## Tail the Kafka Connect logs
	$(KUBECTL) logs -l app.kubernetes.io/name=connect -f --tail=100
logs-tail: ## Tail the pit-tail applier logs
	$(KUBECTL) logs -l app.kubernetes.io/name=pit-tail -f --tail=100

# ---- dev -------------------------------------------------------------------
.PHONY: test
test: ## Run Python unit tests (uv run pytest)
	$(UV) pytest

.PHONY: template
template: deps ## Render the chart locally (no cluster needed)
	$(HELM) template $(RELEASE) $(CHART) -f $(CHART)/values-local.yaml -n $(NAMESPACE)
