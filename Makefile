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
# Role Debezium connects as, and the FOR ALL TABLES publication it decodes
# through. Both are created by the chart's initdb scripts; see source-pg values.
REPLUSER    ?= debezium
PUBLICATION ?= dbz_publication
# Local port `forward` maps source-pg onto; the loadgen DSN defaults here too.
LOCAL_PORT ?= 5432

KUBECTL := kubectl --context $(PROFILE) -n $(NAMESPACE)
HELM    := helm
UV      := uv run

# component -> docker build context. Dockerfiles arrive in later milestones
# (connect=M3, deid=M4, pitctl=M5); build steps skip cleanly until then.
CONNECT_CTX := images/connect
DEID_CTX    := images/deid
PITCTL_CTX  := images/pitctl

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
up: start namespace build load install ## Bring up the cluster and deploy the stack (builds+loads local images)
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
verify: ## DATA-699 acceptance check: wal_level=logical, publication, before images in the WAL
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
	echo "==> the publication Debezium decodes through"; \
	$(KUBECTL) exec sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -c \
		'select pubname, puballtables, pubinsert, pubupdate, pubdelete from pg_publication;'; \
	puball=$$($(KUBECTL) exec sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -Atc \
		"select puballtables from pg_publication where pubname = '$(PUBLICATION)';" | tr -d '\r'); \
	if [[ "$$puball" != "t" ]]; then \
		echo "FAIL: publication '$(PUBLICATION)' missing or not FOR ALL TABLES (puballtables=$$puball)"; exit 1; \
	fi; \
	echo "==> the debezium role can open its own slot without prior setup"; \
	super=$$($(KUBECTL) exec sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -Atc \
		"select rolreplication and rolsuper from pg_roles where rolname = '$(REPLUSER)';" | tr -d '\r'); \
	if [[ "$$super" != "t" ]]; then \
		echo "FAIL: role '$(REPLUSER)' lacks REPLICATION or SUPERUSER"; exit 1; \
	fi; \
	echo "==> running scripts/verify-wal.sql"; \
	$(KUBECTL) exec -i sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) --no-psqlrc -f - < scripts/verify-wal.sql

.PHONY: verify-schema
verify-schema: ## DATA-700 acceptance check: tables, foreign keys, full replica identity
	@set -euo pipefail; \
	echo "==> running scripts/verify-schema.sql"; \
	$(KUBECTL) exec -i sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) --no-psqlrc -f - < scripts/verify-schema.sql; \
	echo "==> replicated tables"; \
	$(KUBECTL) exec sts/$(STS) -- \
		psql -U $(PGUSER) -d $(PGDATABASE) -c 'table pit_replicated_tables;'

.PHONY: verify-all
verify-all: verify-m1 verify verify-schema ## All acceptance checks against the cluster

.PHONY: verify-m1
verify-m1: ## DATA-698 acceptance check: broker, registry and console reachable
	@hack/verify.sh

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
	@for d in $(RELEASE)-connect $(RELEASE)-deid pit-tail; do \
	  if $(KUBECTL) get deploy $$d >/dev/null 2>&1; then \
	    $(KUBECTL) rollout restart deploy/$$d; \
	  fi; \
	done

# ---- access ----------------------------------------------------------------
.PHONY: forward
forward: ## Port-forward console/registry/connect/postgres in the background
	@hack/forward.sh start

.PHONY: forward-stop
forward-stop: ## Stop the background port-forwards
	@hack/forward.sh stop

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

# ---- de-identification policy ----------------------------------------------
# The policy file is the auditable artifact: `policy-check` parses it and prints
# what it actually says, so reviewing a change is reading rules rather than YAML.
POLICY ?= deid/policy/clinic.yml

.PHONY: deid-deps
deid-deps: ## Create the deid venv and install pinned dependencies
	cd deid && $(UV) sync

.PHONY: policy-check
policy-check: ## Validate the de-id policy and print its rules
	@cd deid && $(UV) python -m deid.policy ../$(POLICY)

.PHONY: ops-demo
ops-demo: ## Show what each op does to a value and to its Avro type
	@cd deid && $(UV) python -m deid.ops

.PHONY: schema-derive
schema-derive: ## Derive and print the clean Avro schema for public.patients
	@cd deid && $(UV) python -m deid.schema ../$(POLICY) --namespace clean.public.patients

.PHONY: deid-test
deid-test: ## Unit tests for the de-id transformer; no cluster required
	cd deid && $(UV) pytest

.PHONY: ops-check
ops-check: ## Acceptance check: the two halves of every op agree, across processes
	@set -euo pipefail; \
	cd deid; \
	echo "==> op tests, including the conformance property"; \
	$(UV) pytest tests/test_avro.py tests/test_ops.py; \
	echo "==> the same surrogate out of two separate processes, two hash seeds"; \
	script='from datetime import date; from deid import ops, policy; \
k = ops.Keys(salt=b"acceptance-check-salt-not-a-real-one", reference_date=date(2026, 8, 1)); \
r = policy.Rule(table="public.patients", column="mrn", op=policy.Hmac(domain="patient")); \
print(ops.build(r, "string", keys=k).apply("  MRN-000482 "))'; \
	first=$$(PYTHONHASHSEED=0 $(UV) python -c "$$script"); \
	second=$$(PYTHONHASHSEED=99991 $(UV) python -c "$$script"); \
	echo "    run 1  $$first"; \
	echo "    run 2  $$second"; \
	if [[ "$$first" != "$$second" ]]; then \
		echo "FAIL: the same input under the same salt gave two surrogates"; exit 1; \
	fi; \
	echo "PASS: every op's two halves agree, and hmac is stable across processes"

# Where the registry answers. `make forward` maps it onto localhost:8081.
REGISTRY_URL ?= http://localhost:8081

.PHONY: schema-check
schema-check: ## DATA-711 acceptance check: derived clean schemas register (needs `make forward`)
	@set -euo pipefail; \
	echo "==> schema derivation tests (no registry)"; \
	cd deid && $(UV) pytest tests/test_schema.py; cd ..; \
	echo "==> registering the derived schemas against the live registry"; \
	uv run --with fastavro --with requests --with PyYAML \
	  python scripts/register-clean-schema.py --registry $(REGISTRY_URL) --policy $(POLICY)

# ---- churn -----------------------------------------------------------------
# The seed gives one state; churn gives a timeline with distinguishable points in
# it. Bounded three ways (duration, transactions, ledger rows) because cleaned
# topics run at infinite retention on a laptop PVC.
CHURN_DURATION ?= 5m
CHURN_RATE     ?= 2

.PHONY: churn
churn: ## Apply continuous inserts/updates/deletes to the seeded schema (needs `make forward`)
	cd loadgen && $(UV) python -m loadgen --duration $(CHURN_DURATION) --rate $(CHURN_RATE)

.PHONY: churn-verify
churn-verify: ## Check the ledger: append-only, monotonic tx_at, replays to the live tables
	@cd loadgen && $(UV) python -m loadgen --verify

.PHONY: churn-check
churn-check: ## DATA-702 acceptance check: rows move, and the ledger records every mutation
	@set -euo pipefail; \
	cd loadgen; \
	echo "==> churn tests, including the ones that need a database"; \
	PIT_TEST_DSN="$${PIT_DSN:-host=127.0.0.1 port=$(LOCAL_PORT) user=$(PGUSER) password=pit-dev-password dbname=$(PGDATABASE)}" \
		$(UV) pytest tests/test_churn.py; \
	echo "==> a clean seed, then a minute of churn against it"; \
	$(UV) python -m loadgen.seed --reset --quiet >/dev/null; \
	$(UV) python -m loadgen --duration 60s --rate 4 --quiet; \
	echo "PASS: the timeline has distinguishable points and the ledger replays to the live tables"

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
