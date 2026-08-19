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
# The sink: plain Postgres, its own StatefulSet and credentials. No replication
# role and no publication -- nothing decodes this database's WAL.
SINK_STS        ?= $(RELEASE)-sink-pg
SINK_PGUSER     ?= pit
SINK_PGDATABASE ?= pit_sink
# Local port `forward` maps source-pg onto; the loadgen DSN defaults here too.
LOCAL_PORT ?= 5432

# The de-id policy is a single file in this repo and the deid subchart has no
# default for it: `--set-file` supplies it to every helm invocation that renders
# the umbrella. A second copy inside the chart would be a second artifact
# claiming to be the audited one. Rendering without this fails with a message.
POLICY      ?= deid/policy/clinic.yml
POLICY_FLAG := --set-file deid.policy.contents=$(POLICY)

KUBECTL := kubectl --context $(PROFILE) -n $(NAMESPACE)
HELM    := helm
UV      := uv run

# component -> where its Dockerfile lives. Dockerfiles arrive in later milestones
# (connect=M3, deid=M4, pitctl=M5); build steps skip cleanly until then.
# deid builds with the repo root as its context (it needs the `deid` package, and
# Docker cannot COPY above the context); see .dockerignore.
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
	$(HELM) lint $(CHART) -f $(CHART)/values-local.yaml $(POLICY_FLAG)

.PHONY: install
install: deps namespace ## Install/upgrade the pit umbrella chart and wait for readiness
	$(HELM) upgrade --install $(RELEASE) $(CHART) -n $(NAMESPACE) --create-namespace \
	  -f $(CHART)/values-local.yaml $(POLICY_FLAG) \
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

.PHONY: verify-sink
verify-sink: ## Sink acceptance check: pod ready, database present, user is a superuser
	@set -euo pipefail; \
	echo "==> waiting for $(SINK_STS) to be ready"; \
	$(KUBECTL) rollout status sts/$(SINK_STS) --timeout=5m; \
	echo "==> databases"; \
	$(KUBECTL) exec sts/$(SINK_STS) -- \
		psql -U $(SINK_PGUSER) -d $(SINK_PGDATABASE) -c \
		'select datname, pg_encoding_to_char(encoding) as encoding from pg_database where not datistemplate order by datname;'; \
	super=$$($(KUBECTL) exec sts/$(SINK_STS) -- \
		psql -U $(SINK_PGUSER) -d $(SINK_PGDATABASE) -Atc \
		"select rolsuper from pg_roles where rolname = '$(SINK_PGUSER)';" | tr -d '\r'); \
	if [[ "$$super" != "t" ]]; then \
		echo "FAIL: '$(SINK_PGUSER)' is not a superuser (rolsuper=$$super)"; exit 1; \
	fi; \
	echo "==> it can create and drop a database, which is what M5 needs of it"; \
	$(KUBECTL) exec sts/$(SINK_STS) -- \
		psql -U $(SINK_PGUSER) -d postgres -q -c 'DROP DATABASE IF EXISTS pit_verify_sink;' \
		-c 'CREATE DATABASE pit_verify_sink;' -c 'DROP DATABASE pit_verify_sink;'; \
	echo "==> no CDC machinery here, deliberately"; \
	level=$$($(KUBECTL) exec sts/$(SINK_STS) -- \
		psql -U $(SINK_PGUSER) -d $(SINK_PGDATABASE) -Atc 'show wal_level;' | tr -d '\r'); \
	echo "    wal_level=$$level (replica is expected; the sink decodes nothing)"; \
	echo "PASS: sink is up, '$(SINK_PGDATABASE)' exists, '$(SINK_PGUSER)' is a superuser and can create databases"

.PHONY: verify-all
verify-all: verify-m1 verify verify-schema verify-sink ## All acceptance checks against the cluster

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
	  docker build -f $(DEID_CTX)/Dockerfile -t $(IMG_PREFIX)/deid:$(TAG) .; \
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
	    minikube -p $(PROFILE) ssh -- docker rmi -f $(IMG_PREFIX)/$$img:$(TAG) >/dev/null 2>&1 || true; \
	    minikube image load -p $(PROFILE) $(IMG_PREFIX)/$$img:$(TAG); \
	  else echo "skip $$img: image $(IMG_PREFIX)/$$img:$(TAG) not built yet"; fi; \
	done
# The `docker rmi -f` above is load-bearing, and its absence fails in the worst
# available way. `minikube image load` untags the old image inside the node
# before loading the new one, and `docker rmi` refuses while a running container
# references it -- so as soon as a pod is up on :dev, every subsequent load
# fails, prints nothing alarming, and the next `rollout restart` starts the pod
# on the *old* code. `rmi -f` untags without touching the running container,
# which is enough. Untagging an image nothing is using is harmless, and `|| true`
# covers a profile that is not running yet.

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

.PHONY: psql-sink
psql-sink: ## Open a psql shell in the sink Postgres pod
	$(KUBECTL) exec -it sts/$(SINK_STS) -- psql -U $(SINK_PGUSER) -d $(SINK_PGDATABASE)

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
# POLICY itself is defined at the top, next to the --set-file flag that installs it.

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

# ---- the de-id transformer -------------------------------------------------
# The transformer is the only component that touches Kafka or the registry, and
# the broker's Kafka API is deliberately not port-forwarded (clients run in the
# cluster and address pit-redpanda:9093). So these targets run inside the
# transformer's own pod, which is where the policy is mounted and the salt is.
DEID_DEPLOY ?= $(RELEASE)-deid

.PHONY: deid-dry-run
deid-dry-run: ## Startup only, in-cluster: derive the clean schemas, create the topics, register
	$(KUBECTL) exec deploy/$(DEID_DEPLOY) -- /app/entrypoint.sh --dry-run

.PHONY: deid-verify
deid-verify: ## Check the cleaned topics: schemas, compatibility, topic config, timestamps
	$(KUBECTL) exec deploy/$(DEID_DEPLOY) -- /app/entrypoint.sh --verify

.PHONY: deid-check
deid-check: ## DATA-712 acceptance check: the transformer's promises, in and out of cluster
	@set -euo pipefail; \
	echo "==> transform tests: the key agrees with the value, the timestamp is the commit time"; \
	cd deid && $(UV) pytest tests/test_envelope.py tests/test_runner.py; cd ..; \
	echo "==> waiting for $(DEID_DEPLOY)"; \
	$(KUBECTL) rollout status deploy/$(DEID_DEPLOY) --timeout=5m; \
	echo "==> any halted topics? (a HALT is the design working, and it is still news)"; \
	$(KUBECTL) logs deploy/$(DEID_DEPLOY) --tail=2000 | grep -E '^.*(HALT|HALTED)' || echo "    none"; \
	echo "==> in-cluster acceptance checks against the live broker and registry"; \
	$(KUBECTL) exec deploy/$(DEID_DEPLOY) -- /app/entrypoint.sh --verify

# The one criterion that needs the source database changed under a running
# transformer. Adds a column with no policy rule, waits for the halt, and shows
# that the other tables kept flowing. `make deid-uncover-fix` puts it back.
UNCOVERED_COLUMN ?= insurance_id

.PHONY: deid-uncover
deid-uncover: ## Add an uncovered column to patients: one topic halts, the rest keep flowing
	@set -euo pipefail; \
	echo "==> ALTER TABLE patients ADD COLUMN $(UNCOVERED_COLUMN) text"; \
	$(KUBECTL) exec sts/$(STS) -- psql -U $(PGUSER) -d $(PGDATABASE) -q -c \
	  'ALTER TABLE patients ADD COLUMN IF NOT EXISTS $(UNCOVERED_COLUMN) text;'; \
	echo "==> touching a row so Debezium registers the new schema and emits it"; \
	$(KUBECTL) exec sts/$(STS) -- psql -U $(PGUSER) -d $(PGDATABASE) -q -c \
	  "update patients set $(UNCOVERED_COLUMN) = 'AETNA-99' where patient_id = (select min(patient_id) from patients);" \
	  -c "update providers set specialty = specialty where provider_id = (select min(provider_id) from providers);"; \
	echo "==> waiting for the halt (up to 60s)"; \
	for i in $$(seq 1 30); do \
	  if $(KUBECTL) logs deploy/$(DEID_DEPLOY) --tail=500 | grep -q 'HALT public.patients'; then break; fi; \
	  sleep 2; \
	done; \
	$(KUBECTL) logs deploy/$(DEID_DEPLOY) --tail=500 | grep -A2 'HALT public.patients' || \
	  { echo "FAIL: patients did not halt"; exit 1; }; \
	echo; \
	echo "==> high watermarks: patients is frozen, the rest keep moving"; \
	for t in patients providers appointments claims notes; do \
	  printf '    %-28s %s\n' "clean.public.$$t" \
	    "$$($(KUBECTL) exec sts/$(RELEASE)-redpanda -c redpanda -- \
	       rpk topic describe -p clean.public.$$t 2>/dev/null | tail -1 | awk '{print $$NF}')"; \
	done; \
	echo "PASS: one topic halted by name; add $(UNCOVERED_COLUMN) to $(POLICY) and 'make install' to clear it"

# Dropping the column is NOT enough to clear the halt, and that is Debezium's
# doing rather than the transformer's: it keeps its cached table schema after a
# DROP COLUMN and goes on emitting the column as null, so the raw subject still
# carries it and the derivation still refuses. `make clean && make up` is the
# reliable reset. Kept because dropping the column is still the right first step.
.PHONY: deid-uncover-fix
deid-uncover-fix: ## Drop the column deid-uncover added (see the note: `make clean && make up` to fully reset)
	$(KUBECTL) exec sts/$(STS) -- psql -U $(PGUSER) -d $(PGDATABASE) -q -c \
	  'ALTER TABLE patients DROP COLUMN IF EXISTS $(UNCOVERED_COLUMN);'
	$(KUBECTL) rollout restart deploy/$(DEID_DEPLOY)
	@echo "Note: Debezium does not re-register after a DROP COLUMN, so public.patients"
	@echo "may stay halted on it. 'make clean && make up' resets the pipeline."

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
.PHONY: logs-source-pg logs-sink-pg logs-deid logs-connect logs-tail
logs-source-pg: ## Tail the source Postgres logs
	$(KUBECTL) logs sts/$(STS) -f --tail=100
logs-sink-pg: ## Tail the sink Postgres logs
	$(KUBECTL) logs sts/$(SINK_STS) -f --tail=100
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
	$(HELM) template $(RELEASE) $(CHART) -f $(CHART)/values-local.yaml $(POLICY_FLAG) -n $(NAMESPACE)
