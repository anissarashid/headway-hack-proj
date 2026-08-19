# Point-in-Time De-Identified Database Replica (PoC)

A local, reproducible way to stand up a Postgres database that mirrors a
primary **as of an arbitrary point in time**, with all PHI/PII removed. The
source database is synthetic — **no real PHI enters this project at any point.**

## Pipeline

```
source Postgres ──(Debezium CDC)──▶ raw.*  topics ──(de-id transformer)──▶ clean.* topics ──(applier)──▶ sink Postgres
   (M2)                (M3)          (Redpanda)          (M4)               (Redpanda)        (M5/M6/M7)      (pit_base + PIT dbs)
```

Two load-bearing ideas:

1. **A point in time is an offset manifest, not a timestamp.** Each cleaned
   record's Kafka timestamp is set to `source.ts_ms` (the DB commit time), so
   `offsets_for_times(T)` resolves "the database as of T" to an exact offset per
   partition. That offset set is the manifest.
2. **The schema registry enforces the de-id policy.** The clean Avro schema is
   derived from `(raw schema, policy)`. A source column nobody wrote a policy
   rule for halts that one topic at startup instead of leaking downstream.

Everything runs on Kubernetes under an umbrella Helm chart, with the official
Redpanda chart as a dependency, so the same charts move to a shared cluster
later.

## Repo layout

```
Makefile                       inner-loop targets (make help)
charts/pit/                    umbrella Helm chart
  Chart.yaml                   depends on redpanda (charts.redpanda.com) + source-pg
  values.yaml                  cross-environment invariants
  values-local.yaml            laptop sizing (1 broker, TLS off, small)
  charts/
    source-pg/                 Postgres 16 source, wal_level=logical    (M2)
      files/initdb/20-clinic-schema.sql     clinic tables, synthetic PHI/PII
      files/replication/11-replica-identity.sql  REPLICA IDENTITY FULL, by default
    sink-pg/                   PIT sink Postgres               (M5)
    connect/                   Debezium + Avro Connect         (M3)
    deid/                      de-identification transformer   (M4)
    pitctl/                    pit-tail / snapshot / restore   (M5/M7)
loadgen/                       deterministic synthetic load generator   (M2)
  src/loadgen/config.py        seed constant, counts, distributions
  src/loadgen/seed.py          the generator, loader and CLI
  src/loadgen/__main__.py      the churn loop: the timeline a replay replays
hack/
  forward.sh                   backgrounded port-forwards behind a PID file
  verify.sh                    M1 broker/registry/console acceptance checks
images/
  connect/                     Debezium Connect base           (M3 adds Avro converter)
  deid/                        python + uv base                (M4)
  pitctl/                      python + uv base                (M5/M7)
scripts/
  verify-conf-docker.sh        cluster-free check of the rendered chart
  verify-schema.sql            schema and replica-identity assertions
  verify-wal.sql               proves the before image reaches the WAL
```

## Prerequisites

- Docker Desktop with **≥ 8.5 GB** allocated to its VM (Settings → Resources).
  The `pit` node asks Docker for 8 GB (`--memory=8192`); Docker Desktop needs a
  little more than that for its own overhead. If Docker gives less, `make up`
  exits with `Docker Desktop has only NNNN MB memory but you specified 8192MB`.
  Either raise the Docker Desktop memory or run `make up MEMORY=<fits>`.
- `minikube`, `kubectl`, `helm` (v3.10+), and `uv` on PATH.

## Quickstart (runbook)

```bash
make up          # start the pit minikube profile + namespace, deploy the stack
make forward     # port-forward console/registry/postgres in the background
make verify-all  # all acceptance checks against the cluster

# verify the broker and registry answer:
curl -s localhost:8081/subjects      # -> []   (no schemas registered yet)
open  http://localhost:8080          # Redpanda Console

# populate the source database (needs `make forward` running):
make seed        # wipe and repopulate the clinic schema
make seed-verify # load generator acceptance check

# give the population a timeline to be replayed against:
make churn       # five minutes of inserts, updates and deletes
make churn-verify # the ledger replays to the live tables

make nuke        # tear everything down, including PVCs
```

`make up` is idempotent — re-run it after editing values. `make nuke` deletes
the whole minikube profile, so PVCs go too and Postgres init SQL re-runs on the
next `make up`. `make clean` is the narrower version: it drops the release and
its PVCs but leaves the cluster standing.

`make verify-docker` runs the same schema assertions with no cluster at all.

`make forward` runs in the background behind a PID file — `make forward-stop`
tears it down. `make verify-m1` is the M1 slice of `verify-all`: broker replicas,
registry, console, plus two rendered-manifest invariants (no cert-manager
`Certificate` resources, no NodePort Services) that a values regression would
otherwise reintroduce silently.

Run `make help` for every target.

## Access table

After `make forward` (leave it running in its own terminal):

| Component        | In-cluster service      | Local port | Reach it                                        | Status        |
| ---------------- | ----------------------- | ---------- | ----------------------------------------------- | ------------- |
| Redpanda Console | `pit-console`           | 8080       | `open http://localhost:8080`                    | ✅ M1          |
| Schema Registry  | `pit-redpanda` (broker) | 8081       | `curl localhost:8081/subjects` → `[]`           | ✅ M1          |
| Kafka Connect    | `connect`               | 8083       | `curl localhost:8083/connectors` → `[]`         | ⏳ pending M3  |
| Source Postgres  | `pit-source-pg`         | 5432       | `psql -h localhost -p 5432 -U pit -d pit`       | ✅ M2          |
| Sink Postgres    | `sink-pg`               | 5433       | `psql -h localhost -p 5433 -U postgres`         | ⏳ pending M5  |
| Kafka broker     | `pit-redpanda`          | 9093       | in-cluster only (`pit-redpanda:9093`)           | ✅ M1          |
| Admin API        | `pit-redpanda`          | 9644       | `curl localhost:9644/v1/status/ready`           | ✅ M1          |

The Kafka broker's Kafka API is not port-forwarded — clients run inside the
cluster and address `pit-redpanda:9093` (and the registry at
`pit-redpanda:8081`) directly. The `pit-redpanda` name comes from
`redpanda.fullnameOverride` in `values.yaml`, not from the release name.

`rpk` needs no local install; run it in the broker:

```bash
kubectl --context pit -n pit-poc exec -it sts/pit-redpanda -c redpanda -- rpk cluster info
```

## Chart gotchas

Verified against `redpanda` 26.2.2 while building M1. Each of these fails in a
way that does not look like its cause.

- **`helm template` is not a proof that the chart installs.** Cluster config
  reaches the broker through the `pit-configuration` post-install *hook*, which
  renders fine and then fails at apply time if any property is unrecognised. An
  invalid `config.cluster` key gives `PUT /v1/cluster_config -> Bad Request`, the
  hook retries, and the release fails. Helm always waits on hooks, so `--wait`
  changes only how loudly it fails. `make lint` and `make template` cannot catch
  this class of bug; only `make install` can.
- **The redpanda subchart ships a `values.schema.json` with
  `additionalProperties: false` at its root.** A typo under `redpanda:` fails the
  render outright rather than being ignored — good, but it also means the only
  supported escape hatch for raw broker flags is
  `redpanda.statefulset.additionalRedpandaCmdFlags`.
- **Console config is auto-wired; do not hand-write it.** The chart's
  `consoleChartIntegration` template force-sets `console.configmap.create` and
  `console.deployment.create` and merges a derived overlay (brokers, registry
  URL, TLS) over `console.config`. Setting `console.config.kafka.brokers` by hand
  fights that merge. `console.enabled: true` is the whole configuration.
- **The schema registry is built into the broker on 8081** — there is no separate
  Deployment, so it shares the broker Service (`pit-redpanda`, per
  `fullnameOverride`).
- **Pass `-n $(NAMESPACE)` when rendering.** The broker's advertised addresses
  embed the namespace (`pit-0.pit.pit-poc.svc.cluster.local`), so a
  namespace-less `helm template` renders misleading FQDNs. `make template` does
  this for you.
- **`charts.redpanda.com` publishes a chart literally named `connect` — that is
  Redpanda Connect (Benthos), *not* the Kafka Connect M3 needs.** M3's Debezium
  worker is a custom image built from `images/connect/`.
- **Cleaned topics must be created explicitly in M4** with `retention.ms=-1` and
  `cleanup.policy=delete`. Auto-created topics inherit broker defaults, and a
  compacted `clean.*` topic destroys the history point-in-time replay depends on
  while looking perfectly healthy.

## Milestone status

- **M1 — Cluster and chart skeleton:** ✅
- **M2 — Source Postgres and clinic schema:** ✅
- M3–M8: see the [Linear project](https://linear.app/headway/project/point-in-time-de-identified-database-replica-poc-a605b4c0031e/overview).

## source-pg

Postgres 16 as a StatefulSet, with a ConfigMap-supplied `postgresql.conf` passed
via `-c config_file=`, a PVC from `volumeClaimTemplates`, a headless Service for
stable DNS plus a ClusterIP Service for clients, and init SQL mounted at
`/docker-entrypoint-initdb.d`.

The point of the chart is that Debezium can attach to it and get complete change
events, with nobody having configured the database first. Three settings and two
SQL scripts:

```
wal_level = logical                          postgresql.conf
max_replication_slots = 4
max_wal_senders = 4
REPLICA IDENTITY FULL on every table         11-replica-identity.sql
CREATE PUBLICATION ... FOR ALL TABLES        12-publication.sql
```

Debezium decodes changes through a logical replication slot. Without
`wal_level=logical` the WAL carries no row images, slot creation fails, and there
is no CDC — so nothing downstream exists. `wal_level` cannot be changed at
runtime, which is why it lives in `postgresql.conf` rather than `ALTER SYSTEM`.

The other two are covered under [Replication](#replication). Debezium creates its
own slot; the chart does not. A slot created at init retains WAL from the moment
Postgres boots and keeps retaining it until something reads it, which on a laptop
PVC is how you fill a volume.

We author this chart instead of depending on Bitnami's: Bitnami moved its free
images to `bitnamilegacy` in 2025 and the catalog now points at a paid registry.
A StatefulSet plus a ConfigMap is a short file and gives direct control over the
one setting that matters.

Because `config_file` replaces the `postgresql.conf` that `initdb` writes into
`PGDATA`, the rendered file must declare `data_directory`, `hba_file`, and
`ident_file`. They are set from `.Values.pgData` and must stay in step with the
`PGDATA` env var on the container.

### Verifying

`make verify` waits for rollout, prints the relevant `pg_settings`, asserts
`SHOW wal_level` is `logical`, checks that `dbz_publication` exists and is `FOR
ALL TABLES`, and then runs `scripts/verify-wal.sql`.

That last script is the one that matters. It creates a table nobody configured,
opens a real slot, updates a row, and asserts the *pre-update* value shows up in
the decoded stream. `REPLICA IDENTITY FULL` appearing in `pg_class` is not the
same as the old row reaching the WAL, and the WAL is the only place the old row
exists — the source keeps no audit copy of it. Under the primary-key default the stream still
contains an update and a delete and still looks healthy — it just has no before
image in it, and that failure is invisible to anything that only reads settings.

`make verify-docker` runs every one of these assertions against the rendered
ConfigMaps under plain docker, no cluster needed. Both paths share
`verify-schema.sql` and `verify-wal.sql` so they cannot drift.

### Credentials

Dev defaults live in `charts/pit/charts/source-pg/values.yaml`. Debezium connects
as `debezium` (`LOGIN REPLICATION SUPERUSER`), created by the init script. Point
`auth.existingSecret` at a Secret with keys `postgres-password` and
`replication-password` to supply real ones.

Superuser is a PoC posture and the chart says so. The connector does not need it:
the chart creates the publication, and opening a slot needs only `REPLICATION`.
It is granted so that a connector pointed at a publication nobody created can
still create one — `CREATE PUBLICATION ... FOR ALL TABLES` requires superuser in
Postgres 16 — which is what keeps the database free of manual setup in every case
rather than just the expected one. Set `auth.replicationSuperuser: false` on a
shared cluster and the role falls back to `REPLICATION` plus `SELECT`.

### Extending

`extraInitScripts` is a filename-to-content map merged into the initdb ConfigMap.
Use a `30-` or higher prefix; the chart owns `10-` (role), `11-` (replica
identity), `12-` (publication) and `20-` (schema).
Note that initdb scripts run only on first boot of an empty volume — `make clean`
drops the PVC when you need a rebuild.

## Clinic schema

Five tables, all synthetic, in `public`. All of them replicated — see
[Replication](#replication):

| table | shape that makes it interesting |
| --- | --- |
| `patients` | name split across columns, `date_of_birth date`, `ssn`, email, phone, address split with `postal_code` separate |
| `providers` | `npi`, a public identifier that is still a strong re-identification handle |
| `appointments` | FKs to patient and provider, four timestamps, an enum status, plus free text nested in `intake_answers jsonb` |
| `claims` | `numeric(12,2)` money and `diagnosis_codes text[]` |
| `notes` | `body text` — unstructured PHI mixed into clinical prose, the hardest case |

The shapes are deliberately awkward. A de-identification policy that only knows
how to hash scalar text columns will pass `intake_answers` and `diagnosis_codes`
straight through, and masking `date_of_birth` to NULL destroys every age cohort
that made the data worth keeping. Set against that, `notes.body` is the case that
decides whether the policy works at all.

Foreign keys are kept here and are not needed on the sink: this stands in for a
realistic operational primary, and the `ON DELETE` actions are part of what makes
replay hard. They mix on purpose — `CASCADE` from `patients` (one statement fans
out into deletes across three tables), `RESTRICT` on `providers`, and `SET NULL`
on `notes.appointment_id`, so deleting an appointment shows up on `notes` as an
*update*. A pipeline that only watches the deleted table gets that one wrong.

`decimal.handling.mode` matters for `claims`: Debezium's default encodes
`numeric` as base64 `VariableScaleDecimal`, which compares equal to nothing.

## Replication

Every table in the source database is replicated, in full, and a table created
later is replicated too without anyone opting it in. Two mechanisms, one per half
of the problem:

| what | how | where |
| --- | --- | --- |
| a new table joins the stream | `CREATE PUBLICATION dbz_publication FOR ALL TABLES` | `12-publication.sql` |
| a new table carries before images | an event trigger on `ddl_command_end` | `11-replica-identity.sql` |

Postgres offers no setting for either. `REPLICA IDENTITY` is a per-table property
that defaults to the primary key, and a publication either names its tables or is
`FOR ALL TABLES`. So the default is built rather than configured.

Why full before images. Under the primary-key default an `UPDATE` or `DELETE`
reaches the WAL carrying only the key columns. Debezium then emits a change event
whose `before` is empty, and nothing downstream can say what the row used to hold.
That matters most for the de-id policy in M4, which has to decide what a column
was, not only what it became.

The cost is real and worth stating. `FULL` writes the whole old row to the WAL on
every update and delete, and it makes the downstream apply compare full rows
instead of looking up a key. For a synthetic clinic database that is cheap. On a
large operational primary it is not, and the answer there is a unique index per
table rather than `FULL` everywhere.

### Adding a table

Nothing. Create it:

```sql
CREATE TABLE referrals (referral_id bigint PRIMARY KEY, patient_id bigint, reason text);
```

It comes out `REPLICA IDENTITY FULL` and already in the publication. `make psql`,
then:

```
table pit_replicated_tables;
select * from pg_publication_tables where pubname = 'dbz_publication';
```

`pit_replicated_tables` reads `pg_class`, so it reports what is true rather than
what this README claims. If a table ever does slip through — restored from a dump,
say, or created while the event trigger was dropped — `pit_replicate_all()` repairs
every table in one idempotent call and returns how many it changed.

### What M3 needs from this

The connector needs no setup SQL and no table list:

```
publication.name=dbz_publication
publication.autocreate.mode=disabled
```

It creates its own replication slot, which is why the chart does not. Confirm the
whole path is open before writing any connector config:

```
make verify   # asserts the publication, then decodes a real stream
```

### Verifying the schema

```
make verify-schema   # in-cluster
make verify-docker   # no cluster; runs the same SQL
```

Both run `scripts/verify-schema.sql`, which asserts the tables exist, that
`pg_class.relreplident` is `f` for every table in the database rather than for a
list of them, that the source keeps its foreign keys, that no row-level trigger
or history table survives, and that a table created on the spot comes out
replicated. It works on a loaded database as well as an empty one: the fixtures
are created inside a transaction that is rolled back at the end.

The trigger and history-table assertions look redundant against a fresh install,
and they are the ones most likely to fire. initdb scripts run only on an empty
volume, so a database created before this change keeps its ledger through a
`helm upgrade` and would otherwise pass every other check here. The fix is
`make clean`, and the error message says so.

## Load generator

`loadgen/` fills the schema with synthetic patients, providers, appointments,
claims and notes. Everything it produces is fake — it takes an integer and
returns rows, with no network, no filesystem and no clock involved.

```
make loadgen-deps   # uv sync
make forward        # backgrounded; make forward-stop tears it down
make seed           # wipe and repopulate
make seed-verify    # acceptance check
```

Two runs with the same seed produce identical row contents, so two people
debugging the same failure are looking at the same rows and M8's determinism
test has a stable baseline. `seed-verify` proves it by seeding twice and
comparing a digest of what landed, which also catches anything the load itself
mangles on the way into Postgres — a `numeric` rounded, a timezone lost.

Shape matters more than volume: appointments are apportioned across patients by
largest remainder over lognormal weights, so a few patients carry dozens of
visits, most have one or two, and some have none. The awkward cases the
de-identification policy has to survive are planted rather than sampled — null
emails, a patient past the Safe Harbor age cap, a zip starting with a zero,
decimal money at both ends of `numeric(12,2)`, unicode names across six scripts,
SSNs in five formats, and PHI woven into `notes.body` — and the generator
refuses to return a dataset that is missing any of them.

The whole seed lands in one transaction, so the ledger records it at a single
`tx_at`: a point-in-time query before that instant sees an empty database and
one after it sees the whole population, with no half-loaded state in between for
a replay to land on. `loadgen/README.md` has the details.

## Churn

A static seed gives one state. Point-in-time replay needs history to replay, so
`python -m loadgen` keeps mutating the seeded database — inserts, updates and
deletes, each in its own explicit transaction, at a configurable rate.

```
make churn          # five minutes at two transactions per second
make churn-verify   # check the ledger without changing anything
make churn-check    # acceptance check
```

Three properties are the point.

**Every mutation is in the ledger, and the ledger is written by the trigger.**
Churn does not append to `<table>_history`; `pit_audit` does, inside the same
transaction as the change, which is what lets it be M8's oracle. What churn does
is read the ledger back per transaction and reconcile it against what it meant to
do, which is where cascade fan-out becomes visible: deleting one appointment
deletes its claims and sets `notes.appointment_id` to NULL, so one statement
becomes history in three tables, and the notes arrive as *updates*.

**Some transactions touch several tables at one `tx_at`.** A transaction that
inserts a patient, their first appointment and their intake note has no instant
at which only part of it is true, so a replay that produces one has stopped inside
a transaction rather than between two. That is what `--snap-to-txn` is for, and
without such transactions in the data the flag has nothing to be right or wrong
about.

**The volume is bounded.** Cleaned topics run at infinite retention on a laptop
PVC, so there is no unbounded mode: wall-clock duration, transaction count and
total ledger rows are all capped, and insert/delete weights are biased to hold
each table inside a band around its configured size, so a long run churns instead
of growing.

Because there is one writer and each transaction starts only after the previous
one committed, `tx_at` strictly increases and every transaction is a distinct
point a `T` can land between. `--verify` checks that, and checks the thing the
ledger exists for: replay it to its newest `tx_at` — newest entry per key, absent
if it was a delete — and the result has to be the live table, row for row. That
is the query M6/M8 will run at an arbitrary `T`; `T = now` is the one case with an
independent answer to compare against.
