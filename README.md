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
      files/initdb/21-history-triggers.sql  the mutation ledger
    sink-pg/                   PIT sink Postgres               (M5)
    connect/                   Debezium + Avro Connect         (M3)
    deid/                      de-identification transformer   (M4)
    pitctl/                    pit-tail / snapshot / restore   (M5/M7)
loadgen/                       deterministic synthetic load generator   (M2)
  src/loadgen/config.py        seed constant, counts, distributions
  src/loadgen/seed.py          the generator, loader and CLI
  src/loadgen/__main__.py      the churn loop: the timeline a replay replays
deid/                          de-identification transformer            (M4)
  policy/clinic.yml            the policy: the auditable artifact
  src/deid/policy.py           the typed policy model, validated at the edge
hack/
  forward.sh                   backgrounded port-forwards behind a PID file
  verify.sh                    M1 broker/registry/console acceptance checks
images/
  connect/                     Debezium Connect base           (M3 adds Avro converter)
  deid/                        python + uv base                (M4)
  pitctl/                      python + uv base                (M5/M7)
scripts/
  verify-conf-docker.sh        cluster-free check of the rendered chart
  verify-schema.sql            schema + ledger assertions, run by both paths
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

## De-identification policy

`deid/policy/clinic.yml` is the artifact. When someone asks what we did to
`ssn`, the answer is a line in a file under version control, not a branch buried
in a transformer.

```
make deid-deps     # uv sync
make policy-check  # validate the policy and print what it actually says
make deid-test     # unit tests; no cluster, no database
```

Every column of every captured table appears exactly once, including the ones
that survive untouched, because a column nobody wrote down is a column nobody
reviewed. `on_uncovered_column: halt_topic` is what makes that true going
forward: a column added to the source with no rule stops that one topic at
startup instead of leaking. The only alternative setting is `drop_column` —
there is deliberately no `passthrough`.

`deid/src/deid/policy.py` parses the file into frozen dataclasses once, at
startup, and no raw dict escapes that module. Everything is checked there rather
than at record time, so a mistake surfaces in a file a human is reading instead
of as a `KeyError` on record forty thousand of a replay with a half-written
clean stream behind it. Each failure names the table, the column and the
problem:

```
clinic.yml: public.patients.patient_id: op 'hmac' requires argument 'domain'
```

Six ops, and the arguments each one cannot work without:

| op | what it does | why it needs what it needs |
| --- | --- | --- |
| `hmac` | keyed hash, requires `domain` | the domain is what keeps joins working: two columns hashed under `patient` land on the same token, one under another domain cannot be joined to them. No default is right more often than wrong. |
| `fake` | plausible synthetic value, requires `kind` | a name column full of nulls breaks every UI downstream and is no safer than a fake name. `kind` is a closed set, so a typo fails at startup rather than mid-topic. |
| `generalize` | coarsen, requires `to` | the op that decides whether the replica is worth having. `date_of_birth` → `birth_year` with `cap_age: 89` keeps age cohorts and collapses the Safe Harbor tail; masking to NULL is trivially safe and useless. |
| `date_shift` | move a timestamp, requires `anchor` | one constant offset per anchor entity. A global shift is a caesar cipher on the calendar; a per-record shift destroys every interval, which is usually the reason the data was wanted. |
| `drop` | remove from the clean schema | not null — remove. A nulled column still says the source had one. |
| `passthrough` | copy unchanged | written down explicitly so it was reviewed. |

Three checks are load-bearing rather than tidy:

- **Nothing may address the `source` block.** Replay works because each cleaned
  record's Kafka timestamp is `source.ts_ms`, so `offsets_for_times(T)` is an
  exact answer. A policy able to rewrite or drop it could destroy the timeline
  while every record still looked de-identified, so the envelope is not
  addressable at all — `source`, `op`, `ts_ms` and `transaction` all raise.
- **A `date_shift` anchor must be a column the same table covers**, must not be
  the column itself, and must not be another shifted column — anchoring on a
  moving value gives a per-record offset and silently destroys intervals.
- **A duplicate key is an error.** YAML lets a second `ssn:` win silently, which
  is exactly the audit failure the file exists to prevent.

The tests check the shipped policy against the source DDL in both directions: a
missing rule halts a topic on the day it deploys, and a rule for a column that
does not exist is worse — it reads like protection in review and does nothing.

Two things the current policy does not do, recorded here rather than discovered
later. `notes.body` and `appointments.intake_answers` are **dropped**, not
scrubbed: free text has no column-level answer, and passing it through would
make every other rule in the file decorative. And date shift protects against
re-identification from the replica's contents, not against someone who can also
read the clean topics — the Kafka record timestamp is the unshifted commit time,
by design.

## Milestone status

- **M1 — Cluster and chart skeleton:** ✅
- **M2 — Source Postgres, clinic schema, mutation ledger:** ✅
- M3–M8: see the [Linear project](https://linear.app/headway/project/point-in-time-de-identified-database-replica-poc-a605b4c0031e/overview).

## source-pg

Postgres 16 as a StatefulSet, with a ConfigMap-supplied `postgresql.conf` passed
via `-c config_file=`, a PVC from `volumeClaimTemplates`, a headless Service for
stable DNS plus a ClusterIP Service for clients, and init SQL mounted at
`/docker-entrypoint-initdb.d`.

The point of the chart is these three settings:

```
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Debezium decodes changes through a logical replication slot. Without
`wal_level=logical` the WAL carries no row images, slot creation fails, and there
is no CDC — so nothing downstream exists. `wal_level` cannot be changed at
runtime, which is why it lives in `postgresql.conf` rather than `ALTER SYSTEM`.

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
`SHOW wal_level` is `logical`, and then creates and drops a real `pgoutput` slot
— the setting reading correctly is not the same as decoding working.

`make verify-docker` runs the same assertions against the rendered ConfigMaps
under plain docker, no cluster needed.

### Credentials

Dev defaults live in `charts/pit/charts/source-pg/values.yaml`. Debezium connects
as `debezium` (`LOGIN REPLICATION`), created by the init script. Point
`auth.existingSecret` at a Secret with keys `postgres-password` and
`replication-password` to supply real ones.

### Extending

`extraInitScripts` is a filename-to-content map merged into the initdb ConfigMap.
Use a `30-` or higher prefix; the chart owns `10-` (roles) and `20-` (schema).
Note that initdb scripts run only on first boot of an empty volume — `make clean`
drops the PVC when you need a rebuild.

## Clinic schema

Five captured tables, all synthetic, in `public`:

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

### The mutation ledger

Every captured table has a `<table>_history` twin, written inside the same
transaction as the change by an `AFTER INSERT OR UPDATE OR DELETE` trigger:

```
op          I, U or D
txid        the transaction id
tx_at       transaction_timestamp() -- the point-in-time key
stmt_at     statement_timestamp()   -- which statement inside the transaction
recorded_at clock_timestamp()       -- real elapsed time, for debugging
pk          the primary key, as jsonb, so composite keys need no special case
before_row  the whole row before, for U and D
after_row   the whole row after, for I and U
```

This is the correctness oracle M8 compares against, which is why it is recorded
here rather than reconstructed from CDC events later — reconstructing it from the
thing under test would not be an oracle. The expected state of a table at time
`T` is one query:

```sql
SELECT after_row
FROM (
  SELECT DISTINCT ON (pk) pk, op, after_row
  FROM patients_history
  WHERE tx_at <= :t
  ORDER BY pk, tx_at DESC, history_id DESC
) s
WHERE op <> 'D';
```

`tx_at` rather than a clock reading because it is identical for every row in a
transaction, so a multi-statement transaction lands at a single point on the
timeline and can never be half-visible to a query at `T`. `stmt_at` is how you
order two changes to the same row inside one transaction.

`REPLICA IDENTITY FULL` is set on all five: without it an update or delete
reaches the WAL carrying only the key columns, and there is no before image for
the oracle to compare against.

One known gap: `TRUNCATE` does not fire row-level triggers, so it is invisible to
the ledger. Don't truncate a captured table — delete instead.

### Adding a captured table

Create the table with a primary key, then one call:

```sql
SELECT pit_install_capture('public.referrals');
```

That sets `REPLICA IDENTITY FULL`, builds `referrals_history`, installs the audit
trigger with the primary-key columns as arguments, and adds `updated_at`
maintenance if the column exists. It is idempotent, so it can be re-run against a
live database. The history table holds two jsonb documents rather than a mirror of
the source columns, so a later `ALTER TABLE` cannot silently stop being recorded.

`pit_captured_tables` reports what is actually captured, derived from the
installed triggers rather than from a hardcoded list:

```
make psql -- then: table pit_captured_tables;
```

### Verifying the schema

```
make verify-schema   # in-cluster
make verify-docker   # no cluster; runs the same SQL
```

Both run `scripts/verify-schema.sql`, which asserts the tables exist, that
`pg_class.relreplident` is `f` for each captured table, that the source keeps its
foreign keys, and that inserts, updates, cascade deletes and `SET NULL` all land
in the ledger with the right before and after images. It works on a loaded
database as well as an empty one: the fixtures are created inside a transaction
that is rolled back at the end.

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
