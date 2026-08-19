# headway-hack-proj

A CDC pipeline for exercising point-in-time correctness: a Postgres source with
logical decoding enabled, a clinic schema carrying deliberately varied PHI/PII
shapes, a load generator that produces real history to replay, and a mutation
ledger that acts as the correctness oracle downstream.

## Layout

```
charts/pit/                     umbrella chart
charts/pit/charts/source-pg/    Postgres 16 source, wal_level=logical
scripts/verify-conf-docker.sh   cluster-free check of the rendered config
```

## Requirements

`helm`, `kubectl`, `docker`, and a local cluster (`minikube` by default).

```
brew install helm kubectl minikube
```

## Quickstart

```
make cluster     # start minikube
make install     # helm upgrade --install, waits for readiness
make verify      # acceptance check
```

`make help` lists everything.

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
Use a `20-` or higher prefix; the chart owns `10-`. Note that initdb scripts run
only on first boot of an empty volume — `make clean` drops the PVC when you need
a rebuild.
