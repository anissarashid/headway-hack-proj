# DATA-703 — Does Redpanda's registry accept Debezium Avro schemas?

**Verdict: YES, with one cosmetic caveat. The Avro + Confluent-API path is safe for M4–M8.**

Run against the live M1 registry (`pit-redpanda:8081`) on Redpanda v26.2:

```
uv run --with fastavro --with requests \
  python spikes/data-703-debezium-avro-registry/spike.py --registry http://localhost:18081
```

`spike.py` registers a faithful Debezium Postgres **envelope** value schema (plus
the key schema), fetches it back, and round-trips a record through `fastavro`.
Result: **10/10 checks pass**, and the spike cleans up its `_spike.*` subjects so
the registry is left empty.

## What was confirmed

| Concern (from the ticket) | Result |
| --- | --- |
| Nested `before`/`after` records with namespaced names survive registration | ✅ registered, both subjects listed |
| `["null","raw.public.patients.Value"]` union where `after` references the record `before` defines | ✅ survives (see caveat) |
| Logical types from `time.precision.mode=connect` decode as datetimes/dates | ✅ `date`→`datetime.date`, `timestamp-millis`→`datetime` |
| `decimal.handling.mode=precise` decodes to `Decimal` | ✅ `Decimal('1234.56')` |
| `io.debezium.connector.postgresql.Source` block round-trips with `ts_ms` | ✅ `ts_ms` intact — the value PIT depends on |

## The one caveat

The registry **canonicalizes the named-type reference to its relative form**: the
`after` field comes back as `["null", "Value"]` rather than the fully-qualified
`["null", "raw.public.patients.Value"]` we sent. This is semantically identical —
inside namespace `raw.public.patients`, `Value` *is* `raw.public.patients.Value`.
Proof it resolves: `fastavro.parse_schema` accepts the fetched schema and a record
with a populated `after` round-trips cleanly.

**Implication for M4:** compare derived clean schemas by *parsed/normalized* form,
not by exact JSON string. Do not assert the fully-qualified spelling of a
reference survives a registry round-trip verbatim. No follow-up ticket needed —
this is a normalization detail, not a defect.

## Bottom line

No alternative serialization path is required. M4's transformer can derive clean
Avro schemas, register them against Redpanda's registry, and rely on `fastavro`
for logical-type fidelity.
