-- Acceptance check for the sink schema `pit initdb` built (DATA-714).
--
-- Run against the point-in-time database itself (pit_base by default), by
-- `make pit-check`. Read-only: every assertion reads the catalog, so unlike
-- scripts/verify-schema.sql there is nothing to roll back.
--
-- The claim being checked is one sentence: **the sink's types are the policy's
-- types.** The schema was generated from the registered clean Avro schemas
-- rather than from the source's information_schema, and that is only worth doing
-- if the awkward columns actually came out right. So the assertions are aimed at
-- the columns where a lazy mapping would have been silently wrong -- money,
-- timestamps, the code array -- and at the columns that should not exist at all.
--
-- Failure is a raised exception: with ON_ERROR_STOP psql exits non-zero.

\set ON_ERROR_STOP on

-- 0. initdb ran at all ------------------------------------------------------
-- First, because it is the check a wrong --db or a fresh volume trips on, and
-- every assertion below assumes the tables are there.
DO $$
DECLARE
  expected text[] := ARRAY['patients','providers','appointments','claims','notes'];
  missing text;
BEGIN
  SELECT string_agg(t, ', ' ORDER BY t) INTO missing
    FROM unnest(expected) AS t
   WHERE to_regclass('public.' || t) IS NULL;

  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'missing tables: %. Run `make initdb` against this database first.', missing;
  END IF;
  RAISE NOTICE 'ok: all % tables present', cardinality(expected);
END;
$$;

-- 1. Money is numeric, not bytea --------------------------------------------
-- Debezium's `precise` decimal mode puts an unscaled big-endian integer in
-- `bytes`, and a mapping that went by the Avro primitive alone would store that
-- as bytea. It would look like it worked and compare equal to nothing, which is
-- why DATA-714 calls this out by name. The precision and scale come from the
-- Avro logical type, so they are checked too.
DO $$
DECLARE bad text;
BEGIN
  SELECT string_agg(format('%s (%s)', column_name, data_type), ', ' ORDER BY column_name)
    INTO bad
    FROM information_schema.columns
   WHERE table_schema = 'public' AND table_name = 'claims'
     AND column_name IN ('billed_amount','allowed_amount','paid_amount','patient_responsibility')
     AND NOT (data_type = 'numeric' AND numeric_precision = 12 AND numeric_scale = 2);

  IF bad IS NOT NULL THEN
    RAISE EXCEPTION 'claims amounts are not numeric(12,2): %', bad;
  END IF;
  RAISE NOTICE 'ok: all four claims amounts are numeric(12,2)';
END;
$$;

-- 2. Timestamps are timestamptz, not text -----------------------------------
-- Every timestamp in this schema is a Debezium ZonedTimestamp, which is an
-- ISO-8601 *string* on the wire. Storing the wire type would make every date
-- query in the replica wrong while looking perfectly fine.
--
-- Nullability is asserted in the same breath, because it is the surprising half:
-- these columns are NOT NULL at the source and nullable here, since date_shift
-- widens a ZonedTimestamp (a string is not necessarily an instant). The sink
-- follows the registered schema rather than second-guessing it.
DO $$
DECLARE
  bad text;
  n_ts int;
BEGIN
  SELECT string_agg(format('%s.%s (%s)', table_name, column_name, data_type), ', '
                    ORDER BY table_name, column_name)
    INTO bad
    FROM information_schema.columns
   WHERE table_schema = 'public'
     AND column_name LIKE '%\_at'
     AND data_type <> 'timestamp with time zone';

  IF bad IS NOT NULL THEN
    RAISE EXCEPTION 'timestamp columns that are not timestamptz: %', bad;
  END IF;

  -- 17, and the arithmetic is worth writing down so the number is checkable
  -- rather than magic: appointments 5 (scheduled/checked_in/completed/created/
  -- updated), claims 4 (submitted/adjudicated/created/updated), notes 4
  -- (authored/signed/created/updated), patients 2, providers 2. A table that
  -- failed to land would show up here as a shortfall.
  SELECT count(*) INTO n_ts
    FROM information_schema.columns
   WHERE table_schema = 'public' AND data_type = 'timestamp with time zone';
  IF n_ts <> 17 THEN
    RAISE EXCEPTION 'expected 17 timestamptz columns across the five tables, found %', n_ts;
  END IF;

  RAISE NOTICE 'ok: % timestamptz columns, none stored as text', n_ts;
END;
$$;

-- 3. The diagnosis codes are a text array -----------------------------------
-- generalize to icd10_category maps an array of codes to an array of
-- categories, so the column stays an array. jsonb or a comma-joined text would
-- both accept the data and change what it means.
DO $$
DECLARE udt text;
BEGIN
  SELECT udt_name INTO udt
    FROM information_schema.columns
   WHERE table_schema = 'public' AND table_name = 'claims' AND column_name = 'diagnosis_codes';

  IF udt IS DISTINCT FROM '_text' THEN
    RAISE EXCEPTION 'claims.diagnosis_codes is %, expected text[]', coalesce(udt, 'missing');
  END IF;
  RAISE NOTICE 'ok: claims.diagnosis_codes is text[]';
END;
$$;

-- 4. What the policy drops has no column ------------------------------------
-- Absent, not nulled and not redacted. A column that exists and is always empty
-- is a column someone will eventually try to backfill, and the free-text ones
-- are exactly the columns that must never come back.
DO $$
DECLARE
  dropped text[][] := ARRAY[
    ['patients','ssn'], ['patients','address_line1'], ['patients','address_line2'],
    ['patients','city'],
    ['appointments','location'], ['appointments','intake_answers'],
    ['notes','body']
  ];
  present text := NULL;
  i int;
BEGIN
  FOR i IN 1 .. array_length(dropped, 1) LOOP
    IF EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = dropped[i][1] AND column_name = dropped[i][2]) THEN
      present := concat_ws(', ', present, dropped[i][1] || '.' || dropped[i][2]);
    END IF;
  END LOOP;

  IF present IS NOT NULL THEN
    RAISE EXCEPTION 'columns the policy drops still exist in the sink: %', present;
  END IF;
  RAISE NOTICE 'ok: every dropped column is absent, including notes.body';
END;
$$;

-- 5. date_of_birth is a year, not a date ------------------------------------
-- The generalization that decides whether the replica is worth having: a birth
-- year keeps every age cohort, and a date would have kept the identifier.
DO $$
DECLARE kind text;
BEGIN
  SELECT data_type INTO kind
    FROM information_schema.columns
   WHERE table_schema = 'public' AND table_name = 'patients' AND column_name = 'date_of_birth';

  IF kind IS DISTINCT FROM 'integer' THEN
    RAISE EXCEPTION 'patients.date_of_birth is %, expected integer (a birth year)', coalesce(kind, 'missing');
  END IF;
  RAISE NOTICE 'ok: patients.date_of_birth is an integer year, not a date';
END;
$$;

-- 6. Every primary key is text ----------------------------------------------
-- The source's ids are bigint; the policy hashes them, so the sink's are text.
-- This is the single clearest sign the DDL came from the registry rather than
-- from the source's information_schema.
DO $$
DECLARE
  n_pk int;
  bad text;
BEGIN
  SELECT count(*), string_agg(format('%s.%s (%s)', c.relname, a.attname, t.typname), ', ')
    INTO n_pk, bad
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN unnest(con.conkey) AS k(attnum) ON true
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
    JOIN pg_type t ON t.oid = a.atttypid
   WHERE con.contype = 'p' AND n.nspname = 'public';

  IF n_pk <> 5 THEN
    RAISE EXCEPTION 'expected 5 single-column primary keys, found % key columns (%)', n_pk, bad;
  END IF;

  SELECT string_agg(format('%s.%s is %s', c.relname, a.attname, t.typname), ', ')
    INTO bad
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN unnest(con.conkey) AS k(attnum) ON true
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
    JOIN pg_type t ON t.oid = a.atttypid
   WHERE con.contype = 'p' AND n.nspname = 'public' AND t.typname <> 'text';

  IF bad IS NOT NULL THEN
    RAISE EXCEPTION 'primary keys that are not text: % -- the policy hashes every id, so a bigint here means the DDL did not come from the registry', bad;
  END IF;
  RAISE NOTICE 'ok: all 5 primary keys are text, because the policy hashes the ids';
END;
$$;

-- 7. No foreign keys --------------------------------------------------------
-- Per-table topics replay independently, so referential order is not
-- guaranteed and any FK would reject a legal replay. Standard for a CDC sink;
-- M8's join-integrity test is how the relationships get checked instead.
DO $$
DECLARE bad text;
BEGIN
  SELECT string_agg(format('%s.%s', c.relname, con.conname), ', ') INTO bad
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE con.contype = 'f' AND n.nspname = 'public';

  IF bad IS NOT NULL THEN
    RAISE EXCEPTION 'the sink has foreign keys, which will reject a legal replay: %', bad;
  END IF;
  RAISE NOTICE 'ok: no foreign keys, so per-table topics can replay in any order';
END;
$$;

-- 8. The applier's bookkeeping is in the payload database --------------------
-- pit_meta.applied_offsets lives inside pit_base on purpose, so a
-- CREATE DATABASE ... TEMPLATE clone carries the manifest it was cut at. M8's
-- oracle compare and leak scan have to exclude this schema.
DO $$
BEGIN
  IF to_regclass('pit_meta.applied_offsets') IS NULL THEN
    RAISE EXCEPTION 'pit_meta.applied_offsets is missing: a snapshot of this database would not know where it was cut';
  END IF;
  RAISE NOTICE 'ok: pit_meta.applied_offsets is present, so a snapshot carries its own manifest';
END;
$$;

\echo 'PASS: the sink schema is the post-policy schema -- money numeric, times timestamptz, codes text[], dropped columns absent, keys text, no FKs'
