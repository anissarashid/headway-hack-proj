-- Acceptance check for the clinic schema and full replication.
--
-- Run by both `make verify-schema` (in-cluster) and scripts/verify-conf-docker.sh
-- (no cluster), so the two paths cannot drift. Everything happens inside a
-- transaction that is rolled back, which means it is safe against a database
-- that already has load in it -- and it works against an empty one, because the
-- fixtures are created here.
--
-- Failure is a raised exception: with ON_ERROR_STOP psql exits non-zero and the
-- transaction is discarded.
--
-- Scope note: the publication and the WAL itself are checked by `make verify`
-- and scripts/verify-conf-docker.sh, not here. Logical decoding returns only
-- committed transactions, so a rollback-everything script cannot see its own
-- changes in the stream.

\set ON_ERROR_STOP on

BEGIN;

-- 0. The replication scripts ran at all -------------------------------------
-- First, because it is the check a stale volume trips on, and every assertion
-- below reads the view. initdb runs only on an empty PVC, so a database created
-- before full replication landed survives a `helm upgrade` untouched.
DO $$
BEGIN
  IF to_regclass('pit_replicated_tables') IS NULL THEN
    RAISE EXCEPTION 'pit_replicated_tables does not exist: this database predates full replication. The init scripts only run on an empty volume -- run `make clean` (drops the PVC) and reinstall.';
  END IF;
END;
$$;

-- 1. The tables exist -------------------------------------------------------
DO $$
DECLARE
  expected text[] := ARRAY['patients','providers','appointments','claims','notes'];
  missing text;
BEGIN
  SELECT string_agg(t, ', ' ORDER BY t) INTO missing
    FROM unnest(expected) AS t
   WHERE to_regclass('public.' || t) IS NULL;

  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'missing tables: %', missing;
  END IF;
  RAISE NOTICE 'ok: all % tables present', cardinality(expected);
END;
$$;

-- 2. Every table has complete before images ---------------------------------
-- pg_class.relreplident = 'f' is the whole point: without it an update or
-- delete reaches the WAL carrying only the key columns, and the WAL is now the
-- only copy. No table is exempt -- that is what "full replication by default"
-- means, so the assertion is over the whole database rather than a list.
DO $$
DECLARE
  n_tables int;
  bad text;
BEGIN
  SELECT count(*) INTO n_tables FROM pit_replicated_tables;
  IF n_tables <> 5 THEN
    RAISE EXCEPTION 'expected 5 replicated tables, found %', n_tables;
  END IF;

  SELECT string_agg(format('%s (relreplident=%s)', table_name, relreplident), ', ' ORDER BY table_name)
    INTO bad
    FROM pit_replicated_tables
   WHERE NOT replica_identity_full;
  IF bad IS NOT NULL THEN
    RAISE EXCEPTION 'not REPLICA IDENTITY FULL: %', bad;
  END IF;

  RAISE NOTICE 'ok: 5 replicated tables, all relreplident=f';
END;
$$;

-- 3. The source keeps its foreign keys --------------------------------------
-- Not needed on the sink, deliberately present here: this stands in for a real
-- operational primary, and the ON DELETE actions are part of what makes replay
-- hard.
DO $$
DECLARE n_fk int;
BEGIN
  SELECT count(*) INTO n_fk
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
   WHERE c.contype = 'f'
     AND t.relname IN ('appointments','claims','notes');

  IF n_fk < 7 THEN
    RAISE EXCEPTION 'expected at least 7 foreign keys on appointments/claims/notes, found %', n_fk;
  END IF;
  RAISE NOTICE 'ok: % foreign keys on the child tables', n_fk;
END;
$$;

-- 4. Nothing writes shadow history ------------------------------------------
-- The mutation ledger is gone. Asserted rather than assumed, because a stale
-- PVC survives `helm upgrade`: initdb only runs on an empty volume, so a
-- database created before this change still carries the triggers and would
-- otherwise pass every other check here.
DO $$
DECLARE bad text;
BEGIN
  SELECT string_agg(c.relname, ', ' ORDER BY c.relname) INTO bad
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
   WHERE NOT t.tgisinternal;
  IF bad IS NOT NULL THEN
    RAISE EXCEPTION 'row-level triggers still installed on: % -- the volume predates this schema, run `make clean`', bad;
  END IF;

  SELECT string_agg(c.relname, ', ' ORDER BY c.relname) INTO bad
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'public' AND c.relname LIKE '%\_history';
  IF bad IS NOT NULL THEN
    RAISE EXCEPTION 'history tables still present: %', bad;
  END IF;

  IF to_regproc('pit_install_capture') IS NOT NULL THEN
    RAISE EXCEPTION 'pit_install_capture() still exists';
  END IF;

  RAISE NOTICE 'ok: no row-level triggers, no history tables, no pit_install_capture';
END;
$$;

-- 5. A table created later is replicated too --------------------------------
-- This is the claim the whole design rests on: full replication is the default,
-- not something a new table has to opt into. The event trigger is what makes it
-- true, so exercise it rather than trusting it. DDL is transactional in
-- Postgres, so the fixture table rolls back with everything else.
DO $$
DECLARE ident char;
BEGIN
  CREATE TABLE pit_verify_new_table (id bigint PRIMARY KEY, payload text);

  SELECT relreplident INTO ident
    FROM pg_class WHERE oid = 'public.pit_verify_new_table'::regclass;

  IF ident <> 'f' THEN
    RAISE EXCEPTION 'a newly created table came out relreplident=%, expected f -- the event trigger did not fire', ident;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pit_replicated_tables
                  WHERE table_name = 'pit_verify_new_table' AND replica_identity_full) THEN
    RAISE EXCEPTION 'pit_replicated_tables does not report the new table as replicated';
  END IF;

  RAISE NOTICE 'ok: a table created after init is REPLICA IDENTITY FULL without being asked';
END;
$$;

-- 6. The load generator can still empty the tables in FK order --------------
-- reset() deletes children before parents. If a later schema change adds a FK
-- that inverts that order, this is where it surfaces -- previously the ledger
-- assertions happened to cover it.
DO $$
DECLARE
  pid  bigint;
  prid bigint;
  aid  bigint;
BEGIN
  INSERT INTO providers (npi, full_name, specialty, email)
  VALUES ('0000000001', 'Verify Provider', 'family medicine', 'provider@example.invalid')
  RETURNING provider_id INTO prid;

  INSERT INTO patients (mrn, first_name, last_name, date_of_birth)
  VALUES ('MRN-VERIFY-0001', 'Cascade', 'Fixture', '1988-12-01')
  RETURNING patient_id INTO pid;

  INSERT INTO appointments (patient_id, provider_id, scheduled_at, status, intake_answers)
  VALUES (pid, prid, now(), 'scheduled',
          '{"chief_complaint": "left knee pain since a fall at work"}')
  RETURNING appointment_id INTO aid;

  INSERT INTO claims (patient_id, appointment_id, billed_amount, diagnosis_codes)
  VALUES (pid, aid, 412.50, ARRAY['M25.562','W19.XXXA']);

  INSERT INTO notes (patient_id, provider_id, appointment_id, body)
  VALUES (pid, prid, aid, 'Patient reports knee pain. Reached at (555) 010-0100.');

  DELETE FROM notes        WHERE patient_id = pid;
  DELETE FROM claims       WHERE patient_id = pid;
  DELETE FROM appointments WHERE patient_id = pid;
  DELETE FROM patients     WHERE patient_id = pid;
  DELETE FROM providers    WHERE provider_id = prid;

  RAISE NOTICE 'ok: a full row of fixtures inserts and deletes in FK order';
END;
$$;

-- Leave nothing behind: the fixtures above exist only to be checked.
ROLLBACK;

\echo 'PASS: schema, foreign keys and full replica identity all verified'
