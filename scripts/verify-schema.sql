-- Acceptance check for the clinic schema and the history triggers.
--
-- Run by both `make verify-schema` (in-cluster) and scripts/verify-conf-docker.sh
-- (no cluster), so the two paths cannot drift. Everything happens inside a
-- transaction that is rolled back, which means it is safe against a database
-- that already has load in it -- and it works against an empty one, because the
-- fixtures are created here.
--
-- Failure is a raised exception: with ON_ERROR_STOP psql exits non-zero and the
-- transaction is discarded.

\set ON_ERROR_STOP on

BEGIN;

-- 1. The tables exist -------------------------------------------------------
DO $$
DECLARE
  expected text[] := ARRAY[
    'patients','providers','appointments','claims','notes',
    'patients_history','providers_history','appointments_history',
    'claims_history','notes_history'
  ];
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

-- 2. Every captured table has complete before images ------------------------
-- pg_class.relreplident = 'f' is the whole point: without it an update or
-- delete reaches the WAL carrying only the key columns.
DO $$
DECLARE
  n_captured int;
  bad text;
BEGIN
  SELECT count(*) INTO n_captured FROM pit_captured_tables;
  IF n_captured <> 5 THEN
    RAISE EXCEPTION 'expected 5 captured tables, found %', n_captured;
  END IF;

  SELECT string_agg(format('%s (relreplident=%s)', table_name, relreplident), ', ' ORDER BY table_name)
    INTO bad
    FROM pit_captured_tables
   WHERE NOT replica_identity_full;
  IF bad IS NOT NULL THEN
    RAISE EXCEPTION 'not REPLICA IDENTITY FULL: %', bad;
  END IF;

  SELECT string_agg(table_name, ', ' ORDER BY table_name) INTO bad
    FROM pit_captured_tables WHERE NOT has_history_table;
  IF bad IS NOT NULL THEN
    RAISE EXCEPTION 'captured with no history table: %', bad;
  END IF;

  RAISE NOTICE 'ok: 5 captured tables, all relreplident=f with a history table';
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

-- 4. An update to patients writes a row to patients_history -----------------
DO $$
DECLARE
  pid    bigint;
  ins    patients_history;
  upd    patients_history;
  n_cols int;
BEGIN
  -- updated_at is backdated on purpose. now() is frozen for the whole
  -- transaction, so a row inserted here with the default would already carry
  -- the value the touch trigger is about to write, and the assertion below
  -- would prove nothing.
  INSERT INTO patients (mrn, first_name, last_name, date_of_birth, ssn, email, phone,
                        address_line1, city, state, postal_code, updated_at)
  VALUES ('MRN-VERIFY-0001', 'Verify', 'Fixture', '1971-03-04', '000-00-0000',
          'verify.fixture@example.invalid', '(555) 010-0100',
          '1 Test Way', 'Boston', 'MA', '02108', now() - interval '1 hour')
  RETURNING patient_id INTO pid;

  SELECT * INTO ins FROM patients_history
   WHERE (pk->>'patient_id')::bigint = pid AND op = 'I';
  IF ins IS NULL THEN
    RAISE EXCEPTION 'insert into patients wrote no I row to patients_history';
  END IF;
  IF ins.before_row IS NOT NULL OR ins.after_row->>'mrn' <> 'MRN-VERIFY-0001' THEN
    RAISE EXCEPTION 'patients_history I row has the wrong images: before=%, after=%',
      ins.before_row, ins.after_row;
  END IF;

  UPDATE patients SET email = 'changed@example.invalid' WHERE patient_id = pid;

  SELECT * INTO upd FROM patients_history
   WHERE (pk->>'patient_id')::bigint = pid AND op = 'U';
  IF upd IS NULL THEN
    RAISE EXCEPTION 'update to patients wrote no U row to patients_history';
  END IF;
  IF upd.before_row->>'email' <> 'verify.fixture@example.invalid'
     OR upd.after_row->>'email' <> 'changed@example.invalid' THEN
    RAISE EXCEPTION 'patients_history U row images are wrong: % -> %',
      upd.before_row->>'email', upd.after_row->>'email';
  END IF;

  -- The recorded before image is whole, not just the key columns.
  SELECT count(*) INTO n_cols
    FROM pg_attribute
   WHERE attrelid = 'public.patients'::regclass AND attnum > 0 AND NOT attisdropped;
  IF (SELECT count(*) FROM jsonb_object_keys(upd.before_row)) <> n_cols THEN
    RAISE EXCEPTION 'before image has % of % columns',
      (SELECT count(*) FROM jsonb_object_keys(upd.before_row)), n_cols;
  END IF;

  -- The point-in-time key: identical for every row in the transaction, so a
  -- multi-statement transaction can never be half-visible to a query at T.
  IF upd.tx_at <> transaction_timestamp() OR ins.tx_at <> upd.tx_at THEN
    RAISE EXCEPTION 'tx_at is not the transaction timestamp (ins=%, upd=%, tx=%)',
      ins.tx_at, upd.tx_at, transaction_timestamp();
  END IF;
  IF upd.stmt_at < upd.tx_at OR upd.recorded_at < upd.tx_at THEN
    RAISE EXCEPTION 'stamps are out of order: tx=%, stmt=%, recorded=%',
      upd.tx_at, upd.stmt_at, upd.recorded_at;
  END IF;

  -- updated_at is maintained by the source, not trusted from the writer.
  IF upd.after_row->>'updated_at' = upd.before_row->>'updated_at' THEN
    RAISE EXCEPTION 'updated_at was not touched by the update (still %)',
      upd.before_row->>'updated_at';
  END IF;

  RAISE NOTICE 'ok: insert and update to patients each wrote history, tx_at = %', upd.tx_at;
END;
$$;

-- 5. Cascades and SET NULL are recorded too ---------------------------------
-- One DELETE statement fanning out across tables is the case the oracle is
-- most likely to get wrong, so assert it here rather than discovering it in M8.
DO $$
DECLARE
  pid     bigint;
  prid    bigint;
  aid     bigint;
  nid     bigint;
  n_txids int;
BEGIN
  INSERT INTO providers (npi, full_name, specialty, email)
  VALUES ('0000000001', 'Verify Provider', 'family medicine', 'provider@example.invalid')
  RETURNING provider_id INTO prid;

  INSERT INTO patients (mrn, first_name, last_name, date_of_birth)
  VALUES ('MRN-VERIFY-0002', 'Cascade', 'Fixture', '1988-12-01')
  RETURNING patient_id INTO pid;

  INSERT INTO appointments (patient_id, provider_id, scheduled_at, status, intake_answers)
  VALUES (pid, prid, now(), 'scheduled',
          '{"chief_complaint": "left knee pain since a fall at work"}')
  RETURNING appointment_id INTO aid;

  INSERT INTO claims (patient_id, appointment_id, billed_amount, diagnosis_codes)
  VALUES (pid, aid, 412.50, ARRAY['M25.562','W19.XXXA']);

  INSERT INTO notes (patient_id, provider_id, appointment_id, body)
  VALUES (pid, prid, aid, 'Patient reports knee pain. Reached at (555) 010-0100.')
  RETURNING note_id INTO nid;

  -- ON DELETE SET NULL: an appointment delete surfaces on notes as an update.
  DELETE FROM appointments WHERE appointment_id = aid;

  IF NOT EXISTS (SELECT 1 FROM appointments_history
                  WHERE (pk->>'appointment_id')::bigint = aid AND op = 'D'
                    AND before_row->>'patient_id' = pid::text) THEN
    RAISE EXCEPTION 'deleting an appointment wrote no D row with a before image';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM claims_history
                  WHERE op = 'D' AND after_row IS NULL
                    AND before_row->>'appointment_id' = aid::text) THEN
    RAISE EXCEPTION 'the claims cascade was not recorded';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM notes_history
                  WHERE (pk->>'note_id')::bigint = nid AND op = 'U'
                    AND before_row->>'appointment_id' = aid::text
                    AND after_row->>'appointment_id' IS NULL) THEN
    RAISE EXCEPTION 'the notes ON DELETE SET NULL was not recorded as an update';
  END IF;

  DELETE FROM patients WHERE patient_id = pid;

  IF NOT EXISTS (SELECT 1 FROM notes_history
                  WHERE (pk->>'note_id')::bigint = nid AND op = 'D') THEN
    RAISE EXCEPTION 'the notes cascade from deleting a patient was not recorded';
  END IF;

  -- Everything this block did happened in one transaction, so it is one point
  -- on the timeline. Scoped to the fixture keys: pre-existing load in the
  -- ledger has its own txids and is none of this check's business.
  SELECT count(DISTINCT txid) INTO n_txids FROM (
    SELECT txid FROM patients_history     WHERE (pk->>'patient_id')::bigint = pid
    UNION ALL
    SELECT txid FROM appointments_history WHERE (pk->>'appointment_id')::bigint = aid
    UNION ALL
    SELECT txid FROM notes_history        WHERE (pk->>'note_id')::bigint = nid
  ) h;
  IF n_txids <> 1 THEN
    RAISE EXCEPTION 'expected a single txid across the ledger in one transaction, found %', n_txids;
  END IF;

  RAISE NOTICE 'ok: cascade deletes and ON DELETE SET NULL are both in the ledger';
END;
$$;

-- 6. Statements inside a transaction stay distinguishable --------------------
-- statement_timestamp() only advances per client command, so these have to be
-- separate top-level statements rather than lines inside a DO block. tx_at is
-- what a point-in-time query keys on; stmt_at is how you tell two changes to
-- the same row inside one transaction apart.
INSERT INTO patients (mrn, first_name, last_name, date_of_birth, email)
VALUES ('MRN-VERIFY-0003', 'Stamp', 'Fixture', '1990-06-15', 'first@example.invalid');

UPDATE patients SET email = 'second@example.invalid' WHERE mrn = 'MRN-VERIFY-0003';

UPDATE patients SET email = 'third@example.invalid'  WHERE mrn = 'MRN-VERIFY-0003';

DO $$
DECLARE
  n_tx   int;
  n_stmt int;
BEGIN
  SELECT count(DISTINCT tx_at), count(DISTINCT stmt_at)
    INTO n_tx, n_stmt
    FROM patients_history
   WHERE after_row->>'mrn' = 'MRN-VERIFY-0003';

  IF n_tx <> 1 THEN
    RAISE EXCEPTION 'three statements in one transaction produced % distinct tx_at', n_tx;
  END IF;
  IF n_stmt <> 3 THEN
    RAISE EXCEPTION 'expected 3 distinct stmt_at across 3 statements, found %', n_stmt;
  END IF;
  RAISE NOTICE 'ok: one tx_at, three stmt_at -- statements inside a transaction are ordered';
END;
$$;

-- Leave nothing behind: the fixtures above exist only to be checked.
ROLLBACK;

\echo 'PASS: schema, replica identity, and history triggers all verified'
