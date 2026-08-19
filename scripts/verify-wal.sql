-- Acceptance check for full replication: the before image reaches the WAL.
--
-- Run by both `make verify` (in-cluster) and scripts/verify-conf-docker.sh (no
-- cluster), so the two paths cannot drift.
--
-- Why this exists as its own script. `REPLICA IDENTITY FULL` showing up in
-- pg_class is not the same as the old row reaching the WAL, and since the
-- mutation ledger went away the WAL is the only copy of it. So decode the real
-- stream and look for the old value.
--
-- Unlike verify-schema.sql this cannot run inside a transaction that rolls back:
-- logical decoding returns only committed transactions, so a rollback-everything
-- script would find an empty stream. Every statement here commits. The fixture
-- table is created and dropped, and the slot is dropped at both ends.
--
-- If this script fails partway the slot can survive, and a slot nobody reads
-- retains WAL. That is what the drop at the top is for; re-running cleans up.

\set ON_ERROR_STOP on
\set slot 'pit_verify_wal'
\set marker 'before-image-must-survive'

-- psql does not interpolate :vars inside a dollar-quoted body, so the DO block
-- below reads them back through current_setting instead.
SELECT set_config('pit.slot',   :'slot',   false),
       set_config('pit.marker', :'marker', false);

-- The publication is read from the catalog rather than passed in: there should
-- be exactly one FOR ALL TABLES publication, and finding it is part of the test.
DO $$
DECLARE pub text;
BEGIN
  SELECT pubname INTO pub FROM pg_publication
   WHERE puballtables ORDER BY pubname LIMIT 1;
  IF pub IS NULL THEN
    RAISE EXCEPTION 'no FOR ALL TABLES publication exists -- initdb did not create one';
  END IF;
  PERFORM set_config('pit.publication', pub, false);
  RAISE NOTICE 'ok: decoding through publication %', pub;
END;
$$;

-- Left over from a previous failed run. The script aborts on the first failed
-- assertion, so both the slot and the fixture table can survive it.
SELECT pg_drop_replication_slot(slot_name)
  FROM pg_replication_slots WHERE slot_name = :'slot';
DROP TABLE IF EXISTS pit_verify_wal;

-- Created before the slot so its DDL is not in the stream we read. The event
-- trigger gives it REPLICA IDENTITY FULL, and FOR ALL TABLES puts it in the
-- publication -- neither of which anyone asked for here. That is the point.
CREATE TABLE pit_verify_wal (id bigint PRIMARY KEY, payload text);

DO $$
DECLARE ident char;
BEGIN
  SELECT relreplident INTO ident FROM pg_class
   WHERE oid = 'public.pit_verify_wal'::regclass;
  IF ident <> 'f' THEN
    RAISE EXCEPTION 'fixture table came out relreplident=%, expected f', ident;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_publication_tables
                  WHERE pubname = current_setting('pit.publication')
                    AND tablename = 'pit_verify_wal') THEN
    RAISE EXCEPTION 'fixture table is not in publication % -- FOR ALL TABLES did not pick it up',
      current_setting('pit.publication');
  END IF;
  RAISE NOTICE 'ok: a table nobody configured is FULL and in the publication';
END;
$$;

-- The row is inserted BEFORE the slot exists, so the insert is not in the stream.
-- That is what makes the check below a clean boolean rather than an exercise in
-- counting: after this point the marker exists only in the old version of the
-- row, so the only way it can reach the WAL at all is as a before image.
INSERT INTO pit_verify_wal VALUES (1, :'marker');

SELECT pg_create_logical_replication_slot(:'slot', 'pgoutput');

-- Separate top-level statements so each commits and reaches the stream.
UPDATE pit_verify_wal SET payload = 'after-image' WHERE id = 1;

DELETE FROM pit_verify_wal WHERE id = 1;

-- The assertion. With REPLICA IDENTITY FULL the update writes the whole old row
-- to the WAL, so the marker is there. With the primary-key default the old tuple
-- carries only `id` and the marker is absent -- the decoded stream would still
-- look healthy, with an update and a delete in it, and still be useless.
--
-- 'after-image' is checked too. Without it a stream that decoded nothing useful
-- could fail for the wrong reason and read as a replica-identity problem.
DO $$
DECLARE
  stream text;
  marker text := current_setting('pit.marker');
BEGIN
  SELECT string_agg(encode(data, 'escape'), E'\n') INTO stream
    FROM pg_logical_slot_peek_binary_changes(
      current_setting('pit.slot'), NULL, NULL,
      'proto_version', '1',
      'publication_names', current_setting('pit.publication'));

  IF stream IS NULL THEN
    RAISE EXCEPTION 'the replication slot decoded nothing at all';
  END IF;

  IF position('after-image' IN stream) = 0 THEN
    RAISE EXCEPTION 'the decoded stream does not contain the updated row -- the fixture never reached the WAL, so this says nothing about replica identity';
  END IF;

  IF position(marker IN stream) = 0 THEN
    RAISE EXCEPTION 'the pre-update value is absent from the decoded WAL: the update carried no before image, so REPLICA IDENTITY is not FULL on the wire';
  END IF;

  RAISE NOTICE 'ok: the pre-update row reaches the WAL -- the before image survives';
END;
$$;

SELECT pg_drop_replication_slot(:'slot');
DROP TABLE pit_verify_wal;

\echo 'PASS: a new table is replicated in full, and the before image reaches the WAL'
