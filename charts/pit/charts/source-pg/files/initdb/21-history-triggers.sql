-- Mutation ledger: one <table>_history row per row-level change, stamped with
-- the transaction timestamp.
--
-- This is the correctness oracle. Given a target time T, the expected state of
-- a captured table is, for each primary key, the after_row of the newest
-- history entry with tx_at <= T -- absent if that entry was a delete. M8 diffs
-- the pipeline's point-in-time output against exactly that, so the ledger has
-- to be recorded here, inside the same transaction as the change, rather than
-- reconstructed later from CDC events. Reconstructing it from the thing under
-- test would not be an oracle.
--
-- Why tx_at and not clock time: transaction_timestamp() is identical for every
-- row in a transaction, so a multi-statement transaction lands at a single
-- point on the timeline and can never be half-visible to a query at T. All
-- three stamps are recorded because they answer different questions:
--
--   tx_at       transaction_timestamp() -- the point-in-time key
--   stmt_at     statement_timestamp()   -- which statement inside the tx
--   recorded_at clock_timestamp()       -- real elapsed time, for debugging
--
-- Known gap: TRUNCATE does not fire row-level triggers, so it is invisible
-- here. Do not truncate a captured table -- delete instead.

-- Keeps updated_at honest without trusting the writer to set it. Installed
-- BEFORE the audit trigger fires, so the recorded after_row carries the new
-- value rather than the stale one.
CREATE OR REPLACE FUNCTION pit_touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- One function for every captured table: the target history table is derived
-- from TG_TABLE_NAME and the primary-key column names arrive as trigger args,
-- so adding a table means installing the trigger, not editing this.
--
-- SECURITY DEFINER so a writer cannot mutate a captured table without also
-- writing history -- the ledger must not depend on the load generator holding
-- INSERT on the history tables. search_path is pinned because of it; the
-- dynamic INSERT is schema-qualified from TG_TABLE_SCHEMA.
CREATE OR REPLACE FUNCTION pit_audit() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
  before_j jsonb;
  after_j  jsonb;
  pk_j     jsonb;
BEGIN
  IF TG_OP = 'INSERT' THEN
    after_j := to_jsonb(NEW);
  ELSIF TG_OP = 'UPDATE' THEN
    before_j := to_jsonb(OLD);
    after_j  := to_jsonb(NEW);
  ELSE
    before_j := to_jsonb(OLD);
  END IF;

  -- The key is stored as an object rather than a scalar so composite primary
  -- keys need no special case downstream.
  SELECT jsonb_object_agg(e.key, e.value)
    INTO pk_j
    FROM jsonb_each(coalesce(after_j, before_j)) AS e
   WHERE e.key = ANY (TG_ARGV);

  IF pk_j IS NULL THEN
    RAISE EXCEPTION 'pit_audit: none of the key columns % exist on %.%',
      TG_ARGV, TG_TABLE_SCHEMA, TG_TABLE_NAME;
  END IF;

  EXECUTE format(
    'INSERT INTO %I.%I (op, txid, tx_at, stmt_at, recorded_at, pk, before_row, after_row)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)',
    TG_TABLE_SCHEMA, TG_TABLE_NAME || '_history')
  USING left(TG_OP, 1),
        pg_current_xact_id()::text::bigint,
        transaction_timestamp(),
        statement_timestamp(),
        clock_timestamp(),
        pk_j,
        before_j,
        after_j;

  RETURN NULL;  -- AFTER trigger; the return value is discarded
END;
$$;

-- Marks a table as captured: full before images, a history table, the audit
-- trigger, and updated_at maintenance if the column is there. Idempotent, so
-- it can be re-run against an existing database when a table is added.
CREATE OR REPLACE FUNCTION pit_install_capture(target regclass) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  sch      text;
  tbl      text;
  hist     text;
  pkcols   text[];
  pkargs   text;
  has_upd  boolean;
BEGIN
  SELECT n.nspname, c.relname
    INTO sch, tbl
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE c.oid = target;

  hist := tbl || '_history';

  -- Order does not matter: the key is stored as a jsonb object.
  SELECT array_agg(a.attname::text)
    INTO pkcols
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
   WHERE i.indrelid = target AND i.indisprimary;

  IF pkcols IS NULL THEN
    RAISE EXCEPTION 'pit_install_capture: % has no primary key, so its history could not be keyed', target;
  END IF;

  -- Complete before images for updates and deletes. Without this, an update
  -- reaches Debezium carrying only the key columns and the oracle has nothing
  -- to compare the pipeline's before state against.
  EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL', sch, tbl);

  -- The history table is intentionally schema-agnostic: two jsonb documents
  -- instead of a mirror of the source columns, so a later ALTER TABLE on the
  -- captured table does not silently stop being recorded.
  EXECUTE format($ddl$
    CREATE TABLE IF NOT EXISTS %I.%I (
      history_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      op          char(1)     NOT NULL CHECK (op IN ('I','U','D')),
      txid        bigint      NOT NULL,
      tx_at       timestamptz NOT NULL,
      stmt_at     timestamptz NOT NULL,
      recorded_at timestamptz NOT NULL,
      pk          jsonb       NOT NULL,
      before_row  jsonb,
      after_row   jsonb,
      CONSTRAINT %I CHECK (
        (op = 'I' AND before_row IS NULL     AND after_row IS NOT NULL) OR
        (op = 'U' AND before_row IS NOT NULL AND after_row IS NOT NULL) OR
        (op = 'D' AND before_row IS NOT NULL AND after_row IS NULL)
      )
    )$ddl$, sch, hist, hist || '_op_images_agree');

  -- The oracle's only access pattern: newest entry per key at or before T.
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I (pk, tx_at DESC, history_id DESC)',
                 hist || '_pk_tx_at_idx', sch, hist);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I (tx_at)',
                 hist || '_tx_at_idx', sch, hist);

  SELECT string_agg(quote_literal(c), ', ') INTO pkargs FROM unnest(pkcols) AS c;

  EXECUTE format('DROP TRIGGER IF EXISTS pit_audit ON %I.%I', sch, tbl);
  -- One trigger for all three operations: an op-specific trigger per table is
  -- three times the surface for the ledger to be incomplete in.
  EXECUTE format(
    'CREATE TRIGGER pit_audit AFTER INSERT OR UPDATE OR DELETE ON %I.%I
     FOR EACH ROW EXECUTE FUNCTION pit_audit(%s)', sch, tbl, pkargs);

  SELECT EXISTS (
    SELECT 1 FROM pg_attribute
     WHERE attrelid = target AND attname = 'updated_at' AND attnum > 0 AND NOT attisdropped
  ) INTO has_upd;

  IF has_upd THEN
    EXECUTE format('DROP TRIGGER IF EXISTS pit_touch_updated_at ON %I.%I', sch, tbl);
    -- Named to sort before pit_audit; Postgres fires triggers of the same
    -- timing in name order, and BEFORE beats AFTER regardless, but the audit
    -- reading the touched value is load-bearing enough to be explicit about.
    EXECUTE format(
      'CREATE TRIGGER pit_touch_updated_at BEFORE UPDATE ON %I.%I
       FOR EACH ROW EXECUTE FUNCTION pit_touch_updated_at()', sch, tbl);
  END IF;
END;
$$;

COMMENT ON FUNCTION pit_install_capture(regclass) IS
  'Marks a table as captured: REPLICA IDENTITY FULL, a <table>_history ledger, the pit_audit trigger, and updated_at maintenance. Idempotent.';

SELECT pit_install_capture('public.patients');
SELECT pit_install_capture('public.providers');
SELECT pit_install_capture('public.appointments');
SELECT pit_install_capture('public.claims');
SELECT pit_install_capture('public.notes');

-- What is captured, according to the database rather than according to this
-- file. Verification and anything downstream that needs the table list should
-- read this instead of hardcoding names.
--
-- The history tables are deliberately not captured themselves: they are
-- append-only, so their default replica identity is sufficient, and capturing
-- the oracle alongside the thing it grades invites circular reasoning.
CREATE OR REPLACE VIEW pit_captured_tables AS
SELECT n.nspname AS schema_name,
       c.relname AS table_name,
       c.relreplident,
       c.relreplident = 'f' AS replica_identity_full,
       to_regclass(format('%I.%I', n.nspname, c.relname || '_history')) IS NOT NULL AS has_history_table
  FROM pg_trigger t
  JOIN pg_class c     ON c.oid = t.tgrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE t.tgname = 'pit_audit'
   AND NOT t.tgisinternal;

COMMENT ON VIEW pit_captured_tables IS 'Captured tables and whether each is correctly set up for CDC. Derived from the installed triggers, so it cannot drift from reality.';
