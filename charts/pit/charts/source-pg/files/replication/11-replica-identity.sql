-- Full replication by default: every table in this database carries complete
-- before images, and a table created later carries them too.
--
-- Postgres has no GUC for this. REPLICA IDENTITY is per-table and defaults to
-- the primary key, which means an UPDATE or DELETE reaches the WAL carrying
-- only the key columns. Debezium then emits a change event with no `before`
-- state, and nothing downstream can tell what the row used to look like.
--
-- So the default is manufactured out of two pieces:
--
--   pit_replicate_all()        one-shot, idempotent, for what already exists
--   pit_replicate_new_table()  an event trigger, for what arrives later
--
-- Together they mean nobody has to remember to opt a table in. The publication
-- is FOR ALL TABLES for the same reason -- see 12-publication.sql. One gives a
-- new table its before image; the other puts it in the stream.
--
-- This file runs before 20-clinic-schema.sql on purpose. The clinic tables get
-- REPLICA IDENTITY FULL from the event trigger rather than from a list here, so
-- installing the schema is itself the proof that the mechanism works.
--
-- Cost, stated plainly: FULL makes every UPDATE and DELETE write the whole old
-- row to the WAL, and it makes the downstream apply do a full-row comparison
-- instead of a key lookup. For a synthetic clinic database that is cheap. On a
-- large real primary it is not, and the honest answer there is a unique index
-- per table rather than FULL.

-- Ordinary, permanent tables only. Views and foreign tables have no replica
-- identity, temp and unlogged tables never reach the WAL, and the pg_* schemas
-- are not ours to alter.
CREATE OR REPLACE FUNCTION pit_replicable_tables() RETURNS SETOF regclass
LANGUAGE sql STABLE AS $$
  SELECT c.oid::regclass
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE c.relkind IN ('r', 'p')
     AND c.relpersistence = 'p'
     AND n.nspname NOT IN ('pg_catalog', 'information_schema')
     AND n.nspname NOT LIKE 'pg\_%'
$$;

COMMENT ON FUNCTION pit_replicable_tables() IS
  'Tables that can carry a replica identity: ordinary or partitioned, permanent, outside the system schemas.';

-- Idempotent, so it is safe to re-run against a live database. Skips tables
-- that are already FULL rather than rewriting the catalog row for no reason.
CREATE OR REPLACE FUNCTION pit_replicate_all() RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
  t       regclass;
  changed integer := 0;
BEGIN
  FOR t IN
    SELECT r FROM pit_replicable_tables() AS r
     WHERE (SELECT relreplident FROM pg_class WHERE oid = r) <> 'f'
  LOOP
    EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', t::text);
    changed := changed + 1;
  END LOOP;
  RETURN changed;
END;
$$;

COMMENT ON FUNCTION pit_replicate_all() IS
  'Sets REPLICA IDENTITY FULL on every replicable table that is not already FULL. Returns how many it changed. Idempotent.';

-- The event trigger is what makes FULL the default rather than a chore. It is a
-- DDL trigger, not a row trigger: it fires once per CREATE TABLE and writes no
-- data. Nothing in this database has a row-level trigger.
--
-- ALTER TABLE inside an event trigger on the table being created is allowed --
-- ddl_command_end fires after the CREATE has completed.
CREATE OR REPLACE FUNCTION pit_replicate_new_table() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
  cmd record;
BEGIN
  FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    -- object_type is 'table' for both ordinary and partitioned tables. The
    -- objid check filters out temp and unlogged tables, which never reach the
    -- WAL and so have nothing to replicate.
    CONTINUE WHEN cmd.object_type <> 'table';
    CONTINUE WHEN NOT EXISTS (
      SELECT 1 FROM pit_replicable_tables() AS r WHERE r = cmd.objid
    );

    EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', cmd.object_identity);
  END LOOP;
END;
$$;

COMMENT ON FUNCTION pit_replicate_new_table() IS
  'Event-trigger body: gives a newly created table REPLICA IDENTITY FULL. Fires on DDL, writes no data.';

-- CREATE TABLE AS and SELECT INTO are included: they create a table, so they
-- create something the publication will replicate.
DROP EVENT TRIGGER IF EXISTS pit_replicate_new_table;
CREATE EVENT TRIGGER pit_replicate_new_table ON ddl_command_end
  WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
  EXECUTE FUNCTION pit_replicate_new_table();

-- Catches anything that existed before this file ran. On a fresh initdb that is
-- nothing, which is the point: the call has to be here for the case where the
-- file is applied to a database that already has tables.
SELECT pit_replicate_all();

-- What is actually replicated, according to the catalog rather than according to
-- this file. Verification and anything downstream that needs the table list
-- should read this instead of hardcoding names.
CREATE OR REPLACE VIEW pit_replicated_tables AS
SELECT n.nspname AS schema_name,
       c.relname AS table_name,
       c.relreplident,
       c.relreplident = 'f' AS replica_identity_full
  FROM pit_replicable_tables() AS r
  JOIN pg_class c     ON c.oid = r
  JOIN pg_namespace n ON n.oid = c.relnamespace;

COMMENT ON VIEW pit_replicated_tables IS
  'Every replicable table and whether it carries complete before images. Derived from pg_class, so it cannot drift from reality.';
