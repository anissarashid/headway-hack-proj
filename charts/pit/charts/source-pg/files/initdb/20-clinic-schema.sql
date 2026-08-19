-- Clinic schema: synthetic PHI/PII with deliberately varied shapes.
--
-- Nothing in here is real data, and nothing in here is meant to be easy. The
-- de-identification policy downstream is only as good as the hardest column it
-- has been run against, so the shapes are chosen to be awkward on purpose:
--
--   * identifiers that are structured (ssn, npi, mrn) and want tokenizing
--   * a date that wants generalizing rather than masking (date_of_birth)
--   * an address split across columns, with the zip separate, because
--     HIPAA safe-harbor truncates zip to three digits and leaves the rest
--   * numeric money, which Debezium mangles by default (see claims)
--   * an array of diagnosis codes -- the sensitive part of a claim
--   * free text inside a jsonb document (appointments.intake_answers)
--   * free text in a column of its own (notes.body), the hardest case, where
--     the PHI is unstructured and co-mingled with clinically useful prose
--
-- Foreign keys are kept here even though the sink will not have them: this is
-- standing in for a realistic operational primary, and the FK actions are part
-- of what makes replay hard. See the comments on each one.

-- Constrained at the source; Debezium emits enums as strings, so the sink sees
-- plain text and cannot rely on the constraint.
CREATE TYPE appointment_status AS ENUM (
  'scheduled',
  'checked_in',
  'completed',
  'cancelled',
  'no_show'
);

CREATE TABLE patients (
  patient_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  -- Pseudonymous, but still a direct identifier: it joins to everything.
  mrn             text        NOT NULL UNIQUE,
  first_name      text        NOT NULL,
  middle_name     text,
  last_name       text        NOT NULL,
  -- date, not timestamp. A policy that masks this to NULL destroys every
  -- age-based cohort; one that shifts or generalizes to year keeps them.
  date_of_birth   date        NOT NULL,
  -- Stored with dashes. The load generator varies formatting deliberately, so
  -- do not add a CHECK that assumes one.
  ssn             text,
  email           text,
  phone           text,
  address_line1   text,
  address_line2   text,
  city            text,
  state           char(2),
  postal_code     text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE patients IS 'Synthetic patient demographics. Every column except the identity, mrn and timestamps is PHI or a quasi-identifier.';
COMMENT ON COLUMN patients.postal_code IS 'Kept separate from the street address so a zip3 generalization can be applied without parsing.';

CREATE TABLE providers (
  provider_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  -- National Provider Identifier: 10 digits, public, but a strong
  -- re-identification handle when joined to a small appointment set.
  npi          text        NOT NULL UNIQUE,
  full_name    text        NOT NULL,
  specialty    text,
  email        text,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE providers IS 'Synthetic providers. Not patient data, but a quasi-identifier in aggregate -- de-id policy has to make a call rather than skip the table.';

CREATE TABLE appointments (
  appointment_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  -- CASCADE on purpose: deleting a patient becomes many delete events across
  -- several tables from one statement, which is a genuinely hard replay case.
  patient_id       bigint      NOT NULL REFERENCES patients(patient_id) ON DELETE CASCADE,
  -- RESTRICT on purpose: providers are not deletable while they have history,
  -- so this FK never fans out.
  provider_id      bigint      NOT NULL REFERENCES providers(provider_id) ON DELETE RESTRICT,
  scheduled_at     timestamptz NOT NULL,
  checked_in_at    timestamptz,
  completed_at     timestamptz,
  duration_minutes int         NOT NULL DEFAULT 30 CHECK (duration_minutes > 0),
  status           appointment_status NOT NULL DEFAULT 'scheduled',
  location         text,
  -- Patient-supplied intake answers: free text nested inside a document, so a
  -- column-level policy cannot see it. Deliberately unstructured.
  intake_answers   jsonb,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX appointments_patient_id_idx  ON appointments (patient_id);
CREATE INDEX appointments_provider_id_idx ON appointments (provider_id);
CREATE INDEX appointments_scheduled_at_idx ON appointments (scheduled_at);

COMMENT ON COLUMN appointments.intake_answers IS 'Free text inside jsonb. A column-level de-id policy will pass this straight through unless it descends into the document.';

CREATE TABLE claims (
  claim_id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  patient_id             bigint      NOT NULL REFERENCES patients(patient_id) ON DELETE CASCADE,
  appointment_id         bigint      REFERENCES appointments(appointment_id) ON DELETE CASCADE,
  -- numeric, not float. Debezium's default decimal.handling.mode encodes these
  -- as base64 VariableScaleDecimal, which compares equal to nothing; the
  -- connector has to be configured for string or double before the oracle can
  -- diff amounts at all.
  billed_amount          numeric(12,2) NOT NULL CHECK (billed_amount >= 0),
  allowed_amount         numeric(12,2) CHECK (allowed_amount >= 0),
  paid_amount            numeric(12,2) CHECK (paid_amount >= 0),
  patient_responsibility numeric(12,2) CHECK (patient_responsibility >= 0),
  -- ICD-10. The array is the sensitive part of the row, and it is an array, so
  -- a policy that only knows how to hash scalars has nothing to say about it.
  diagnosis_codes        text[]      NOT NULL DEFAULT '{}',
  procedure_code         text,
  claim_status           text        NOT NULL DEFAULT 'submitted'
                           CHECK (claim_status IN ('submitted','pending','paid','denied','appealed')),
  submitted_at           timestamptz NOT NULL DEFAULT now(),
  adjudicated_at         timestamptz,
  created_at             timestamptz NOT NULL DEFAULT now(),
  updated_at             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX claims_patient_id_idx     ON claims (patient_id);
CREATE INDEX claims_appointment_id_idx ON claims (appointment_id);

COMMENT ON COLUMN claims.diagnosis_codes IS 'ICD-10 codes. Diagnoses are the sensitive part of a claim; generalizing to category is usually the only usable policy.';

CREATE TABLE notes (
  note_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  patient_id     bigint      NOT NULL REFERENCES patients(patient_id) ON DELETE CASCADE,
  provider_id    bigint      NOT NULL REFERENCES providers(provider_id) ON DELETE RESTRICT,
  -- SET NULL on purpose: deleting an appointment shows up here as an *update*,
  -- not a delete. A pipeline that only watches the deleted table gets this
  -- wrong. It shows up in the CDC stream as an update on notes carrying the old
  -- appointment_id in its before image, which is what REPLICA IDENTITY FULL is
  -- for -- under the primary-key default the before image is just note_id and
  -- the cause of the update is unrecoverable.
  appointment_id bigint      REFERENCES appointments(appointment_id) ON DELETE SET NULL,
  -- Self-reference: an amendment points at the note it supersedes.
  amends_note_id bigint      REFERENCES notes(note_id) ON DELETE SET NULL,
  note_type      text        NOT NULL DEFAULT 'progress'
                   CHECK (note_type IN ('progress','intake','discharge','telephone','addendum')),
  -- The hardest case. Names, dates, phone numbers, employers and relatives all
  -- appear here in prose, mixed in with the clinical content that has to
  -- survive de-identification for the data to be worth anything.
  body           text        NOT NULL,
  authored_at    timestamptz NOT NULL DEFAULT now(),
  signed_at      timestamptz,
  is_amended     boolean     NOT NULL DEFAULT false,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX notes_patient_id_idx     ON notes (patient_id);
CREATE INDEX notes_provider_id_idx    ON notes (provider_id);
CREATE INDEX notes_appointment_id_idx ON notes (appointment_id);

COMMENT ON TABLE notes IS 'Free-text clinical notes: unstructured PHI. The de-id policy either handles this or it does not work.';
