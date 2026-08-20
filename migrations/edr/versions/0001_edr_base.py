"""Create the immutable EDR journal and durable projection tables."""

from collections.abc import Sequence

from alembic import op

revision: str = "0001_edr_base"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE edr_events (
      event_id text PRIMARY KEY, schema_version integer NOT NULL CHECK (schema_version > 0),
      event_type text NOT NULL, event_time timestamptz NOT NULL,
      ingestion_time timestamptz NOT NULL, job_id text NOT NULL, correlation_id text,
      job_type text, attempt_number integer NOT NULL CHECK (attempt_number >= 0),
      max_attempts integer NOT NULL CHECK (max_attempts > 0), canonical_payload jsonb NOT NULL,
      payload_hash char(64) NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
      kafka_topic text NOT NULL, kafka_partition integer NOT NULL, kafka_offset bigint NOT NULL,
      persisted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      UNIQUE (kafka_topic, kafka_partition, kafka_offset)
    );
    CREATE INDEX edr_events_projection_idx ON edr_events (persisted_at, event_id);
    CREATE INDEX edr_events_job_idx ON edr_events (job_id, event_time, event_id);

    CREATE FUNCTION preserve_edr_event() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF TG_OP = 'UPDATE'
         AND NEW.event_id = OLD.event_id
         AND NEW.payload_hash = OLD.payload_hash
         AND NEW.canonical_payload = OLD.canonical_payload
      THEN RETURN OLD; END IF;
      RAISE EXCEPTION 'edr_events are immutable (event_id=%)', OLD.event_id
        USING ERRCODE = 'integrity_constraint_violation';
    END $$;
    CREATE TRIGGER edr_events_immutable BEFORE UPDATE OR DELETE ON edr_events
      FOR EACH ROW EXECUTE FUNCTION preserve_edr_event();

    CREATE TABLE job_visibility (
      job_id text PRIMARY KEY, correlation_id text NOT NULL DEFAULT '',
      job_type text NOT NULL DEFAULT 'GENERIC', recorded_status text NOT NULL,
      projection jsonb NOT NULL, data_as_of timestamptz NOT NULL,
      version integer NOT NULL DEFAULT 0 CHECK (version >= 0)
    );
    CREATE INDEX job_visibility_search_idx ON job_visibility (correlation_id, recorded_status);
    CREATE TABLE projected_events (
      event_id text PRIMARY KEY REFERENCES edr_events(event_id) ON DELETE RESTRICT,
      job_id text NOT NULL, projected_at timestamptz NOT NULL DEFAULT clock_timestamp()
    );
    CREATE TABLE job_attempts (
      job_id text NOT NULL REFERENCES job_visibility(job_id) ON DELETE CASCADE,
      attempt_number integer NOT NULL CHECK (attempt_number > 0), projection jsonb NOT NULL,
      PRIMARY KEY (job_id, attempt_number)
    );
    CREATE TABLE projection_decisions (
      event_id text PRIMARY KEY REFERENCES edr_events(event_id) ON DELETE RESTRICT,
      job_id text NOT NULL, decision text NOT NULL, reason text,
      decided_at timestamptz NOT NULL DEFAULT clock_timestamp()
    );
    CREATE TABLE reconciliation_findings (
      finding_id bigserial PRIMARY KEY, job_id text NOT NULL, code text NOT NULL,
      message text NOT NULL, first_observed_at timestamptz NOT NULL, active boolean NOT NULL,
      resolved_at timestamptz, UNIQUE (job_id, code, first_observed_at)
    );
    CREATE INDEX reconciliation_findings_active_idx ON reconciliation_findings (job_id, code)
      WHERE active;
    GRANT SELECT, INSERT, UPDATE ON edr_events TO edr_sink;
    """)


def downgrade() -> None:
    op.execute("""
    DROP TABLE reconciliation_findings; DROP TABLE projection_decisions; DROP TABLE job_attempts;
    DROP TABLE projected_events; DROP TABLE job_visibility; DROP TABLE edr_events;
    DROP FUNCTION preserve_edr_event()
    """)
