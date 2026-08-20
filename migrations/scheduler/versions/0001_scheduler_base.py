"""Create the scheduler-owned durable queue and transactional outbox."""

from collections.abc import Sequence

from alembic import op

revision: str = "0001_scheduler_base"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE scheduler_jobs (
      job_id text PRIMARY KEY, correlation_id text NOT NULL, job_type text NOT NULL,
      payload jsonb, payload_reference text, scheduled_at timestamptz NOT NULL,
      available_at timestamptz NOT NULL, status text NOT NULL,
      attempt_number integer NOT NULL DEFAULT 0 CHECK (attempt_number >= 0),
      max_attempts integer NOT NULL CHECK (max_attempts > 0), claimed_by text,
      claim_token uuid, claimed_at timestamptz, claim_expires_at timestamptz,
      created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      version integer NOT NULL DEFAULT 0 CHECK (version >= 0),
      CHECK ((payload IS NULL) <> (payload_reference IS NULL)),
      CHECK ((claim_token IS NULL) = (claimed_by IS NULL)),
      CHECK (available_at >= scheduled_at),
      CHECK ((claim_token IS NULL) = (claim_expires_at IS NULL))
    );
    CREATE INDEX scheduler_jobs_eligibility_idx
      ON scheduler_jobs (available_at, job_id) WHERE status IN ('PENDING', 'RETRY_WAIT');
    CREATE INDEX scheduler_jobs_correlation_idx ON scheduler_jobs (correlation_id);
    CREATE INDEX scheduler_jobs_expired_claim_idx ON scheduler_jobs (claim_expires_at)
      WHERE claim_token IS NOT NULL;

    CREATE TABLE scheduler_attempts (
      job_id text NOT NULL REFERENCES scheduler_jobs(job_id) ON DELETE RESTRICT,
      attempt_number integer NOT NULL CHECK (attempt_number > 0), claim_token uuid,
      claimed_by text, claimed_at timestamptz, started_at timestamptz,
      completed_at timestamptz, outcome text, retryable boolean, error_code text,
      result_summary jsonb, next_retry_at timestamptz,
      PRIMARY KEY (job_id, attempt_number)
    );

    CREATE TABLE scheduler_outbox (
      event_id text PRIMARY KEY, topic text NOT NULL, message_key text NOT NULL,
      schema_version integer NOT NULL CHECK (schema_version > 0),
      canonical_payload text NOT NULL, payload_hash char(64) NOT NULL,
      created_at timestamptz NOT NULL DEFAULT clock_timestamp(), published_at timestamptz,
      publish_attempts integer NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
      last_error text, next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      lease_owner text, lease_token uuid, lease_expires_at timestamptz,
      kafka_partition integer, kafka_offset bigint,
      CHECK (payload_hash ~ '^[0-9a-f]{64}$')
    );
    CREATE INDEX scheduler_outbox_due_idx ON scheduler_outbox (next_attempt_at, created_at)
      WHERE published_at IS NULL;
    """)


def downgrade() -> None:
    op.execute(
        "DROP TABLE scheduler_outbox; DROP TABLE scheduler_attempts; DROP TABLE scheduler_jobs"
    )
