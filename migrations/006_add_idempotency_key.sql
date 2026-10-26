ALTER TABLE jobs ADD COLUMN idempotency_key text;

CREATE UNIQUE INDEX idx_jobs_idempotency_key ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;