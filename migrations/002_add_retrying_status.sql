ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;

ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('queued', 'processing', 'completed', 'failed', 'retrying'));

CREATE INDEX idx_jobs_retrying_run_at ON jobs (run_at) WHERE status = 'retrying';