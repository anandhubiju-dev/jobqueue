ALTER TABLE jobs ADD COLUMN heartbeat_at timestamptz;

CREATE INDEX idx_jobs_processing_heartbeat ON jobs (heartbeat_at) WHERE status = 'processing';