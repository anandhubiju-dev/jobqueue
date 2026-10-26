DELETE FROM charges a
USING charges b
WHERE a.job_id = b.job_id AND a.id > b.id;

ALTER TABLE charges ADD CONSTRAINT charges_job_id_key UNIQUE (job_id);