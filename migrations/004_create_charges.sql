CREATE TABLE charges (
    id           bigserial   PRIMARY KEY,
    job_id       uuid        NOT NULL,
    amount_cents integer     NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);