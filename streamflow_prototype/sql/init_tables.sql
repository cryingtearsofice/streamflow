CREATE TABLE IF NOT EXISTS valid_transactions (
    event_id uuid PRIMARY KEY,
    schema_version text NOT NULL,
    event_type text NOT NULL,
    event_ts timestamptz NOT NULL,
    source text NOT NULL,
    account_id text NOT NULL,
    amount numeric(18, 2) NOT NULL,
    status text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS invalid_transactions (
    id bigserial PRIMARY KEY,
    schema_version text,
    event_id text,
    event_type text,
    event_ts text,
    source text,
    account_id text,
    amount text,
    status text,
    original_record jsonb NOT NULL,
    validation_error text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now()
);
