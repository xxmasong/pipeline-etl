-- Warehouse objects the pipeline needs.

CREATE TABLE IF NOT EXISTS etl_watermarks (
    pipeline   TEXT        PRIMARY KEY,
    value      TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Rejected rows land here with the reason attached, so a bad record is
-- diagnosable without re-running the batch.
CREATE TABLE IF NOT EXISTS etl_quarantine (
    id         BIGSERIAL   PRIMARY KEY,
    pipeline   TEXT        NOT NULL,
    row        JSONB       NOT NULL,
    reasons    TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS etl_quarantine_pipeline_idx
    ON etl_quarantine (pipeline, created_at DESC);

-- Example target. The primary key is what makes the load idempotent: the
-- runner's overlapping read window deliberately re-sends rows.
CREATE TABLE IF NOT EXISTS fact_sales (
    id          TEXT        PRIMARY KEY,
    store_id    TEXT        NOT NULL,
    sold_at     TIMESTAMPTZ NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    currency    TEXT        NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fact_sales_sold_at_idx ON fact_sales (sold_at);
