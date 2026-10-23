BEGIN;

CREATE TABLE operators (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY CHECK (id > 0),
    name text NOT NULL CHECK (btrim(name) <> '')
);

CREATE TABLE tickets (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY CHECK (id > 0),
    status text NOT NULL DEFAULT 'waiting'
        CHECK (status IN ('waiting', 'serving', 'completed', 'skipped')),
    operator_id bigint REFERENCES operators(id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    CONSTRAINT tickets_state_fields CHECK (
        (status = 'waiting' AND operator_id IS NULL AND started_at IS NULL AND finished_at IS NULL)
        OR (status = 'serving' AND operator_id IS NOT NULL AND started_at IS NOT NULL AND finished_at IS NULL)
        OR (status IN ('completed', 'skipped') AND operator_id IS NOT NULL AND started_at IS NOT NULL AND finished_at IS NOT NULL)
    ),
    CONSTRAINT tickets_time_order CHECK (
        (started_at IS NULL OR started_at >= created_at)
        AND (finished_at IS NULL OR finished_at >= started_at)
    )
);

CREATE INDEX tickets_status_id_idx ON tickets (status, id);
CREATE UNIQUE INDEX tickets_one_serving_per_operator
    ON tickets (operator_id) WHERE status = 'serving';

COMMIT;
