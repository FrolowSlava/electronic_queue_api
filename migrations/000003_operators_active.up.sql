BEGIN;

ALTER TABLE operators
    ADD COLUMN active boolean NOT NULL DEFAULT true;

COMMIT;
