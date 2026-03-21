-- Chaos test schema for PostgreSQL source scenarios.
--
-- Schema mirrors the MySQL chaos tables so the SourceBackend implementations
-- use the same INSERT statements regardless of source type.

-- Tables

CREATE TABLE IF NOT EXISTS customers (
    id      SERIAL PRIMARY KEY,
    name    TEXT             NOT NULL,
    email   TEXT             NOT NULL UNIQUE,
    balance DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS order_events (
    id          SERIAL PRIMARY KEY,
    customer_id INTEGER          NOT NULL,
    amount      DOUBLE PRECISION,
    status      TEXT             NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

-- Full replica identity so DeltaForge gets before/after row images.
ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE order_events REPLICA IDENTITY FULL;

-- Publication: DeltaForge subscribes to this via the slot.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'deltaforge_pub'
    ) THEN
        CREATE PUBLICATION deltaforge_pub FOR TABLE customers, order_events;
    END IF;
END $$;

-- Seed a customer so foreign-key inserts into order_events work.
INSERT INTO customers (name, email, balance)
VALUES ('Alice', 'alice@chaos.local', 1000)
ON CONFLICT (email) DO NOTHING;
