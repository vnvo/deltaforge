-- Soak test schema for PostgreSQL: 120 tables across 6 domains (20 per domain).
--
-- Mirrors the MySQL soak tables with Postgres type equivalents.
-- All tables have the same 4 writer columns (tag, data, value, status) with
-- defaults so INSERT (tag, data, value, status) works uniformly.
--
-- Domains: customer, order, product, inventory, payment, event
--
-- Requirements:
--   - Run against the 'orders' database
--   - REPLICA IDENTITY FULL on all tables (required for DeltaForge CDC)
--   - Publication 'deltaforge_soak_pub' covering all soak tables

-- ── Helper: generate soak tables via DO block ──────────────────────────────────
-- Each domain gets domain-specific extra columns with appropriate Postgres types.

DO $$
DECLARE
    i INT;
    tbl TEXT;
    domains TEXT[] := ARRAY['customer', 'order', 'product', 'inventory', 'payment', 'event'];
    domain TEXT;
    extra_cols TEXT;
    all_tables TEXT[] := ARRAY[]::TEXT[];
BEGIN
    FOREACH domain IN ARRAY domains LOOP
        -- Domain-specific extra columns
        CASE domain
            WHEN 'customer' THEN
                extra_cols := ', name VARCHAR(255) DEFAULT '''', email VARCHAR(255) DEFAULT '''', phone VARCHAR(32) DEFAULT '''', active BOOLEAN DEFAULT TRUE, loyalty_points INTEGER DEFAULT 0, credit_score REAL DEFAULT 0.0, preferences JSONB DEFAULT NULL, dob DATE DEFAULT NULL, last_login TIMESTAMP DEFAULT NULL';
            WHEN 'order' THEN
                extra_cols := ', currency CHAR(3) DEFAULT ''USD'', items_count SMALLINT DEFAULT 0, shipping_cost NUMERIC(8,4) DEFAULT 0.0000, gross_amount NUMERIC(12,4) DEFAULT 0.0000, is_gift BOOLEAN DEFAULT FALSE, placed_at TIMESTAMP DEFAULT NULL, dispatched_at TIMESTAMP DEFAULT NULL, notes TEXT';
            WHEN 'product' THEN
                extra_cols := ', sku VARCHAR(64) DEFAULT '''', name VARCHAR(255) DEFAULT '''', description TEXT, price NUMERIC(10,4) DEFAULT 0.0000, weight REAL DEFAULT 0.0, dimensions JSONB DEFAULT NULL, is_active BOOLEAN DEFAULT TRUE, category_id INTEGER DEFAULT 0, brand VARCHAR(128) DEFAULT ''''';
            WHEN 'inventory' THEN
                extra_cols := ', warehouse_id INTEGER DEFAULT 0, sku VARCHAR(64) DEFAULT '''', quantity INTEGER DEFAULT 0, reserved INTEGER DEFAULT 0, reorder_point SMALLINT DEFAULT 0, unit_cost NUMERIC(10,4) DEFAULT 0.0000, last_counted DATE DEFAULT NULL, location VARCHAR(32) DEFAULT ''''';
            WHEN 'payment' THEN
                extra_cols := ', method VARCHAR(32) DEFAULT ''card'', currency CHAR(3) DEFAULT ''USD'', amount NUMERIC(12,4) DEFAULT 0.0000, fee NUMERIC(8,4) DEFAULT 0.0000, gateway_ref VARCHAR(128) DEFAULT '''', settled BOOLEAN DEFAULT FALSE, settled_at TIMESTAMP DEFAULT NULL';
            WHEN 'event' THEN
                extra_cols := ', event_type VARCHAR(64) DEFAULT '''', source VARCHAR(128) DEFAULT '''', payload JSONB DEFAULT NULL, severity SMALLINT DEFAULT 0, acknowledged BOOLEAN DEFAULT FALSE, expires_at TIMESTAMP DEFAULT NULL';
        END CASE;

        FOR i IN 1..20 LOOP
            tbl := format('soak_%s_%s', domain, lpad(i::text, 2, '0'));
            all_tables := array_append(all_tables, tbl);

            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I (
                    id SERIAL PRIMARY KEY,
                    tag VARCHAR(100) NOT NULL DEFAULT '''',
                    data TEXT,
                    value NUMERIC(10,4) DEFAULT 0.0000,
                    status SMALLINT DEFAULT 0
                    %s,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )', tbl, extra_cols
            );

            -- REPLICA IDENTITY FULL is required for DeltaForge to get before/after images.
            EXECUTE format('ALTER TABLE %I REPLICA IDENTITY FULL', tbl);

            -- Create indexes for common query patterns.
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_tag ON %I (tag)', tbl, tbl);
        END LOOP;
    END LOOP;

    -- Create or replace publication covering all soak tables.
    -- Drop and recreate to handle table additions cleanly.
    IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'deltaforge_soak_pub') THEN
        DROP PUBLICATION deltaforge_soak_pub;
    END IF;

    EXECUTE format(
        'CREATE PUBLICATION deltaforge_soak_pub FOR TABLE %s',
        array_to_string(all_tables, ', ')
    );

    RAISE NOTICE 'Created % soak tables with publication deltaforge_soak_pub', array_length(all_tables, 1);
END $$;
