-- DeltaForge PostgreSQL Development Setup
-- This script runs on container startup to configure CDC

-- Create the orders database schema
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    total DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    product_id INTEGER NOT NULL,
    sku VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);

-- Enable full replica identity for before images on CDC
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
ALTER TABLE customers REPLICA IDENTITY FULL;

-- Create publication for DeltaForge CDC
-- Note: In PostgreSQL, wal_level must be 'logical' (set in postgresql.conf)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'deltaforge_pub'
    ) THEN
        CREATE PUBLICATION deltaforge_pub FOR TABLE orders, order_items, customers;
    END IF;
END $$;

-- Insert sample data
INSERT INTO customers (email, name, metadata) VALUES
    ('alice@example.com', 'Alice Smith', '{"tier": "gold", "since": "2023-01-15"}'),
    ('bob@example.com', 'Bob Jones', '{"tier": "silver", "since": "2023-06-20"}'),
    ('carol@example.com', 'Carol White', '{"tier": "bronze", "since": "2024-01-01"}')
ON CONFLICT (email) DO NOTHING;

INSERT INTO orders (customer_id, status, total) VALUES
    (1, 'completed', 150.00),
    (1, 'pending', 75.50),
    (2, 'shipped', 200.00)
ON CONFLICT DO NOTHING;

INSERT INTO order_items (order_id, product_id, sku, quantity, price, metadata) VALUES
    (1, 101, 'WIDGET-001', 2, 50.00, '{"color": "red"}'),
    (1, 102, 'GADGET-002', 1, 50.00, '{"size": "large"}'),
    (2, 101, 'WIDGET-001', 1, 75.50, null),
    (3, 103, 'THING-003', 4, 50.00, '{"bundle": true}')
ON CONFLICT DO NOTHING;

-- Grant permissions to postgres user (for local dev)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO postgres;

-- Show setup summary
DO $$
DECLARE
    pub_count INTEGER;
    table_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO pub_count FROM pg_publication WHERE pubname = 'deltaforge_pub';
    SELECT COUNT(*) INTO table_count FROM pg_publication_tables WHERE pubname = 'deltaforge_pub';
    
    RAISE NOTICE '=== DeltaForge PostgreSQL Setup Complete ===';
    RAISE NOTICE 'Publication: deltaforge_pub (% tables)', table_count;
    RAISE NOTICE 'Replica Identity: FULL on all tables';
    RAISE NOTICE '';
    RAISE NOTICE 'To verify: SELECT * FROM pg_publication_tables;';
END $$;