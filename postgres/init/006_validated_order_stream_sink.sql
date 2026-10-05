-- Add a stable Kafka record key to the drift sink so redelivered records
-- do not create duplicate rows. UNIQUE permits multiple NULLs, preserving
-- the existing seed script's rows that do not carry order IDs.
\c datashield

ALTER TABLE validated_orders
    ADD COLUMN IF NOT EXISTS order_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_validated_orders_order_id
    ON validated_orders (order_id);

GRANT ALL PRIVILEGES ON TABLE validated_orders TO datashield;
