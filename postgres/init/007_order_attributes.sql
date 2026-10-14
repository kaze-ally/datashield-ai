\c datashield

-- Extend existing installations as well as fresh databases with the current
-- order dimensions consumed by persistence and drift monitoring.
ALTER TABLE validated_orders
    ADD COLUMN IF NOT EXISTS unit_price DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS currency TEXT,
    ADD COLUMN IF NOT EXISTS customer_segment TEXT,
    ADD COLUMN IF NOT EXISTS region TEXT,
    ADD COLUMN IF NOT EXISTS shipping_method TEXT,
    ADD COLUMN IF NOT EXISTS device_type TEXT,
    ADD COLUMN IF NOT EXISTS order_channel TEXT,
    ADD COLUMN IF NOT EXISTS is_first_purchase BOOLEAN,
    ADD COLUMN IF NOT EXISTS loyalty_tier TEXT,
    ADD COLUMN IF NOT EXISTS warehouse_id TEXT;

GRANT ALL PRIVILEGES ON TABLE validated_orders TO datashield;
