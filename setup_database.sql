-- ========================================
-- Databricks Lakebase End-to-End Training Database Setup
-- Enhanced version with Audit Trail and Triggers
-- ========================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- ========================================
-- CORE TABLES
-- ========================================

-- Users table
CREATE TABLE IF NOT EXISTS ecommerce.users (
    user_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(50) UNIQUE NOT NULL,
    full_name VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    metadata JSONB,
    preferences JSONB DEFAULT '{}'::jsonb
);

-- Products table
CREATE TABLE IF NOT EXISTS ecommerce.products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    category VARCHAR(100),
    tags TEXT[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Orders table
CREATE TABLE IF NOT EXISTS ecommerce.orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES ecommerce.users(user_id),
    order_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(10, 2),
    shipping_address JSONB,
    payment_method VARCHAR(50)
);

-- Order items table with computed column
CREATE TABLE IF NOT EXISTS ecommerce.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES ecommerce.orders(order_id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES ecommerce.products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL,
    subtotal DECIMAL(10, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);

-- ========================================
-- AUDIT TRAIL TABLE (NEW)
-- Tracks all INSERT, UPDATE, DELETE operations
-- ========================================

CREATE TABLE IF NOT EXISTS ecommerce.audit_log (
    audit_id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    operation VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    record_id INTEGER,
    old_data JSONB,
    new_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT CURRENT_USER
);

-- Index for efficient audit log queries
CREATE INDEX IF NOT EXISTS idx_audit_log_table ON ecommerce.audit_log(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_log_operation ON ecommerce.audit_log(operation);
CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON ecommerce.audit_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_new_data ON ecommerce.audit_log USING GIN(new_data);

-- ========================================
-- AUDIT TRIGGER FUNCTIONS
-- ========================================

-- Generic audit trigger function for products
CREATE OR REPLACE FUNCTION ecommerce.audit_products()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, new_data)
        VALUES ('products', 'INSERT', NEW.product_id, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, old_data, new_data)
        VALUES ('products', 'UPDATE', NEW.product_id, row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, old_data)
        VALUES ('products', 'DELETE', OLD.product_id, row_to_json(OLD)::jsonb);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Audit trigger function for users
CREATE OR REPLACE FUNCTION ecommerce.audit_users()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, new_data)
        VALUES ('users', 'INSERT', NEW.user_id, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, old_data, new_data)
        VALUES ('users', 'UPDATE', NEW.user_id, row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, old_data)
        VALUES ('users', 'DELETE', OLD.user_id, row_to_json(OLD)::jsonb);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Audit trigger function for orders
CREATE OR REPLACE FUNCTION ecommerce.audit_orders()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, new_data)
        VALUES ('orders', 'INSERT', NEW.order_id, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, old_data, new_data)
        VALUES ('orders', 'UPDATE', NEW.order_id, row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, old_data)
        VALUES ('orders', 'DELETE', OLD.order_id, row_to_json(OLD)::jsonb);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Audit trigger function for order_items
CREATE OR REPLACE FUNCTION ecommerce.audit_order_items()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, new_data)
        VALUES ('order_items', 'INSERT', NEW.order_item_id, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, old_data, new_data)
        VALUES ('order_items', 'UPDATE', NEW.order_item_id, row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, old_data)
        VALUES ('order_items', 'DELETE', OLD.order_item_id, row_to_json(OLD)::jsonb);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- CREATE TRIGGERS
-- ========================================

-- Drop existing triggers if they exist (for idempotent script)
DROP TRIGGER IF EXISTS trg_audit_products ON ecommerce.products;
DROP TRIGGER IF EXISTS trg_audit_users ON ecommerce.users;
DROP TRIGGER IF EXISTS trg_audit_orders ON ecommerce.orders;
DROP TRIGGER IF EXISTS trg_audit_order_items ON ecommerce.order_items;

-- Create triggers for each table
CREATE TRIGGER trg_audit_products
    AFTER INSERT OR UPDATE OR DELETE ON ecommerce.products
    FOR EACH ROW EXECUTE FUNCTION ecommerce.audit_products();

CREATE TRIGGER trg_audit_users
    AFTER INSERT OR UPDATE OR DELETE ON ecommerce.users
    FOR EACH ROW EXECUTE FUNCTION ecommerce.audit_users();

CREATE TRIGGER trg_audit_orders
    AFTER INSERT OR UPDATE OR DELETE ON ecommerce.orders
    FOR EACH ROW EXECUTE FUNCTION ecommerce.audit_orders();

CREATE TRIGGER trg_audit_order_items
    AFTER INSERT OR UPDATE OR DELETE ON ecommerce.order_items
    FOR EACH ROW EXECUTE FUNCTION ecommerce.audit_order_items();

-- ========================================
-- INDEXES FOR CORE TABLES
-- ========================================

CREATE INDEX IF NOT EXISTS idx_users_email ON ecommerce.users(email);
CREATE INDEX IF NOT EXISTS idx_products_category ON ecommerce.products(category);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON ecommerce.orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON ecommerce.orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_date ON ecommerce.orders(order_date DESC);
CREATE INDEX IF NOT EXISTS idx_users_metadata ON ecommerce.users USING GIN(metadata);
CREATE INDEX IF NOT EXISTS idx_products_tags ON ecommerce.products USING GIN(tags);

-- ========================================
-- SAMPLE DATA
-- ========================================

-- Insert sample users
INSERT INTO ecommerce.users (email, username, full_name, metadata) VALUES
('john.doe@example.com', 'johndoe', 'John Doe', '{"role": "customer", "tier": "gold"}'),
('jane.smith@example.com', 'janesmith', 'Jane Smith', '{"role": "customer", "tier": "silver"}'),
('admin@company.com', 'admin', 'Admin User', '{"role": "admin", "permissions": ["all"]}'),
('bob.wilson@example.com', 'bobwilson', 'Bob Wilson', '{"role": "customer", "tier": "bronze"}'),
('alice.johnson@example.com', 'alicejohnson', 'Alice Johnson', '{"role": "vendor", "store": "TechGadgets"}')
ON CONFLICT (email) DO NOTHING;

-- Insert sample products
INSERT INTO ecommerce.products (name, description, price, stock_quantity, category, tags) VALUES
('Laptop Pro 2024', 'High-performance laptop with AI capabilities', 1999.99, 50, 'Electronics', ARRAY['laptop', 'ai', 'professional']),
('Wireless Mouse', 'Ergonomic wireless mouse with precision tracking', 49.99, 200, 'Accessories', ARRAY['mouse', 'wireless', 'ergonomic']),
('USB-C Hub', '7-in-1 USB-C hub with HDMI and SD card reader', 79.99, 150, 'Accessories', ARRAY['usb', 'hub', 'connectivity']),
('AI Development Book', 'Complete guide to AI and machine learning', 59.99, 100, 'Books', ARRAY['ai', 'programming', 'education']),
('Mechanical Keyboard', 'RGB mechanical keyboard with Cherry MX switches', 149.99, 75, 'Accessories', ARRAY['keyboard', 'gaming', 'rgb']),
('4K Monitor', '32-inch 4K monitor with HDR support', 599.99, 30, 'Electronics', ARRAY['monitor', '4k', 'display']),
('Webcam HD', '1080p webcam with noise-canceling microphone', 89.99, 120, 'Electronics', ARRAY['webcam', 'video', 'streaming']),
('Desk Lamp', 'LED desk lamp with adjustable brightness', 39.99, 200, 'Accessories', ARRAY['lamp', 'led', 'office']),
('Bluetooth Speaker', 'Portable speaker with 20-hour battery life', 79.99, 80, 'Electronics', ARRAY['speaker', 'bluetooth', 'portable']),
('External SSD 1TB', 'Fast external SSD with USB 3.2 support', 129.99, 60, 'Electronics', ARRAY['storage', 'ssd', 'portable'])
ON CONFLICT DO NOTHING;

-- Insert sample orders
DO $$
DECLARE
    v_order_id INT;
BEGIN
    -- Order 1: Completed order
    INSERT INTO ecommerce.orders (user_id, status, total_amount, shipping_address, payment_method)
    VALUES (1, 'completed', 2099.98, '{"street": "123 Main St", "city": "Denver", "state": "CO", "zip": "80202"}', 'credit_card')
    RETURNING order_id INTO v_order_id;

    INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price)
    VALUES
    (v_order_id, 1, 1, 1999.99),
    (v_order_id, 2, 2, 49.99);

    -- Order 2: Pending order
    INSERT INTO ecommerce.orders (user_id, status, total_amount, shipping_address, payment_method)
    VALUES (2, 'pending', 229.98, '{"street": "456 Oak Ave", "city": "Boulder", "state": "CO", "zip": "80301"}', 'paypal')
    RETURNING order_id INTO v_order_id;

    INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price)
    VALUES
    (v_order_id, 5, 1, 149.99),
    (v_order_id, 3, 1, 79.99);

    -- Order 3: Completed order
    INSERT INTO ecommerce.orders (user_id, status, total_amount, shipping_address, payment_method)
    VALUES (1, 'completed', 689.98, '{"street": "123 Main St", "city": "Denver", "state": "CO", "zip": "80202"}', 'credit_card')
    RETURNING order_id INTO v_order_id;

    INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price)
    VALUES
    (v_order_id, 6, 1, 599.99),
    (v_order_id, 7, 1, 89.99);

    -- Order 4: Shipped order
    INSERT INTO ecommerce.orders (user_id, status, total_amount, shipping_address, payment_method)
    VALUES (4, 'shipped', 209.98, '{"street": "789 Pine Rd", "city": "Fort Collins", "state": "CO", "zip": "80521"}', 'credit_card')
    RETURNING order_id INTO v_order_id;

    INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price)
    VALUES
    (v_order_id, 9, 1, 79.99),
    (v_order_id, 10, 1, 129.99);

END $$;

-- Update order totals
UPDATE ecommerce.orders o
SET total_amount = (
    SELECT SUM(subtotal)
    FROM ecommerce.order_items oi
    WHERE oi.order_id = o.order_id
)
WHERE total_amount IS NULL OR total_amount = 0;

-- ========================================
-- VERIFICATION QUERIES
-- ========================================

-- Display setup summary
SELECT 'Database setup completed successfully!' as status,
       (SELECT COUNT(*) FROM ecommerce.users) as users_count,
       (SELECT COUNT(*) FROM ecommerce.products) as products_count,
       (SELECT COUNT(*) FROM ecommerce.orders) as orders_count,
       (SELECT COUNT(*) FROM ecommerce.audit_log) as audit_log_entries;

-- Show sample audit log entries
SELECT 'Sample Audit Log Entries:' as info;
SELECT table_name, operation, record_id, created_at
FROM ecommerce.audit_log
ORDER BY created_at DESC
LIMIT 10;
