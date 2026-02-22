#!/usr/bin/env python3
"""
Setup Lakebase Autoscaling tables with audit trail for End-to-End Training App

Supports two connection modes:
  1. Autoscaling (recommended): Set LAKEBASE_PROJECT_ID and LAKEBASE_BRANCH_ID
     - Host and credentials are discovered automatically via w.postgres SDK
  2. Legacy/Provisioned: Set PGHOST, PGUSER, PGPASSWORD directly

Usage:
    python setup_lakebase_tables.py
    LAKEBASE_PROJECT_ID=my-project python setup_lakebase_tables.py
"""

import os
import sys
import subprocess
import socket
import time

from databricks.sdk import WorkspaceClient

# Configuration
LAKEBASE_PROJECT_ID = os.getenv('LAKEBASE_PROJECT_ID', '')
LAKEBASE_BRANCH_ID = os.getenv('LAKEBASE_BRANCH_ID', 'production')
LAKEBASE_DATABASE = os.getenv('PGDATABASE', 'databricks_postgres')
LAKEBASE_PORT = os.getenv('PGPORT', '5432')

# Legacy overrides
PGHOST_OVERRIDE = os.getenv('PGHOST', '')
PGUSER_OVERRIDE = os.getenv('PGUSER', '')


def _resolve_hostname(hostname):
    """Resolve hostname with macOS DNS workaround."""
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        pass
    try:
        result = subprocess.run(
            ["dig", "+short", hostname],
            capture_output=True, text=True, timeout=5,
        )
        ips = result.stdout.strip().split('\n')
        for ip in ips:
            if ip and not ip.startswith(';') and '.' in ip:
                return ip
    except Exception:
        pass
    return None


def get_connection_params():
    """Get connection parameters using Autoscaling SDK or legacy env vars."""
    w = WorkspaceClient()

    if LAKEBASE_PROJECT_ID and not PGHOST_OVERRIDE:
        # Autoscaling mode: discover endpoint and generate credential
        print(f"Autoscaling mode: project={LAKEBASE_PROJECT_ID}, branch={LAKEBASE_BRANCH_ID}")
        endpoints = list(w.postgres.list_endpoints(
            parent=f"projects/{LAKEBASE_PROJECT_ID}/branches/{LAKEBASE_BRANCH_ID}"
        ))
        if not endpoints:
            print("ERROR: No compute endpoints found")
            sys.exit(1)

        endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
        host = endpoint.status.hosts.host
        username = w.current_user.me().user_name

        cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
        password = cred.token
        print(f"Endpoint discovered: {host}")
    elif PGHOST_OVERRIDE:
        # Legacy/provisioned mode
        host = PGHOST_OVERRIDE
        username = PGUSER_OVERRIDE or w.current_user.me().user_name
        password = os.getenv('PGPASSWORD', '')
        if not password:
            password = w.config.oauth_token().access_token
        print(f"Legacy mode: host={host}")
    else:
        print("ERROR: Set LAKEBASE_PROJECT_ID or PGHOST environment variable")
        sys.exit(1)

    return host, username, password


def setup_database():
    """Create schema, tables, audit log, and triggers"""
    import psycopg
    from psycopg.rows import dict_row

    host, username, password = get_connection_params()

    # Resolve IP for macOS DNS workaround
    hostaddr = _resolve_hostname(host)

    conn_params = (
        f"dbname={LAKEBASE_DATABASE} "
        f"user={username} "
        f"password={password} "
        f"host={host} "
        f"port={LAKEBASE_PORT} "
        f"sslmode=require"
    )
    if hostaddr:
        conn_params += f" hostaddr={hostaddr}"

    print(f"Connecting to {host}...")
    conn = psycopg.connect(conn_params)
    cursor = conn.cursor()

    # Create schema
    print("Creating schema...")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS ecommerce")
    conn.commit()

    # Create tables
    print("Creating tables...")

    # Users table
    cursor.execute("""
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
        )
    """)
    conn.commit()

    # Products table
    cursor.execute("""
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
        )
    """)
    conn.commit()

    # Orders table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ecommerce.orders (
            order_id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES ecommerce.users(user_id),
            order_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(50) DEFAULT 'pending',
            total_amount DECIMAL(10, 2),
            shipping_address JSONB,
            payment_method VARCHAR(50)
        )
    """)
    conn.commit()

    # Order items table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ecommerce.order_items (
            order_item_id SERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL REFERENCES ecommerce.orders(order_id) ON DELETE CASCADE,
            product_id INTEGER NOT NULL REFERENCES ecommerce.products(product_id),
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            unit_price DECIMAL(10, 2) NOT NULL,
            subtotal DECIMAL(10, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED
        )
    """)
    conn.commit()

    # Audit log table
    print("Creating audit log table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ecommerce.audit_log (
            audit_id SERIAL PRIMARY KEY,
            table_name VARCHAR(50) NOT NULL,
            operation VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
            record_id INTEGER,
            old_data JSONB,
            new_data JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR(100) DEFAULT CURRENT_USER
        )
    """)
    conn.commit()

    # Create indexes
    print("Creating indexes...")
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_users_email ON ecommerce.users(email)",
        "CREATE INDEX IF NOT EXISTS idx_products_category ON ecommerce.products(category)",
        "CREATE INDEX IF NOT EXISTS idx_orders_user_id ON ecommerce.orders(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_orders_status ON ecommerce.orders(status)",
        "CREATE INDEX IF NOT EXISTS idx_audit_log_table ON ecommerce.audit_log(table_name)",
        "CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON ecommerce.audit_log(created_at DESC)",
    ]
    for idx in indexes:
        try:
            cursor.execute(idx)
            conn.commit()
        except Exception as e:
            conn.rollback()

    # Create trigger functions
    print("Creating trigger functions...")

    # Products audit trigger
    cursor.execute("""
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
        $$ LANGUAGE plpgsql
    """)
    conn.commit()

    # Users audit trigger
    cursor.execute("""
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
        $$ LANGUAGE plpgsql
    """)
    conn.commit()

    # Orders audit trigger
    cursor.execute("""
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
        $$ LANGUAGE plpgsql
    """)
    conn.commit()

    # Order items audit trigger
    cursor.execute("""
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
        $$ LANGUAGE plpgsql
    """)
    conn.commit()

    # Create triggers
    print("Creating triggers...")
    triggers = [
        "DROP TRIGGER IF EXISTS trg_audit_products ON ecommerce.products",
        "DROP TRIGGER IF EXISTS trg_audit_users ON ecommerce.users",
        "DROP TRIGGER IF EXISTS trg_audit_orders ON ecommerce.orders",
        "DROP TRIGGER IF EXISTS trg_audit_order_items ON ecommerce.order_items",
        "CREATE TRIGGER trg_audit_products AFTER INSERT OR UPDATE OR DELETE ON ecommerce.products FOR EACH ROW EXECUTE FUNCTION ecommerce.audit_products()",
        "CREATE TRIGGER trg_audit_users AFTER INSERT OR UPDATE OR DELETE ON ecommerce.users FOR EACH ROW EXECUTE FUNCTION ecommerce.audit_users()",
        "CREATE TRIGGER trg_audit_orders AFTER INSERT OR UPDATE OR DELETE ON ecommerce.orders FOR EACH ROW EXECUTE FUNCTION ecommerce.audit_orders()",
        "CREATE TRIGGER trg_audit_order_items AFTER INSERT OR UPDATE OR DELETE ON ecommerce.order_items FOR EACH ROW EXECUTE FUNCTION ecommerce.audit_order_items()",
    ]
    for trig in triggers:
        try:
            cursor.execute(trig)
            conn.commit()
        except Exception as e:
            print(f"   Warning: {str(e)[:80]}")
            conn.rollback()

    # Insert sample data
    print("Inserting sample data...")

    # Users
    cursor.execute("""
        INSERT INTO ecommerce.users (email, username, full_name, metadata) VALUES
        ('john.doe@example.com', 'johndoe', 'John Doe', '{"role": "customer", "tier": "gold"}'),
        ('jane.smith@example.com', 'janesmith', 'Jane Smith', '{"role": "customer", "tier": "silver"}'),
        ('admin@company.com', 'admin', 'Admin User', '{"role": "admin", "permissions": ["all"]}'),
        ('bob.wilson@example.com', 'bobwilson', 'Bob Wilson', '{"role": "customer", "tier": "bronze"}'),
        ('alice.johnson@example.com', 'alicejohnson', 'Alice Johnson', '{"role": "vendor", "store": "TechGadgets"}')
        ON CONFLICT (email) DO NOTHING
    """)
    conn.commit()

    # Products
    cursor.execute("""
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
        ON CONFLICT DO NOTHING
    """)
    conn.commit()

    # Orders with items
    cursor.execute("SELECT user_id FROM ecommerce.users LIMIT 1")
    user = cursor.fetchone()
    if user:
        user_id = user[0]

        cursor.execute("""
            INSERT INTO ecommerce.orders (user_id, status, total_amount, shipping_address, payment_method)
            VALUES (%s, 'completed', 2149.97, '{"street": "123 Main St", "city": "Denver", "state": "CO", "zip": "80202"}', 'credit_card')
            RETURNING order_id
        """, (user_id,))
        order = cursor.fetchone()
        conn.commit()

        if order:
            order_id = order[0]
            cursor.execute("""
                INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price) VALUES
                (%s, 1, 1, 1999.99),
                (%s, 2, 3, 49.99)
            """, (order_id, order_id))
            conn.commit()

        # Another order
        cursor.execute("""
            INSERT INTO ecommerce.orders (user_id, status, total_amount, shipping_address, payment_method)
            VALUES (%s, 'pending', 229.98, '{"street": "456 Oak Ave", "city": "Boulder", "state": "CO", "zip": "80301"}', 'paypal')
            RETURNING order_id
        """, (user_id,))
        order2 = cursor.fetchone()
        conn.commit()

        if order2:
            order_id2 = order2[0]
            cursor.execute("""
                INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price) VALUES
                (%s, 5, 1, 149.99),
                (%s, 3, 1, 79.99)
            """, (order_id2, order_id2))
            conn.commit()

    # Verify
    cursor.execute("""
        SELECT
            (SELECT COUNT(*) FROM ecommerce.users) as users,
            (SELECT COUNT(*) FROM ecommerce.products) as products,
            (SELECT COUNT(*) FROM ecommerce.orders) as orders,
            (SELECT COUNT(*) FROM ecommerce.audit_log) as audit_entries
    """)
    result = cursor.fetchone()

    print(f"\n✅ Database setup completed!")
    print(f"   - Users: {result[0]}")
    print(f"   - Products: {result[1]}")
    print(f"   - Orders: {result[2]}")
    print(f"   - Audit Log Entries: {result[3]}")

    cursor.close()
    conn.close()
    return True

if __name__ == '__main__':
    try:
        success = setup_database()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
