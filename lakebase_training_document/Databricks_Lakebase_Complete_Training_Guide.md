# Databricks Lakebase Complete Training Guide

## End-to-End Training with Advanced Features

**Duration:** 1 Day (8 hours)
**Level:** Intermediate to Advanced
**Prerequisites:** Basic SQL knowledge, familiarity with cloud databases

---

## Table of Contents

1. [Training Overview](#training-overview)
2. [Module 1: Introduction & Architecture](#module-1-introduction--architecture)
3. [Module 2: Setup & Configuration](#module-2-setup--configuration)
4. [Module 3: Database Schema Design](#module-3-database-schema-design)
5. [Module 4: Basic Operations - DDL & DML](#module-4-basic-operations---ddl--dml)
6. [Module 5: Building the Dashboard Application](#module-5-building-the-dashboard-application)
7. [Module 6: Advanced Features](#module-6-advanced-features)
   - [6.1 Create Order with Transactions](#61-create-order-with-transactions)
   - [6.2 Real-Time Insert Verification](#62-real-time-insert-verification)
   - [6.3 Audit Trail with PostgreSQL Triggers](#63-audit-trail-with-postgresql-triggers)
   - [6.4 Bulk CSV Import](#64-bulk-csv-import)
8. [Module 7: Deploying to Databricks Apps](#module-7-deploying-to-databricks-apps)
9. [Lab Exercises](#lab-exercises)
10. [Troubleshooting Guide](#troubleshooting-guide)
11. [Resources & References](#resources--references)

---

## Training Overview

This comprehensive training guide covers everything you need to know about building production-ready applications with Databricks Lakebase. You will learn:

- How to set up and configure Lakebase instances
- Database schema design with advanced PostgreSQL features
- Building interactive dashboards with real-time data
- Implementing audit trails using database triggers
- Bulk data import functionality
- Deploying applications to Databricks Apps

---

## Module 1: Introduction & Architecture

**Duration:** 9:00 AM - 10:00 AM (1 hour)

### 1.1 What is Databricks Lakebase?

Databricks Lakebase is a fully-managed serverless PostgreSQL database that emerged from Databricks' acquisition of Neon in 2024. It represents a new category of operational databases designed specifically for the AI era.

**Key Features:**
- **Serverless Architecture:** Separates compute from storage for elastic scaling
- **Copy-on-Write Branching:** Instant database clones without data duplication
- **Unity Catalog Integration:** Unified governance across lakehouse and operational data
- **Auto-scaling:** Scales from zero to thousands of concurrent connections
- **Built on PostgreSQL 17:** Full compatibility with PostgreSQL ecosystem

### 1.2 Architecture Overview

| Component | Features |
|-----------|----------|
| **Compute Layer** | Auto-scaling, Sub-second cold starts, Connection pooling (10K+) |
| **Storage Layer** | Bottomless storage, Multi-zone replication, Point-in-time recovery (35 days) |
| **Integration** | Unity Catalog, OAuth authentication, Databricks SDK |

### 1.3 Why Lakebase for AI Applications?

- Native vector support via pg_vector extension
- Seamless integration with Databricks ML workflows
- Real-time operational data for AI/ML models
- ACID compliance for transactional integrity

---

## Module 2: Setup & Configuration

**Duration:** 10:00 AM - 11:00 AM (1 hour)

### 2.1 Creating a Lakebase Instance

**Step 1: Access Databricks Console**
1. Navigate to your Databricks workspace
2. Go to **Compute** → **OLTP Databases** tab
3. Click **Create Database**

**Step 2: Configure Instance**
| Setting | Value |
|---------|-------|
| Name | `training-lakebase` |
| Region | `us-east-2` |
| Compute Size | Auto-scaling |

### 2.2 Connecting to Lakebase

**Using DBeaver:**
1. Create new PostgreSQL connection
2. Enter connection details:
   - Host: `instance-XXXXX.database.cloud.databricks.com`
   - Port: `5432`
   - Database: `databricks_postgres`
   - Username: `token`
   - Password: `<your-oauth-token>`
   - SSL Mode: `require`

**Using psql:**
```bash
export PGHOST="instance-XXXXX.database.cloud.databricks.com"
export PGDATABASE="databricks_postgres"
export PGUSER="token"
export PGPASSWORD="<your-oauth-token>"
export PGSSLMODE="require"

psql
```

### 2.3 OAuth Token Authentication

Lakebase uses OAuth tokens for authentication. Tokens expire after 1 hour.

```python
from databricks import sdk

# Initialize workspace client
workspace_client = sdk.WorkspaceClient()

# Get OAuth token
token = workspace_client.config.oauth_token().access_token

# Use as password
PGPASSWORD = token
```

---

## Module 3: Database Schema Design

**Duration:** 11:00 AM - 12:00 PM (1 hour)

### 3.1 E-commerce Schema Overview

Our training application uses an e-commerce schema with the following tables:

```
ecommerce
├── users          (customer information)
├── products       (product catalog)
├── orders         (order headers)
├── order_items    (order line items)
└── audit_log      (change tracking)
```

### 3.2 Creating the Schema

```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS ecommerce;
```

### 3.3 Users Table

```sql
CREATE TABLE IF NOT EXISTS ecommerce.users (
    user_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(50) UNIQUE NOT NULL,
    full_name VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    metadata JSONB,              -- Flexible user attributes
    preferences JSONB DEFAULT '{}'::jsonb
);
```

**Key Features Demonstrated:**
- `SERIAL` for auto-incrementing primary keys
- `UNIQUE` constraints for email and username
- `TIMESTAMP WITH TIME ZONE` for timezone-aware dates
- `JSONB` for flexible semi-structured data

### 3.4 Products Table

```sql
CREATE TABLE IF NOT EXISTS ecommerce.products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    category VARCHAR(100),
    tags TEXT[],                 -- Array of tags
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

**Key Features Demonstrated:**
- `DECIMAL` for precise financial calculations
- `CHECK` constraints for data validation
- `TEXT[]` array type for tags

### 3.5 Orders and Order Items Tables

```sql
-- Orders table
CREATE TABLE IF NOT EXISTS ecommerce.orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES ecommerce.users(user_id),
    order_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(10, 2),
    shipping_address JSONB,      -- Structured address data
    payment_method VARCHAR(50)
);

-- Order items with computed column
CREATE TABLE IF NOT EXISTS ecommerce.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES ecommerce.orders(order_id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES ecommerce.products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL,
    subtotal DECIMAL(10, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);
```

**Key Features Demonstrated:**
- Foreign key relationships with `REFERENCES`
- `ON DELETE CASCADE` for referential integrity
- `GENERATED ALWAYS AS ... STORED` for computed columns

### 3.6 Creating Indexes

```sql
-- B-tree indexes for lookups
CREATE INDEX idx_users_email ON ecommerce.users(email);
CREATE INDEX idx_products_category ON ecommerce.products(category);
CREATE INDEX idx_orders_user_id ON ecommerce.orders(user_id);
CREATE INDEX idx_orders_status ON ecommerce.orders(status);

-- GIN indexes for JSONB and array queries
CREATE INDEX idx_users_metadata ON ecommerce.users USING GIN(metadata);
CREATE INDEX idx_products_tags ON ecommerce.products USING GIN(tags);
```

---

## Module 4: Basic Operations - DDL & DML

**Duration:** 12:00 PM - 1:00 PM (1 hour)

### 4.1 Insert Operations

**Single Insert:**
```sql
INSERT INTO ecommerce.users (email, username, full_name, metadata)
VALUES ('john.doe@example.com', 'johndoe', 'John Doe',
        '{"role": "customer", "tier": "gold"}');
```

**Insert with Returning:**
```sql
INSERT INTO ecommerce.orders (user_id, status, payment_method)
VALUES (1, 'pending', 'credit_card')
RETURNING order_id;
```

**Bulk Insert:**
```sql
INSERT INTO ecommerce.products (name, price, category, tags)
VALUES
    ('Product A', 29.99, 'Electronics', ARRAY['new', 'featured']),
    ('Product B', 49.99, 'Accessories', ARRAY['sale']),
    ('Product C', 19.99, 'Books', ARRAY['bestseller']);
```

### 4.2 Query Operations

**JSONB Queries:**
```sql
-- Query users by role in metadata
SELECT * FROM ecommerce.users
WHERE metadata->>'role' = 'customer';

-- Query users with specific tier
SELECT * FROM ecommerce.users
WHERE metadata @> '{"tier": "gold"}';
```

**Array Queries:**
```sql
-- Find products with specific tag
SELECT * FROM ecommerce.products
WHERE 'ai' = ANY(tags);

-- Find products with multiple tags
SELECT * FROM ecommerce.products
WHERE tags && ARRAY['laptop', 'professional'];
```

**Aggregation with JOINs:**
```sql
SELECT
    u.username,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as lifetime_value
FROM ecommerce.users u
LEFT JOIN ecommerce.orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.username
ORDER BY lifetime_value DESC;
```

### 4.3 Update and Delete Operations

**Update with JSONB:**
```sql
-- Update metadata
UPDATE ecommerce.users
SET metadata = metadata || '{"last_login": "2024-12-02"}'::jsonb
WHERE user_id = 1;
```

**Delete with Cascade:**
```sql
-- Deleting order will cascade to order_items
DELETE FROM ecommerce.orders
WHERE order_id = 1;
```

---

## Module 5: Building the Dashboard Application

**Duration:** 2:00 PM - 3:30 PM (1.5 hours)

### 5.1 Application Architecture

The training application is built with:
- **Backend:** Python with psycopg3 for database connectivity
- **Frontend:** Dash framework with Bootstrap components
- **Visualization:** Plotly for interactive charts
- **Authentication:** Databricks OAuth token management

### 5.2 Database Connection Manager

```python
from databricks import sdk
from psycopg_pool import ConnectionPool
import os
import time

# Initialize workspace client
workspace_client = sdk.WorkspaceClient()
postgres_password = None
last_password_refresh = 0
connection_pool = None

def refresh_oauth_token():
    """Refresh OAuth token if expired (every 15 minutes)."""
    global postgres_password, last_password_refresh
    if postgres_password is None or time.time() - last_password_refresh > 900:
        postgres_password = workspace_client.config.oauth_token().access_token
        last_password_refresh = time.time()

def get_connection_pool():
    """Get or create the connection pool."""
    global connection_pool
    if connection_pool is None:
        refresh_oauth_token()
        conn_string = (
            f"dbname={os.getenv('PGDATABASE')} "
            f"user={os.getenv('PGUSER')} "
            f"password={postgres_password} "
            f"host={os.getenv('PGHOST')} "
            f"port={os.getenv('PGPORT')} "
            f"sslmode=require"
        )
        connection_pool = ConnectionPool(conn_string, min_size=2, max_size=10)
    return connection_pool
```

### 5.3 LakebaseConnection Context Manager

```python
class LakebaseConnection:
    """Manage Lakebase database connections with OAuth token refresh"""

    def __init__(self):
        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """Establish connection to Lakebase"""
        self.connection = get_connection_pool().connection()
        self.cursor = self.connection.cursor(row_factory=dict_row)
        return True

    def execute_query(self, query, params=None):
        """Execute a query and return results"""
        try:
            self.cursor.execute(query, params)
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            elif 'RETURNING' in query.upper():
                result = self.cursor.fetchall()
                self.connection.commit()
                return result
            else:
                self.connection.commit()
                return self.cursor.rowcount
        except Exception as e:
            self.connection.rollback()
            raise e

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
```

### 5.4 Dashboard Metrics

```python
@app.callback(
    [Output("metric-users", "children"),
     Output("metric-products", "children"),
     Output("metric-orders", "children"),
     Output("metric-revenue", "children")],
    Input("interval-component", "n_intervals")
)
def update_metrics(n):
    try:
        with LakebaseConnection() as db:
            users = db.execute_query(
                "SELECT COUNT(*) as count FROM ecommerce.users"
            )[0]['count']
            products = db.execute_query(
                "SELECT COUNT(*) as count FROM ecommerce.products"
            )[0]['count']
            orders = db.execute_query(
                "SELECT COUNT(*) as count FROM ecommerce.orders"
            )[0]['count']
            revenue = db.execute_query(
                "SELECT COALESCE(SUM(total_amount), 0) as revenue "
                "FROM ecommerce.orders WHERE status='completed'"
            )[0]['revenue']

            return (f"{users:,}", f"{products:,}",
                    f"{orders:,}", f"${float(revenue):,.2f}")
    except Exception as e:
        return "Error", "Error", "Error", "Error"
```

---

## Module 6: Advanced Features

**Duration:** 3:30 PM - 5:30 PM (2 hours)

### 6.1 Create Order with Transactions

The Create Order feature demonstrates multi-table transactions with foreign key relationships.

**Database Operations:**

```sql
-- Step 1: Insert order header
INSERT INTO ecommerce.orders (user_id, status, shipping_address, payment_method)
VALUES (1, 'pending', '{"street": "123 Main St", "city": "Denver"}', 'credit_card')
RETURNING order_id;

-- Step 2: Insert order items
INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price)
VALUES (1, 1, 2, 49.99);

-- Step 3: Update order total
UPDATE ecommerce.orders
SET total_amount = (
    SELECT SUM(subtotal)
    FROM ecommerce.order_items
    WHERE order_id = 1
)
WHERE order_id = 1;
```

**Python Implementation:**

```python
def create_order(customer_id, payment_method, shipping_address, items):
    """Create order with items - demonstrates transaction handling"""
    with LakebaseConnection() as db:
        try:
            # Insert order
            result = db.execute_query("""
                INSERT INTO ecommerce.orders
                (user_id, status, shipping_address, payment_method)
                VALUES (%s, 'pending', %s::jsonb, %s)
                RETURNING order_id
            """, (customer_id, json.dumps(shipping_address), payment_method))
            order_id = result[0]['order_id']

            # Insert order items
            for item in items:
                db.execute_query("""
                    INSERT INTO ecommerce.order_items
                    (order_id, product_id, quantity, unit_price)
                    VALUES (%s, %s, %s, %s)
                """, (order_id, item['product_id'],
                      item['quantity'], item['price']))

            # Update order total using computed subtotals
            db.execute_query("""
                UPDATE ecommerce.orders
                SET total_amount = (
                    SELECT SUM(subtotal)
                    FROM ecommerce.order_items
                    WHERE order_id = %s
                )
                WHERE order_id = %s
            """, (order_id, order_id))

            return order_id
        except Exception as e:
            # Transaction will rollback automatically
            raise e
```

### 6.2 Real-Time Insert Verification

The dashboard includes a real-time panel showing recent database activity.

**SQL Query:**

```sql
SELECT
    table_name,
    operation,
    record_id,
    new_data->>'name' as item_name,
    created_at
FROM ecommerce.audit_log
ORDER BY created_at DESC
LIMIT 10;
```

**Dashboard Callback:**

```python
@app.callback(
    Output("recent-inserts-table", "children"),
    Input("interval-component", "n_intervals")
)
def update_recent_inserts(n):
    with LakebaseConnection() as db:
        results = db.execute_query("""
            SELECT table_name, operation, record_id,
                   new_data->>'name' as item_name,
                   created_at
            FROM ecommerce.audit_log
            ORDER BY created_at DESC
            LIMIT 10
        """)
        # Format and display results...
```

### 6.3 Audit Trail with PostgreSQL Triggers

The audit trail automatically captures all INSERT, UPDATE, and DELETE operations.

**Audit Log Table:**

```sql
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
```

**Trigger Function:**

```sql
CREATE OR REPLACE FUNCTION ecommerce.audit_products()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, new_data)
        VALUES ('products', 'INSERT', NEW.product_id, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, old_data, new_data)
        VALUES ('products', 'UPDATE', NEW.product_id,
                row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO ecommerce.audit_log (table_name, operation, record_id, old_data)
        VALUES ('products', 'DELETE', OLD.product_id, row_to_json(OLD)::jsonb);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
```

**Create Trigger:**

```sql
CREATE TRIGGER trg_audit_products
    AFTER INSERT OR UPDATE OR DELETE ON ecommerce.products
    FOR EACH ROW EXECUTE FUNCTION ecommerce.audit_products();
```

### 6.4 Bulk CSV Import

The bulk import feature allows uploading CSV files to insert multiple products.

**CSV Template:**

```csv
name,description,price,stock_quantity,category
"Wireless Earbuds","Bluetooth 5.0 earbuds",79.99,100,Electronics
"USB-C Cable","Fast charging 6ft cable",12.99,500,Accessories
"Python Book","Learn Python in 30 days",34.99,75,Books
```

**Python Implementation:**

```python
@app.callback(
    Output("import-feedback", "children"),
    Input("execute-import", "n_clicks"),
    State("uploaded-data-store", "data"),
    prevent_initial_call=True
)
def execute_bulk_import(n_clicks, json_data):
    df = pd.read_json(json_data)
    inserted = 0
    errors = []

    with LakebaseConnection() as db:
        for idx, row in df.iterrows():
            try:
                db.execute_query("""
                    INSERT INTO ecommerce.products
                    (name, description, price, stock_quantity, category)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    row.get('name'),
                    row.get('description', ''),
                    float(row.get('price', 0)),
                    int(row.get('stock_quantity', 0)),
                    row.get('category', 'Other')
                ))
                inserted += 1
            except Exception as e:
                errors.append(f"Row {idx}: {str(e)}")

    return f"Successfully imported {inserted} products!"
```

---

## Module 7: Deploying to Databricks Apps

**Duration:** 5:30 PM - 6:00 PM (30 minutes)

### 7.1 Application Configuration

**app.yaml:**
```yaml
command:
  - python
  - dash_app.py
```

**databricks.yml:**
```yaml
bundle:
  name: lakebase-end-to-end-training

resources:
  apps:
    lakebase_training_dashboard:
      name: lakebase-end-to-end-training
      description: "End-to-End Lakebase Training Dashboard"
      source_code_path: .
```

### 7.2 Deployment Commands

```bash
# Validate the bundle
databricks bundle validate

# Deploy to workspace
databricks bundle deploy

# Run the app
databricks bundle run lakebase_training_dashboard

# Check app status
databricks apps list

# View logs
databricks apps logs lakebase-end-to-end-training
```

### 7.3 Environment Variables

The app expects these environment variables (automatically set by Databricks Apps):
- `PGHOST`: Lakebase instance hostname
- `PGDATABASE`: Database name (databricks_postgres)
- `PGUSER`: Username (token for OAuth)
- `PGPORT`: Port (5432)
- `PGSSLMODE`: SSL mode (require)
- `DATABRICKS_APP_PORT`: Application port

---

## Lab Exercises

### Lab 1: Add a New Product (15 minutes)
1. Navigate to the **Data Entry** tab
2. Fill in product details
3. Click **Add Product**
4. Verify the product appears in the Dashboard
5. Check the **Audit Trail** tab to see the INSERT record

### Lab 2: Create an Order (20 minutes)
1. Navigate to the **Create Order** tab
2. Select a customer
3. Enter shipping address
4. Submit the order
5. Query the audit log to see multiple INSERT records

### Lab 3: Bulk Import Products (15 minutes)
1. Download the CSV template
2. Add 5-10 new products to the CSV
3. Upload the file in the **Bulk Import** tab
4. Verify products were imported
5. Check the audit trail for all INSERT operations

### Lab 4: Query Builder Practice (20 minutes)
Run these queries and analyze results:

```sql
-- Find top customers by order value
SELECT u.username, SUM(o.total_amount) as total_spent
FROM ecommerce.users u
JOIN ecommerce.orders o ON u.user_id = o.user_id
WHERE o.status = 'completed'
GROUP BY u.username
ORDER BY total_spent DESC;

-- Products with low stock
SELECT name, stock_quantity, category
FROM ecommerce.products
WHERE stock_quantity < 50
ORDER BY stock_quantity;

-- Audit summary by table
SELECT table_name, operation, COUNT(*) as count
FROM ecommerce.audit_log
GROUP BY table_name, operation
ORDER BY count DESC;
```

### Lab 5: Explore Audit Trail (15 minutes)
1. Navigate to the **Audit Trail** tab
2. Filter by table (products, users, orders)
3. Filter by operation (INSERT, UPDATE, DELETE)
4. Observe the real-time updates

---

## Troubleshooting Guide

### Connection Issues

**Error: Connection refused**
```
could not connect to server: Connection refused
```
**Solution:** Verify PGHOST is correct and instance is running.

**Error: Authentication failed**
```
password authentication failed
```
**Solution:** OAuth tokens expire after 1 hour. Refresh the token.

### Database Errors

**Error: Relation does not exist**
```
relation "ecommerce.users" does not exist
```
**Solution:** Run `setup_database.sql` to create tables.

**Error: Audit log not found**
```
relation "ecommerce.audit_log" does not exist
```
**Solution:** The enhanced schema includes audit tables. Run the full setup script.

### Application Errors

**Error: 502 Bad Gateway**
**Solutions:**
1. Check app.yaml command is correct
2. Verify port matches DATABRICKS_APP_PORT
3. Check app logs: `databricks apps logs <app-name>`

---

## Resources & References

### Official Documentation
- [Databricks Lakebase Documentation](https://docs.databricks.com/en/lakebase/index.html)
- [Databricks Apps Documentation](https://docs.databricks.com/en/apps/index.html)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [psycopg3 Documentation](https://www.psycopg.org/psycopg3/docs/)
- [Dash Documentation](https://dash.plotly.com/)

### Training Repository
- **Basic Training:** https://github.com/suryasai87/lakebase_training
- **End-to-End Training:** https://github.com/suryasai87/lakebase_end_to_end_training

### Related Technologies
- [pg_vector for Vector Search](https://github.com/pgvector/pgvector)
- [PostgREST for REST APIs](https://postgrest.org/)

---

**Training Version:** 2.0
**Last Updated:** December 2024
**Target Audience:** Intermediate to Advanced Developers
**Duration:** 8 hours (1 day)

---

*Good luck with your Databricks Lakebase journey!*
