# Databricks Lakebase End-to-End Training Application

A comprehensive training application demonstrating **advanced Databricks Lakebase (PostgreSQL)** features including audit trails, bulk imports, real-time insert verification, and multi-table transactions.

**Live Demo:** [https://lakebase-training-app-1602460480284688.aws.databricksapps.com](https://lakebase-training-app-1602460480284688.aws.databricksapps.com)

---

## What's New in This Version

This repository extends the [basic lakebase_training](https://github.com/suryasai87/lakebase_training) with advanced features:

| Feature | Description |
|---------|-------------|
| **Create Order** | Multi-table transactions with foreign key relationships |
| **Real-Time Insert Panel** | Live dashboard showing recent database activity |
| **Audit Trail** | PostgreSQL triggers tracking all INSERT/UPDATE/DELETE operations |
| **Bulk CSV Import** | Upload CSV files to batch insert products |
| **Enhanced Query Builder** | Additional sample queries for audit analysis |

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Prerequisites](#prerequisites)
3. [Step-by-Step Setup Guide](#step-by-step-setup-guide)
4. [Application Features](#application-features)
5. [How to Verify Inserts in Lakebase](#how-to-verify-inserts-in-lakebase)
6. [Audit Trail Deep Dive](#audit-trail-deep-dive)
7. [Key Lakebase Features Demonstrated](#key-lakebase-features-demonstrated)
8. [Deployment](#deployment)
9. [Troubleshooting](#troubleshooting)
10. [Training Documentation](#training-documentation)

---

## Quick Start

```bash
# Clone the repository
git clone https://github.com/suryasai87/lakebase_end_to_end_training.git
cd lakebase_end_to_end_training

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export PGHOST="your-lakebase-host"
export PGDATABASE="databricks_postgres"
export PGUSER="token"
export PGPORT="5432"
export PGSSLMODE="require"

# Run the database setup (creates tables, triggers, and sample data)
# Connect to your Lakebase instance and run:
psql -f setup_database.sql

# Run the application
python dash_app.py
```

---

## Prerequisites

Before starting, ensure you have:

1. **Databricks Workspace** with Lakebase enabled
2. **Python 3.9+** installed
3. **Databricks CLI** configured
4. **Git** for version control

### Install Databricks CLI

```bash
# macOS
brew install databricks/tap/databricks

# Or using pip
pip install databricks-cli

# Configure
databricks configure
```

---

## Step-by-Step Setup Guide

### Step 1: Create a Lakebase Instance

1. Navigate to your Databricks workspace
2. Go to **Compute** → **OLTP Databases**
3. Click **Create Database**
4. Configure:
   - Name: `training-lakebase`
   - Region: `us-east-2` (or your preferred region)
5. Note the connection details:
   - Host: `instance-XXXXX.database.cloud.databricks.com`
   - Port: `5432`
   - Database: `databricks_postgres`

### Step 2: Get OAuth Token

```python
from databricks import sdk

# Initialize workspace client
workspace_client = sdk.WorkspaceClient()

# Get OAuth token (valid for 1 hour)
token = workspace_client.config.oauth_token().access_token
print(f"Token: {token}")
```

### Step 3: Clone and Configure Repository

```bash
# Clone the repository
git clone https://github.com/suryasai87/lakebase_end_to_end_training.git
cd lakebase_end_to_end_training

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 4: Set Environment Variables

```bash
export PGHOST="instance-XXXXX.database.cloud.databricks.com"
export PGDATABASE="databricks_postgres"
export PGUSER="token"
export PGPASSWORD="<your-oauth-token>"
export PGPORT="5432"
export PGSSLMODE="require"
export PGAPPNAME="lakebase-training-app"
```

### Step 5: Run Database Setup

Connect to your Lakebase instance and run the setup script:

```bash
# Using psql
psql "postgresql://token:$PGPASSWORD@$PGHOST:5432/databricks_postgres?sslmode=require" \
  -f setup_database.sql
```

This script creates:
- `ecommerce` schema
- `users`, `products`, `orders`, `order_items` tables
- `audit_log` table
- Trigger functions for audit logging
- Triggers on all tables
- Sample data

### Step 6: Run the Application

```bash
# Run locally
python dash_app.py

# Access at http://localhost:8080
```

---

## Application Features

### 1. Dashboard
- Real-time metrics (users, products, orders, revenue)
- Product inventory bar chart
- Revenue trend line chart
- **Real-Time Database Activity panel** (NEW)

### 2. Data Entry
- Add products with categories, tags, and descriptions
- Add users with role metadata
- Form validation

### 3. Create Order (NEW)
- Select customer from dropdown
- Enter shipping address (stored as JSONB)
- Multi-table transaction handling
- Demonstrates `RETURNING` clause

### 4. Bulk Import (NEW)
- Upload CSV files
- Preview data before import
- Download template
- Batch insert with error handling

### 5. Audit Trail (NEW)
- Filter by table (products, users, orders)
- Filter by operation (INSERT, UPDATE, DELETE)
- Configurable refresh rate
- Full change history with old/new data

### 6. Query Builder
- Pre-built sample queries including:
  - Audit log summary
  - Recent inserts
  - Top selling products
  - Revenue by category
- Custom SQL execution
- CSV export

### 7. Vector Search (Demo)
- Semantic search interface
- Hybrid search options
- Demonstrates pg_vector capabilities

---

## How to Verify Inserts in Lakebase

### Method 1: Using the App's Real-Time Panel

The Dashboard includes a **Real-Time Database Activity** panel that shows the last 10 inserts automatically.

### Method 2: Using the Audit Trail Tab

1. Navigate to the **Audit Trail** tab
2. Filter by table or operation
3. See all changes with timestamps

### Method 3: Using the Query Builder

Run this query:

```sql
SELECT
    table_name,
    operation,
    record_id,
    new_data->>'name' as item_name,
    created_at
FROM ecommerce.audit_log
WHERE operation = 'INSERT'
ORDER BY created_at DESC
LIMIT 20;
```

### Method 4: Direct Database Query

```bash
psql "postgresql://token:$TOKEN@$PGHOST:5432/databricks_postgres?sslmode=require"
```

```sql
-- Check recent products
SELECT product_id, name, price, created_at
FROM ecommerce.products
ORDER BY created_at DESC
LIMIT 5;

-- Check audit log
SELECT * FROM ecommerce.audit_log
ORDER BY created_at DESC
LIMIT 10;
```

---

## Audit Trail Deep Dive

### How It Works

1. **Triggers** are attached to each table (products, users, orders, order_items)
2. When any INSERT, UPDATE, or DELETE occurs, the trigger fires
3. The trigger function captures the operation details and inserts them into `audit_log`
4. The app queries `audit_log` to display real-time activity

### Audit Log Schema

```sql
CREATE TABLE ecommerce.audit_log (
    audit_id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    operation VARCHAR(10) NOT NULL,  -- INSERT, UPDATE, DELETE
    record_id INTEGER,
    old_data JSONB,                   -- Previous state (UPDATE/DELETE)
    new_data JSONB,                   -- New state (INSERT/UPDATE)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT CURRENT_USER
);
```

### Trigger Example

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

CREATE TRIGGER trg_audit_products
    AFTER INSERT OR UPDATE OR DELETE ON ecommerce.products
    FOR EACH ROW EXECUTE FUNCTION ecommerce.audit_products();
```

### Querying the Audit Trail

```sql
-- Summary by table and operation
SELECT table_name, operation, COUNT(*) as count,
       MAX(created_at) as last_activity
FROM ecommerce.audit_log
GROUP BY table_name, operation
ORDER BY last_activity DESC;

-- Find all changes to a specific product
SELECT * FROM ecommerce.audit_log
WHERE table_name = 'products'
  AND record_id = 1
ORDER BY created_at DESC;

-- Compare old vs new data for updates
SELECT
    record_id,
    old_data->>'price' as old_price,
    new_data->>'price' as new_price,
    created_at
FROM ecommerce.audit_log
WHERE table_name = 'products'
  AND operation = 'UPDATE';
```

---

## Key Lakebase Features Demonstrated

### 1. PostgreSQL-Compatible Database Operations
```python
conn_string = (
    f"dbname={os.getenv('PGDATABASE')} "
    f"user={os.getenv('PGUSER')} "
    f"password={postgres_password} "
    f"host={os.getenv('PGHOST')} "
    f"sslmode=require"
)
```

### 2. OAuth Token Authentication
```python
from databricks import sdk
workspace_client = sdk.WorkspaceClient()
postgres_password = workspace_client.config.oauth_token().access_token
```
- Automatic token refresh every 15 minutes

### 3. Connection Pooling
```python
from psycopg_pool import ConnectionPool
connection_pool = ConnectionPool(conn_string, min_size=2, max_size=10)
```

### 4. Advanced Data Types
- **JSONB:** For metadata and shipping addresses
- **TEXT[]:** Array types for product tags
- **SERIAL:** Auto-incrementing primary keys
- **DECIMAL:** Precise financial calculations
- **GENERATED ALWAYS AS:** Computed columns

### 5. PostgreSQL Triggers
- Audit trail implementation
- Automatic change tracking
- Full history with old/new data

### 6. Transaction Management
```python
try:
    self.cursor.execute(query, params)
    self.connection.commit()
except Exception as e:
    self.connection.rollback()
    raise e
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Databricks Apps Platform                      │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────┐ │
│  │   Dash App      │    │   OAuth Token   │    │  Lakebase   │ │
│  │  (dash_app.py)  │───▶│   Management    │───▶│  PostgreSQL │ │
│  │                 │    │                 │    │             │ │
│  │  - Dashboard    │    │  - Auto-refresh │    │  - ecommerce│ │
│  │  - Data Entry   │    │  - SDK Client   │    │    schema   │ │
│  │  - Create Order │    │                 │    │  - audit_log│ │
│  │  - Bulk Import  │    │                 │    │  - triggers │ │
│  │  - Audit Trail  │    │                 │    │             │ │
│  │  - Query Builder│    │                 │    │             │ │
│  └─────────────────┘    └─────────────────┘    └─────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## Deployment

### Deploy to Databricks Apps

```bash
# Validate bundle
databricks bundle validate

# Deploy
databricks bundle deploy

# Run the app
databricks bundle run lakebase_training_dashboard

# Check status
databricks apps list

# View logs
databricks apps logs lakebase-end-to-end-training
```

### Required Files

| File | Purpose |
|------|---------|
| `app.yaml` | Databricks App entry point |
| `databricks.yml` | DAB bundle configuration |
| `dash_app.py` | Main application |
| `setup_database.sql` | Database schema with triggers |
| `requirements.txt` | Python dependencies |

---

## File Structure

```
lakebase_end_to_end_training/
├── dash_app.py                      # Main enhanced application
├── app.py                           # Streamlit version (alternative)
├── setup_database.sql               # Schema with audit trail and triggers
├── requirements.txt                 # Python dependencies
├── app.yaml                         # Databricks App config
├── databricks.yml                   # DAB bundle config
├── README.md                        # This file
└── lakebase_training_document/      # Training materials
    └── Databricks_Lakebase_Complete_Training_Guide.md
```

---

## Troubleshooting

### Common Issues

**1. Audit log table not found**
```
Error: relation "ecommerce.audit_log" does not exist
```
**Solution:** Run `setup_database.sql` to create all tables including audit_log.

**2. Triggers not working**
```
No audit records appearing
```
**Solution:** Verify triggers exist:
```sql
SELECT trigger_name, event_manipulation, action_statement
FROM information_schema.triggers
WHERE trigger_schema = 'ecommerce';
```

**3. OAuth token expired**
```
Error: password authentication failed
```
**Solution:** Tokens expire after 1 hour. Refresh using `workspace_client.config.oauth_token()`.

**4. Connection pool exhausted**
```
Error: connection pool is full
```
**Solution:** The app automatically recycles connections. Restart the app if this persists.

**5. 502 Bad Gateway**
```
Error: 502 Bad Gateway
```
**Solutions:**
- Check app.yaml command is correct
- Verify port matches DATABRICKS_APP_PORT
- Check app logs: `databricks apps logs <app-name>`

---

## Training Documentation

For comprehensive training materials, see the [`lakebase_training_document`](./lakebase_training_document/) directory:

- **[Complete Training Guide](./lakebase_training_document/Databricks_Lakebase_Complete_Training_Guide.md)** - Full 8-hour training curriculum with lab exercises

---

## Related Repositories

- **Basic Training:** [lakebase_training](https://github.com/suryasai87/lakebase_training) - Simpler version without advanced features

---

## Additional Resources

### Documentation
- [Databricks Lakebase Documentation](https://docs.databricks.com/en/lakebase/index.html)
- [Databricks Apps Documentation](https://docs.databricks.com/en/apps/index.html)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [psycopg3 Documentation](https://www.psycopg.org/psycopg3/docs/)
- [Dash Documentation](https://dash.plotly.com/)

### Related Technologies
- [pg_vector for Vector Search](https://github.com/pgvector/pgvector)
- [PostgREST for REST APIs](https://postgrest.org/)

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Push and create a Pull Request

---

## License

This training material is provided for educational purposes.

---

**Version:** 2.0
**Last Updated:** December 2024
**Target Audience:** Intermediate to Advanced Developers
**Duration:** 8 hours (1 day)

---

*Built with Databricks Lakebase and Dash*
