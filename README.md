# Databricks Lakebase Autoscaling End-to-End Training Application

A comprehensive training application demonstrating **Databricks Lakebase Autoscaling** features including project/branch/endpoint hierarchy, autoscaling compute (0.5-112 CU), scale-to-zero, OAuth credential generation via `w.postgres` SDK, audit trails, bulk imports, and multi-table transactions.

**Live Demo:** [https://lakebase-end-to-end-training-1602460480284688.aws.databricksapps.com](https://lakebase-end-to-end-training-1602460480284688.aws.databricksapps.com)

---

## What's New in v3.0 (Lakebase Autoscaling)

This version migrates from **Lakebase Provisioned** to **Lakebase Autoscaling**:

| Feature | Provisioned (v2.0) | Autoscaling (v3.0) |
|---------|--------------------|--------------------|
| **SDK** | `w.database` | `w.postgres` |
| **Top Resource** | Instance | Project > Branches > Endpoints |
| **Hostname** | `instance-UUID.database.cloud.databricks.com` | `ep-XXXXX.cloud.databricks.com` |
| **Capacity** | Fixed CU_1/CU_2/CU_4/CU_8 (16 GB/CU) | 0.5-112 CU autoscaling (2 GB/CU) |
| **Scale-to-Zero** | Not supported | Configurable per endpoint |
| **Branching** | Not supported | Full branching (production, dev, staging) |
| **Token Generation** | `w.config.oauth_token()` | `w.postgres.generate_database_credential()` |
| **Operations** | Synchronous | Long-running with `.wait()` |

### Additional Features
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
4. [Lakebase Autoscaling Concepts](#lakebase-autoscaling-concepts)
5. [Application Features](#application-features)
6. [How to Verify Inserts in Lakebase](#how-to-verify-inserts-in-lakebase)
7. [Audit Trail Deep Dive](#audit-trail-deep-dive)
8. [Key Lakebase Features Demonstrated](#key-lakebase-features-demonstrated)
9. [Deployment](#deployment)
10. [Troubleshooting](#troubleshooting)
11. [Training Documentation](#training-documentation)

---

## Quick Start

```bash
# Clone the repository
git clone https://github.com/suryasai87/lakebase_end_to_end_training.git
cd lakebase_end_to_end_training

# Install dependencies
pip install -r requirements.txt

# Step 1: Create a Lakebase Autoscaling project (one-time setup)
python setup_lakebase_project.py --project-id training-app --create

# Step 2: Set environment variables
export LAKEBASE_PROJECT_ID="training-app"
export LAKEBASE_BRANCH_ID="production"

# Step 3: Run database table setup
python setup_lakebase_tables.py

# Step 4: Run the application
python dash_app.py
```

---

## Prerequisites

Before starting, ensure you have:

1. **Databricks Workspace** with Lakebase Autoscaling enabled
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

### Step 1: Create a Lakebase Autoscaling Project

Lakebase Autoscaling uses a **Project > Branch > Endpoint** hierarchy:

```
Project (training-app)
├── Branch: production (auto-created)
│   └── Compute Endpoint (auto-created, autoscaling)
└── Branch: development (optional)
    └── Compute Endpoint (independent scaling)
```

**Option A: Using the setup script (recommended)**

```bash
# Create project with default settings
python setup_lakebase_project.py --project-id training-app --create

# View connection info
python setup_lakebase_project.py --project-id training-app --info

# Optionally create a development branch
python setup_lakebase_project.py --project-id training-app --create-dev-branch

# Optionally resize compute (default: 0.5-4.0 CU)
python setup_lakebase_project.py --project-id training-app --resize --min-cu 1.0 --max-cu 4.0
```

**Option B: Using the Databricks UI**

1. Navigate to your Databricks workspace
2. Go to **SQL** > **OLTP Databases**
3. Click **Create Database** > **Autoscaling**
4. Configure:
   - Name: `training-app`
   - PostgreSQL version: 17

**Option C: Using the Databricks Python SDK**

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import Project, ProjectSpec

w = WorkspaceClient()

# Create project (long-running operation)
operation = w.postgres.create_project(
    project=Project(spec=ProjectSpec(
        display_name="Lakebase Training: training-app",
        pg_version="17",
    )),
    project_id="training-app",
)
result = operation.wait()
print(f"Project created: {result.name}")
```

### Step 2: Generate Credentials

Lakebase Autoscaling uses `w.postgres.generate_database_credential()` for token generation:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# List endpoints for your project
endpoints = list(w.postgres.list_endpoints(
    parent="projects/training-app/branches/production"
))

# Generate credential (valid for 1 hour)
cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
print(f"Token: {cred.token}")

# Get endpoint host
endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
print(f"Host: {endpoint.status.hosts.host}")
print(f"Username: {w.current_user.me().user_name}")
```

> **Note:** The app handles credential generation automatically. You only need this for manual database access (psql, DBeaver, etc.).

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

**For Autoscaling (recommended):**

```bash
export LAKEBASE_PROJECT_ID="training-app"
export LAKEBASE_BRANCH_ID="production"
```

The app discovers the endpoint host and generates credentials automatically using the `w.postgres` SDK.

**For legacy/provisioned mode (backward compatible):**

```bash
export PGHOST="instance-<instance-id>.database.cloud.databricks.com"
export PGUSER="<service-principal-id>"
export PGDATABASE="databricks_postgres"
export PGPORT="5432"
export PGSSLMODE="require"
```

### Step 5: Run Database Setup

```bash
# Using the Python setup script (recommended - handles auth automatically)
python setup_lakebase_tables.py

# Or using psql with manual token
python setup_lakebase_project.py --project-id training-app --info
# Copy the connection string from the output, then:
psql "<connection-string>" -f setup_database.sql
```

This creates:
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

## Lakebase Autoscaling Concepts

### Resource Hierarchy

```
Workspace
└── Project (e.g., "training-app")
    ├── Branch: production (auto-created with project)
    │   ├── Compute Endpoint (autoscaling, 0.5-112 CU)
    │   ├── Roles & Permissions
    │   └── Databases (databricks_postgres)
    ├── Branch: development (copy-on-write from production)
    │   └── Compute Endpoint (independent scaling)
    └── Branch: staging
        └── Compute Endpoint (independent scaling)
```

### Autoscaling Compute

- **Range:** 0.5 to 112 Compute Units (CU)
- **Memory:** 2 GB per CU (e.g., 4 CU = 8 GB RAM)
- **Constraint:** `max_cu - min_cu` cannot exceed 8 CU
- **Scale-to-Zero:** Endpoint suspends after idle period, wakes in 200-500ms

### Branching

Branches provide copy-on-write database clones:
- **Production:** Main branch, auto-created with project
- **Development:** Create from production for testing
- **Staging:** Pre-production validation
- Each branch has independent compute endpoints

### Scale-to-Zero

- Endpoints automatically suspend when idle
- First connection after suspension takes 200-500ms to wake
- Session context (temp tables, prepared statements) is lost after suspension
- The app includes retry logic for cold starts

---

## Application Features

### 1. Dashboard
- Real-time metrics (users, products, orders, revenue)
- Product inventory bar chart
- Revenue trend line chart
- **Real-Time Database Activity panel**
- Connection status showing Autoscaling/Provisioned mode

### 2. Data Entry
- Add products with categories, tags, and descriptions
- Add users with role metadata
- Form validation

### 3. Create Order
- Select customer from dropdown
- Enter shipping address (stored as JSONB)
- Multi-table transaction handling
- Demonstrates `RETURNING` clause

### 4. Bulk Import
- Upload CSV files
- Preview data before import
- Download template
- Batch insert with error handling

### 5. Audit Trail
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
# Get connection info
python setup_lakebase_project.py --project-id training-app --info

# Connect with psql (use the connection string from --info output)
psql "host=<endpoint-host> dbname=databricks_postgres user=<username> password=<token> sslmode=require"
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

---

## Key Lakebase Autoscaling Features Demonstrated

### 1. Autoscaling Endpoint Discovery
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Discover endpoint from project/branch
endpoints = list(w.postgres.list_endpoints(
    parent=f"projects/{project_id}/branches/{branch_id}"
))
endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
host = endpoint.status.hosts.host
```

### 2. Credential Generation (replaces `oauth_token()`)
```python
# Generate database credential for the autoscaling endpoint
cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
password = cred.token  # Valid for 1 hour
username = w.current_user.me().user_name
```

### 3. Connection with Scale-to-Zero Retry
```python
import psycopg

def get_db_connection(retries=3):
    for attempt in range(retries):
        try:
            return psycopg.connect(
                host=host, dbname="databricks_postgres",
                user=username, password=token,
                sslmode="require", connect_timeout=10,
            )
        except Exception as e:
            if attempt < retries - 1:
                time.sleep((attempt + 1) * 2)  # Exponential backoff
            else:
                raise
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
┌──────────────────────────────────────────────────────────────────────────┐
│                       Databricks Apps Platform                           │
├──────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐   ┌─────────────────────┐   ┌──────────────────┐  │
│  │   Dash App      │   │  Credential Manager │   │  Lakebase        │  │
│  │  (dash_app.py)  │──▶│  (w.postgres SDK)   │──▶│  Autoscaling     │  │
│  │                 │   │                     │   │                  │  │
│  │  - Dashboard    │   │  - Endpoint         │   │  Project:        │  │
│  │  - Data Entry   │   │    discovery        │   │   training-app   │  │
│  │  - Create Order │   │  - generate_        │   │  Branch:         │  │
│  │  - Bulk Import  │   │    database_        │   │   production     │  │
│  │  - Audit Trail  │   │    credential()     │   │  Endpoint:       │  │
│  │  - Query Builder│   │  - Token refresh    │   │   0.5-4 CU       │  │
│  │                 │   │    (50 min cycle)   │   │  - ecommerce     │  │
│  │                 │   │  - Scale-to-zero    │   │    schema        │  │
│  │                 │   │    retry logic      │   │  - audit_log     │  │
│  └─────────────────┘   └─────────────────────┘   └──────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
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
databricks apps logs lakebase-training-app
```

### Required Files

| File | Purpose |
|------|---------|
| `app.yaml` | Databricks App config (env vars for project/branch) |
| `databricks.yml` | DAB bundle configuration |
| `dash_app.py` | Main application with autoscaling connection logic |
| `setup_lakebase_project.py` | Create/manage Lakebase Autoscaling projects |
| `setup_lakebase_tables.py` | Create database tables, triggers, sample data |
| `setup_database.sql` | Raw SQL for database schema with triggers |
| `requirements.txt` | Python dependencies |

### app.yaml Configuration

```yaml
command:
  - python
  - dash_app.py

env:
  - name: LAKEBASE_PROJECT_ID
    value: "training-app"
  - name: LAKEBASE_BRANCH_ID
    value: "production"
  - name: PGDATABASE
    value: "databricks_postgres"
```

> **Note:** No hardcoded hostname or credentials. The app discovers the endpoint and generates tokens at runtime using the Databricks SDK.

---

## File Structure

```
lakebase_end_to_end_training/
├── dash_app.py                      # Main application (autoscaling-aware)
├── setup_lakebase_project.py        # Create/manage Lakebase projects (NEW)
├── setup_lakebase_tables.py         # Database table setup (autoscaling-aware)
├── setup_database.sql               # Schema with audit trail and triggers
├── requirements.txt                 # Python dependencies
├── app.yaml                         # Databricks App config
├── databricks.yml                   # DAB bundle config
├── README.md                        # This file
├── SETUP_GUIDE.md                   # Detailed setup guide
└── lakebase_training_document/      # Training materials
    └── Databricks_Lakebase_Complete_Training_Guide.md
```

---

## Troubleshooting

### Common Issues

**1. LAKEBASE_PROJECT_ID not set**
```
Error: LAKEBASE_PROJECT_ID must be set
```
**Solution:** Set the environment variable:
```bash
export LAKEBASE_PROJECT_ID="training-app"
```

**2. No compute endpoints found**
```
Error: No compute endpoints found for project=training-app, branch=production
```
**Solution:** Verify the project exists and has a running endpoint:
```bash
python setup_lakebase_project.py --list
```

**3. Scale-to-zero cold start timeout**
```
Error: connection timeout / connection refused
```
**Solution:** The app includes retry logic with exponential backoff. If the endpoint is suspended, the first connection may take 200-500ms. Increase retries if needed.

**4. Compute endpoint suspended**
```
Error: endpoint is suspended
```
**Solution:** The endpoint will auto-wake on the next connection attempt. The app retries automatically.

**5. OAuth token identity mismatch**
```
Error: OAuth token identity 'xxxx-xxxx' did not match the security label
```
**Solution:** When using autoscaling, the credential is generated via `w.postgres.generate_database_credential()` which handles identity correctly. If using legacy mode, ensure PGUSER matches the token identity.

**6. Permission denied on table**
```
Error: permission denied for table users
```
**Solution:** Grant permissions to the service principal:
```sql
GRANT USAGE ON SCHEMA ecommerce TO "<service-principal-id>";
GRANT ALL ON ALL TABLES IN SCHEMA ecommerce TO "<service-principal-id>";
```

**7. JSONB column display error in DataTable**
```
Error: Expected string, number, boolean - got dict/list
```
**Solution:** The app converts JSONB columns to strings before displaying in Dash DataTable.

**8. macOS DNS resolution failure**
```
Error: could not translate host name to address
```
**Solution:** The app includes a macOS DNS workaround using `dig` and `hostaddr`. This is handled automatically.

**9. OAuth token expired**
```
Error: password authentication failed
```
**Solution:** Tokens expire after 1 hour. The app refreshes tokens every 50 minutes automatically.

**10. 502 Bad Gateway**
```
Error: 502 Bad Gateway
```
**Solutions:**
- Check app.yaml command is correct
- Verify port matches DATABRICKS_APP_PORT (default: 8080)
- Check app logs: `databricks apps logs lakebase-training-app`

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

**Version:** 3.0
**Last Updated:** February 2026
**Target Audience:** Intermediate to Advanced Developers
**Duration:** 8 hours (1 day)

---

*Built with Databricks Lakebase Autoscaling and Dash*
