# Databricks Lakebase Autoscaling Training App - Setup Guide

## Your App Configuration

### Databricks Workspace
**URL**: https://fe-vm-hls-amer.cloud.databricks.com/

### File Locations

#### Option 1: Databricks Repos (Recommended - Synced with GitHub)
```
/Repos/suryasai.turaga@databricks.com/lakebase-training/
```

#### Option 2: Workspace Directory
```
/Workspace/Users/suryasai.turaga@databricks.com/lakebase-training/
```

---

## Lakebase Autoscaling Connection

The app uses **Lakebase Autoscaling** which discovers endpoints and generates credentials automatically.

**Configuration (app.yaml):**
- `LAKEBASE_PROJECT_ID`: Your project ID (e.g., `training-app`)
- `LAKEBASE_BRANCH_ID`: Branch to connect to (default: `production`)

> **Note:** No hardcoded hostnames or tokens. The app uses the Databricks SDK (`w.postgres`) to discover endpoints and generate short-lived credentials at runtime.

---

## What's Been Deployed

1. **dash_app.py** - Main Dash application with autoscaling connection logic
2. **setup_lakebase_project.py** - Script to create/manage Lakebase Autoscaling projects
3. **setup_lakebase_tables.py** - Script to create tables and sample data
4. **app.yaml** - Configured with LAKEBASE_PROJECT_ID and LAKEBASE_BRANCH_ID
5. **requirements.txt** - All Python dependencies
6. **setup_database.sql** - Complete database setup script
7. **databricks.yml** - Databricks bundle configuration

---

## Quick Start - Run the App

### Step 1: Create a Lakebase Autoscaling Project (5 minutes)

```bash
# Install dependencies
pip install -r requirements.txt

# Create the project (long-running operation, will wait for completion)
python setup_lakebase_project.py --project-id training-app --create

# View connection info
python setup_lakebase_project.py --project-id training-app --info
```

This creates:
- A Lakebase Autoscaling **project** named `training-app`
- A **production** branch (auto-created)
- A **compute endpoint** with autoscaling (auto-created)

### Step 2: Set Up the Database (5 minutes)

**Option A: Using the Python setup script (recommended)**

```bash
export LAKEBASE_PROJECT_ID="training-app"
export LAKEBASE_BRANCH_ID="production"

python setup_lakebase_tables.py
```

**Option B: Using a Databricks Python Notebook**

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Discover endpoint
endpoints = list(w.postgres.list_endpoints(
    parent="projects/training-app/branches/production"
))
endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
host = endpoint.status.hosts.host

# Generate credential
cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
username = w.current_user.me().user_name

# Connect and setup
import psycopg

conn = psycopg.connect(
    host=host,
    database='databricks_postgres',
    user=username,
    password=cred.token,
    port=5432,
    sslmode='require'
)

cursor = conn.cursor()

# Read and execute setup script
with open('setup_database.sql', 'r') as f:
    sql_script = f.read()

cursor.execute(sql_script)
conn.commit()

# Verify setup
cursor.execute("""
    SELECT
        (SELECT COUNT(*) FROM ecommerce.users) as users,
        (SELECT COUNT(*) FROM ecommerce.products) as products,
        (SELECT COUNT(*) FROM ecommerce.orders) as orders
""")
result = cursor.fetchone()

print(f"Database setup completed!")
print(f"   - Users: {result[0]}")
print(f"   - Products: {result[1]}")
print(f"   - Orders: {result[2]}")

cursor.close()
conn.close()
```

**Option C: Using SQL Editor**

1. Go to: https://fe-vm-hls-amer.cloud.databricks.com/sql/editor
2. Connect to your Lakebase Autoscaling endpoint
3. Copy and paste the contents of `setup_database.sql`
4. Click "Run"

### Step 3: Run the App (1 minute)

```bash
export LAKEBASE_PROJECT_ID="training-app"
export LAKEBASE_BRANCH_ID="production"

python dash_app.py
```

Access at http://localhost:8080

---

## What You'll See

### Dashboard Features
1. **Dashboard Tab** - Animated metric cards, charts, real-time activity
2. **Data Entry Tab** - Add products and users with form validation
3. **Create Order Tab** - Multi-table transactions with JSONB addresses
4. **Bulk Import Tab** - CSV upload with preview
5. **Audit Trail Tab** - Real-time change tracking via triggers
6. **Query Builder Tab** - Pre-built and custom SQL queries
7. **Vector Search Tab** - Semantic search demo (pg_vector)

### Connection Status

The app shows connection status at the top:
- **"Connected to Lakebase (Autoscaling): ep-XXXXX... / databricks_postgres | Project: training-app/production"** - Autoscaling mode
- **"Connected to Lakebase (Provisioned): instance-XXXXX... / databricks_postgres"** - Legacy mode

---

## Database Schema Created

### Tables
- **ecommerce.users** - User accounts with JSONB metadata
- **ecommerce.products** - Product catalog with tags and categories
- **ecommerce.orders** - Order management with JSONB addresses
- **ecommerce.order_items** - Order line items with computed subtotals
- **ecommerce.audit_log** - Change tracking via triggers

### Sample Data
- 5 users (including admin and vendor roles)
- 10 products across categories (Electronics, Accessories, Books)
- 2 sample orders with line items

---

## Configuration Details

### app.yaml
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
  - name: PGPORT
    value: "5432"
  - name: PGSSLMODE
    value: "require"
  - name: PGAPPNAME
    value: "lakebase-end-to-end-training"
```

---

## Deploy as Databricks App

```bash
# Validate
databricks bundle validate

# Deploy
databricks bundle deploy

# Run
databricks bundle run lakebase_training_dashboard

# Check status
databricks apps list

# View logs
databricks apps logs lakebase-training-app
```

---

## Quick Access Links

| Resource | Link |
|----------|------|
| **Databricks Workspace** | https://fe-vm-hls-amer.cloud.databricks.com/ |
| **Repos Location** | `/Repos/suryasai.turaga@databricks.com/lakebase-training` |
| **GitHub Repository** | https://github.com/suryasai87/lakebase_end_to_end_training |

---

## Troubleshooting

### Issue: LAKEBASE_PROJECT_ID not set
**Solution**: Set the environment variable: `export LAKEBASE_PROJECT_ID="training-app"`

### Issue: No compute endpoints found
**Solution**: Verify the project exists: `python setup_lakebase_project.py --list`

### Issue: Connection timeout (scale-to-zero)
**Solution**: The app retries with exponential backoff. First connection after idle may take 200-500ms.

### Issue: Tables Already Exist
**Solution**: The setup scripts use `IF NOT EXISTS`, so they're safe to run multiple times.

### Issue: Module Not Found
**Solution**: Run `pip install -r requirements.txt` to install all dependencies.

### Issue: Permission Denied
**Solution**: Grant permissions to the service principal:
```sql
GRANT USAGE ON SCHEMA ecommerce TO "<service-principal-id>";
GRANT ALL ON ALL TABLES IN SCHEMA ecommerce TO "<service-principal-id>";
```

---

## Sample Queries to Try

### Top Selling Products
```sql
SELECT p.name, COUNT(oi.order_item_id) as times_ordered,
       SUM(oi.quantity) as total_quantity
FROM ecommerce.products p
JOIN ecommerce.order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.name
ORDER BY times_ordered DESC
LIMIT 10
```

### User Purchase History
```sql
SELECT u.username, COUNT(o.order_id) as total_orders,
       SUM(o.total_amount) as lifetime_value
FROM ecommerce.users u
LEFT JOIN ecommerce.orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.username
ORDER BY lifetime_value DESC
```

### Audit Log Summary
```sql
SELECT table_name, operation, COUNT(*) as count,
       MAX(created_at) as last_activity
FROM ecommerce.audit_log
GROUP BY table_name, operation
ORDER BY last_activity DESC
```

---

**Last Updated**: February 2026
**Workspace**: https://fe-vm-hls-amer.cloud.databricks.com/
