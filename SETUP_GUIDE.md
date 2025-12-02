# 🚀 Databricks Lakebase Training App - Complete Setup Guide

## ✅ All Files Deployed Successfully!

---

## 📍 Your App is Here

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

## 🗄️ Your Lakebase Database Connection

**Instance**: `instance-868832b3-5ee5-4d06-a412-b5d13e28d853.database.cloud.databricks.com`
**Database**: `databricks_postgres`
**User**: `suryasai.turaga@databricks.com`
**Port**: `5432`
**SSL Mode**: `require`

✅ **Configuration already set up** in `app.yaml` and `setup_and_deploy.py`

---

## 📦 What's Been Deployed

All files are ready in your Databricks workspace:

1. ✅ **dash_app.py** - Main Dash application with Framer Motion animations
2. ✅ **app.yaml** - Configured with your Lakebase credentials
3. ✅ **requirements.txt** - All Python dependencies
4. ✅ **setup_database.sql** - Complete database setup script
5. ✅ **setup_and_deploy.py** - Automated setup script
6. ✅ **README.md**, **DEPLOYMENT.md**, **FINAL_DEPLOYMENT.md** - Documentation
7. ✅ **databricks.yml** - Databricks bundle configuration

---

## 🎯 Quick Start - Run the App in 3 Steps

### Step 1: Set Up the Database (5 minutes)

Navigate to your Databricks workspace and open a **SQL Editor** or **Notebook**:

#### Option A: Using SQL Editor (Easiest)
1. Go to: https://fe-vm-hls-amer.cloud.databricks.com/sql/editor
2. Connect to your Lakebase instance
3. Copy and paste the entire contents of `setup_database.sql`
4. Click "Run"

#### Option B: Using Python Notebook
1. Create a new Python notebook in Databricks
2. Copy this code:

```python
import psycopg2
from psycopg2.extras import RealDictCursor

# Lakebase connection configuration
TOKEN = 'eyJraWQiOiI2NDZiZWZkNGY5NjYwMTdiNjk1MjRjOTRlMjcxNzljY2YyZmRlZDU1ZGJiMzQ5N2UwZjEwM2EwMzljZjI2ODU3IiwidHlwIjoiYXQrand0IiwiYWxnIjoiUlMyNTYifQ.eyJjbGllbnRfaWQiOiJkYXRhYnJpY2tzLXNlc3Npb24iLCJzY29wZSI6ImlhbS5jdXJyZW50LXVzZXI6cmVhZCBpYW0uZ3JvdXBzOnJlYWQgaWFtLnNlcnZpY2UtcHJpbmNpcGFsczpyZWFkIGlhbS51c2VyczpyZWFkIiwiaWRtIjoiRUFBPSIsImlzcyI6Imh0dHBzOi8vZTItZGVtby1maWVsZC1lbmcuY2xvdWQuZGF0YWJyaWNrcy5jb20vb2lkYyIsImF1ZCI6IjE0NDQ4MjgzMDU4MTA0ODUiLCJzdWIiOiJzdXJ5YXNhaS50dXJhZ2FAZGF0YWJyaWNrcy5jb20iLCJpYXQiOjE3NjM3NDAxMjgsImV4cCI6MTc2Mzc0MzcyOCwianRpIjoiN2VkYTk5ZTAtNWI2NC00NWVjLTkyZmQtYjAzOTlmMmIxNTU2In0.oaSX4wnqF0Je-H781pYT_nRdl99bo9PWcu5TMqcJY0-i8M1UugiN5_EM2i1aWrLjIXdBmlIdF-NdBVeBSUNgOy5z_RNg21b0H8gkzS6Gw0nnv4uqqFizLcjMkstnt9Xp2-h4kmTK7DZ5Jfr4Wl4nQKSHnII3d6uOHp4srUkc3C7HtzBcmzZON1D2Capc-JSEwytU5eGzVpZiXegfdbcjxXHNOcyY_HD6wJYRfMbdJeFqX236mVtVMYSsgyiIDTWvDF9fmYM-Z_IKaB1MFMm2O19-BFqP4MN-Wbt3H2g2U8KdPHTf-85nrd7ylVIFno3iNJwfzbq0NMoGF-G_sZiy4Q'

conn = psycopg2.connect(
    host='instance-868832b3-5ee5-4d06-a412-b5d13e28d853.database.cloud.databricks.com',
    database='databricks_postgres',
    user='token',
    password=TOKEN,
    port=5432,
    sslmode='require'
)

cursor = conn.cursor()

# Read and execute setup script
with open('/Workspace/Users/suryasai.turaga@databricks.com/lakebase-training/setup_database.sql', 'r') as f:
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

print(f"✅ Database setup completed!")
print(f"   - Users: {result[0]}")
print(f"   - Products: {result[1]}")
print(f"   - Orders: {result[2]}")

cursor.close()
conn.close()
```

3. Run the cell

### Step 2: Install Dependencies (2 minutes)

In a Databricks notebook, run:

```python
%pip install dash==2.14.2 dash-bootstrap-components==1.5.0 plotly==5.18.0 psycopg2-binary==2.9.9
```

### Step 3: Run the Dash App (1 minute)

1. Navigate to: `/Repos/suryasai.turaga@databricks.com/lakebase-training/`
2. Open: `dash_app.py`
3. Click: **"Run"**
4. The app will start on port 8080
5. Databricks will provide you a URL to access the dashboard

---

## 🎨 What You'll See

Once the app is running, you'll experience:

### ✨ Beautiful Framer Motion-Style Animations
- Smooth fadeIn effects for page loads
- SlideIn animations for navigation tabs
- ScaleIn effects for metric cards
- Hover animations with lift and shadow
- Purple gradient theme (#667eea → #764ba2)

### 📊 Complete Dashboard Features
1. **Dashboard Tab**
   - 4 animated metric cards (users, products, orders, revenue)
   - Product inventory bar chart
   - Revenue trend line chart
   - Recent orders table with auto-refresh

2. **Data Entry Tab**
   - Add products form with validation
   - Add users form with JSONB metadata
   - Smooth form animations

3. **Query Builder Tab**
   - Sample pre-built queries
   - Custom SQL editor
   - Results display with CSV download

4. **Vector Search Tab**
   - Semantic search demo
   - Similarity scoring
   - Hybrid search options

5. **API Testing Tab**
   - HTTP request builder
   - Response viewer
   - PostgREST endpoint testing

---

## 🗄️ Database Schema Created

The setup script creates:

### Tables
- **ecommerce.users** - User accounts with JSONB metadata
- **ecommerce.products** - Product catalog with tags and categories
- **ecommerce.orders** - Order management with addresses
- **ecommerce.order_items** - Order line items with subtotals

### Sample Data
- 3 users (including admin)
- 8 products across categories
- 3 sample orders with line items

### Indexes
- Email and username lookups
- Product category filtering
- Order user relationships
- JSONB metadata search

---

## 🔧 Configuration Details

### app.yaml (Already Configured)
```yaml
command:
  - python
  - dash_app.py

env:
  - name: LAKEBASE_HOST
    value: "instance-868832b3-5ee5-4d06-a412-b5d13e28d853.database.cloud.databricks.com"
  - name: LAKEBASE_DB
    value: "databricks_postgres"
  - name: LAKEBASE_USER
    value: "suryasai.turaga@databricks.com"
  - name: LAKEBASE_PASSWORD
    value: "[YOUR_TOKEN_HERE]"  # Already set
  - name: LAKEBASE_PORT
    value: "5432"
```

---

## 📝 Alternative: Deploy as Databricks App

If you want to deploy as a permanent Databricks App:

```bash
cd /Workspace/Users/suryasai.turaga@databricks.com/lakebase-training

# Note: workspace has 300 apps limit
# You may need to delete an old app first:
# databricks apps list
# databricks apps delete <old-app-name>

# Then deploy
databricks apps create lakebase-training --description "Lakebase Training Dashboard"
databricks apps deploy lakebase-training --source-code-path .
```

---

## 🔗 Quick Access Links

| Resource | Link |
|----------|------|
| **Databricks Workspace** | https://fe-vm-hls-amer.cloud.databricks.com/ |
| **Repos Location** | `/Repos/suryasai.turaga@databricks.com/lakebase-training` |
| **Workspace Location** | `/Workspace/Users/suryasai.turaga@databricks.com/lakebase-training` |
| **GitHub Repository** | https://github.com/suryasai87/lakebase-training-app |

---

## 🐛 Troubleshooting

### Issue: Database Connection Error
**Solution**: The token is already configured in `app.yaml`. If it expires, you'll need to generate a new one.

### Issue: Tables Already Exist
**Solution**: The setup script uses `IF NOT EXISTS`, so it's safe to run multiple times.

### Issue: Module Not Found
**Solution**: Run the pip install command in Step 2 to install all dependencies.

### Issue: App Won't Start
**Solution**: Make sure you run the app from a Databricks environment (Notebook or Repos), not locally.

---

## 📚 Sample Queries to Try

Once the database is set up, try these queries in the Query Builder:

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

### Low Stock Alert
```sql
SELECT name, stock_quantity, category
FROM ecommerce.products
WHERE stock_quantity < 10
ORDER BY stock_quantity ASC
```

---

## ✅ Deployment Checklist

- [x] GitHub repository cloned to Databricks Repos
- [x] All files deployed to Workspace
- [x] Database connection configured in app.yaml
- [x] Authentication token set up
- [x] Database setup script ready
- [x] Documentation provided
- [ ] **Run database setup script** (Step 1 above)
- [ ] **Install dependencies** (Step 2 above)
- [ ] **Start the Dash app** (Step 3 above)

---

## 🎉 You're Ready to Go!

Everything is set up and ready. Just follow the 3 quick steps above to:
1. Set up your database
2. Install dependencies
3. Run the app

Your beautiful Dash dashboard with Framer Motion animations awaits! 🚀

---

**Deployment Date**: November 21, 2025
**Workspace**: https://fe-vm-hls-amer.cloud.databricks.com/
**Deployed By**: Claude Code
**Repository**: https://github.com/suryasai87/lakebase-training-app
