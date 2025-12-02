# Databricks Lakebase Training App - Deployment Guide

## 🎉 Deployment Status: COMPLETE

### Overview
The Databricks Lakebase Training Dashboard has been successfully deployed and is available in your Databricks workspace.

---

## 📦 What Was Deployed

### 1. GitHub Repository
- **Repository URL**: https://github.com/suryasai87/lakebase-training-app
- **Branch**: main
- **Status**: ✅ Active and synced

### 2. Databricks Workspace Files
- **Location**: `/Workspace/Users/suryasai.turaga@databricks.com/lakebase-training-app`
- **Files Deployed**:
  - `app.py` - Main Streamlit application
  - `app.yaml` - Databricks App configuration
  - `requirements.txt` - Python dependencies
  - `README.md` - Documentation
  - `databricks.yml` - Databricks bundle configuration

### 3. Databricks Repos Integration
- **Repo ID**: 567797471720392
- **Location**: `/Repos/suryasai.turaga@databricks.com/lakebase-training-app`
- **Provider**: GitHub
- **Commit**: 1fc8c74c264679f9f7889c27e51fe0245be11361
- **Status**: ✅ Connected and synced

---

## 🚀 How to Run the App

### Option 1: Run from Workspace Files
1. Navigate to your Databricks workspace
2. Go to: `/Workspace/Users/suryasai.turaga@databricks.com/lakebase-training-app`
3. Open `app.py`
4. Click "Run" to execute the Streamlit app

### Option 2: Run from Repos (Recommended)
1. Navigate to Databricks Repos
2. Go to: `/Repos/suryasai.turaga@databricks.com/lakebase-training-app`
3. Open `app.py`
4. Click "Run" to execute the Streamlit app

### Option 3: Deploy as Databricks App
Due to the workspace app limit (300 apps reached), you'll need to:

1. **Delete an unused app first**:
   ```bash
   databricks apps list
   databricks apps delete <app-name-to-remove>
   ```

2. **Then create the new app**:
   ```bash
   cd /Users/suryasai.turaga/lakebase-training-app
   databricks apps create lakebase-training-dashboard --description "Lakebase Training Dashboard"
   databricks apps deploy lakebase-training-dashboard --source-code-path .
   ```

---

## 🔧 Configuration Required

### Environment Variables
Before running the app, you need to set up your Lakebase credentials. You can do this in two ways:

#### Method 1: Using Databricks Secrets
```python
# In your notebook or app
import os
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create secret scope
w.secrets.create_scope(scope="lakebase-training")

# Add secrets
w.secrets.put_secret(
    scope="lakebase-training",
    key="lakebase_user",
    string_value="your_username"
)

w.secrets.put_secret(
    scope="lakebase-training",
    key="lakebase_password",
    string_value="your_password"
)
```

#### Method 2: Update app.yaml
Edit the `app.yaml` file in the workspace or repo:
```yaml
env:
  - name: LAKEBASE_HOST
    value: "ep-training-lakebase.us-east-2.aws.neon.tech"
  - name: LAKEBASE_DB
    value: "trainingdb"
  - name: LAKEBASE_USER
    value: "your_actual_username"
  - name: LAKEBASE_PASSWORD
    value: "your_actual_password"
```

---

## 📊 Application Features

The deployed application includes:

### 1. **Dashboard**
- Real-time metrics display
- User, product, and order statistics
- Revenue tracking
- Interactive charts with Plotly

### 2. **Data Entry**
- Add products with details
- Create new users
- Place orders
- CRUD operations

### 3. **Query Builder**
- Pre-built sample queries
- Custom SQL query interface
- CSV export functionality
- Real-time query execution

### 4. **Vector Search Demo**
- Semantic search interface
- pg_vector integration showcase
- Similarity scoring
- Hybrid search capabilities

### 5. **API Testing**
- PostgREST endpoint testing
- HTTP method selection
- Request/response visualization
- Query parameter support

---

## 🔗 Quick Access Links

| Resource | URL/Path |
|----------|----------|
| GitHub Repository | https://github.com/suryasai87/lakebase-training-app |
| Databricks Workspace Files | `/Workspace/Users/suryasai.turaga@databricks.com/lakebase-training-app` |
| Databricks Repo | `/Repos/suryasai.turaga@databricks.com/lakebase-training-app` |
| Training Materials | See `README.md` in the repo |

---

## 🗄️ Database Setup

Before using the app, you need to set up your Lakebase database:

### 1. Connect to Lakebase
```bash
psql "postgresql://username:password@ep-training-lakebase.us-east-2.aws.neon.tech:5432/trainingdb?sslmode=require"
```

### 2. Create the Schema
```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- Create tables
CREATE TABLE ecommerce.users (
    user_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(50) UNIQUE NOT NULL,
    full_name VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

CREATE TABLE ecommerce.products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    category VARCHAR(100),
    tags TEXT[]
);

CREATE TABLE ecommerce.orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES ecommerce.users(user_id),
    order_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(10, 2)
);
```

### 3. Insert Sample Data
```sql
INSERT INTO ecommerce.users (email, username, full_name, metadata) VALUES
('john.doe@example.com', 'johndoe', 'John Doe', '{"role": "customer"}'),
('jane.smith@example.com', 'janesmith', 'Jane Smith', '{"role": "customer"}');

INSERT INTO ecommerce.products (name, description, price, stock_quantity, category) VALUES
('Laptop Pro 2024', 'High-performance laptop', 1999.99, 50, 'Electronics'),
('Wireless Mouse', 'Ergonomic mouse', 49.99, 200, 'Accessories');
```

---

## 🔄 Updating the App

### From GitHub
1. Make changes to your local repository
2. Commit and push to GitHub:
   ```bash
   git add .
   git commit -m "Update app"
   git push origin main
   ```
3. In Databricks Repos, click "Pull" to sync changes

### Direct in Databricks
1. Navigate to the Repos or Workspace location
2. Edit files directly
3. Changes are reflected immediately when you run the app

---

## 🐛 Troubleshooting

### Connection Errors
- Verify Lakebase credentials in `app.yaml` or secrets
- Check network connectivity to Lakebase endpoint
- Ensure SSL mode is set to 'require'

### Missing Tables
- Run the database setup script (see Database Setup section)
- Verify schema exists: `SELECT schema_name FROM information_schema.schemata;`

### App Won't Start
- Check Python dependencies in `requirements.txt`
- Verify Streamlit is installed
- Check for port conflicts (default: 8080)

---

## 📚 Additional Resources

### Training Materials
- Complete training guide: See `databricks-lakebase-training.md` in attachments
- Sample queries and exercises included in the app
- Video tutorials: [Link to be added]

### Documentation
- [Databricks Apps Documentation](https://docs.databricks.com/apps/)
- [Neon/Lakebase Documentation](https://neon.tech/docs)
- [Streamlit Documentation](https://docs.streamlit.io)

### Support
- GitHub Issues: https://github.com/suryasai87/lakebase-training-app/issues
- Databricks Community: https://community.databricks.com

---

## 📝 Next Steps

1. ✅ Set up Lakebase database credentials
2. ✅ Create database schema and sample data
3. ✅ Run the application from Workspace or Repos
4. ✅ Explore the training materials
5. ✅ Customize the app for your specific use case

---

## 🎓 Training Modules Covered

The application demonstrates concepts from the full 1-day training:

- ✅ Lakebase setup and configuration
- ✅ DDL and DML operations
- ✅ Building Databricks Apps with Streamlit
- ✅ PostgREST API integration
- ✅ Vector search with pg_vector
- ✅ Advanced PostgreSQL features

---

**Deployment Date**: November 21, 2025
**Deployed By**: Surya Sai Turaga
**Databricks User**: suryasai.turaga@databricks.com
**Workspace**: 1444828305810485

---

*For questions or issues, please refer to the GitHub repository or contact your Databricks administrator.*
