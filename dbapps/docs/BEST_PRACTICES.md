# Databricks Apps - Best Practices Guide

## 🎯 Critical Configuration Requirements

### Port Configuration

#### ✅ DO: Use Environment Variables
```python
# Correct approach for all frameworks
import os

# Get port from DATABRICKS_APP_PORT (set by Databricks)
# Fallback to PORT for other cloud platforms
# Default to 8080 for local development
port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))

# Framework-specific examples:

# Dash
app.run_server(debug=False, host='0.0.0.0', port=port)

# Flask
app.run(host='0.0.0.0', port=port, debug=False)

# FastAPI/Uvicorn
uvicorn.run(app, host="0.0.0.0", port=port)

# Gradio
demo.launch(server_name="0.0.0.0", server_port=port)
```

#### ❌ DON'T: Hardcode Ports
```python
# This will cause 502 Bad Gateway errors in Databricks Apps
app.run_server(host='0.0.0.0', port=8080)  # ❌ WRONG
app.run_server(host='0.0.0.0', port=8000)  # ❌ WRONG
```

**Why?** Databricks Apps expects your application to listen on the port specified in the `DATABRICKS_APP_PORT` environment variable. Hardcoding the port creates a mismatch between what Databricks expects and what your app provides.

---

### Host Binding

#### ✅ DO: Bind to 0.0.0.0
```python
# Correct - Accepts connections from any network interface
host = '0.0.0.0'
app.run(host=host, port=port)
```

#### ❌ DON'T: Bind to localhost or 127.0.0.1
```python
# Wrong - Databricks proxy cannot connect
app.run(host='localhost', port=port)     # ❌ WRONG
app.run(host='127.0.0.1', port=port)     # ❌ WRONG
```

**Why?** The Databricks reverse proxy needs to connect to your app from outside the container. Binding to `localhost` or `127.0.0.1` only allows local connections.

---

## 🔒 Security Best Practices

### 1. Environment Variables & Secrets

#### ✅ DO: Use Databricks Secrets
```yaml
# app.yaml
env:
  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: database-credentials
        key: password

  - name: API_KEY
    valueFrom:
      secretKeyRef:
        name: api-credentials
        key: key
```

```python
# app.py
import os
db_password = os.environ.get('DB_PASSWORD')  # ✅ Secure
```

#### ❌ DON'T: Hardcode Secrets
```python
db_password = "my-secret-password"  # ❌ NEVER DO THIS
api_key = "sk-1234567890abcdef"     # ❌ NEVER DO THIS
```

```yaml
# app.yaml
env:
  - name: DB_PASSWORD
    value: "my-secret-password"  # ❌ NEVER DO THIS
```

### 2. Debug Mode

#### ✅ DO: Disable Debug in Production
```python
# Use environment variable to control debug mode
debug_mode = os.environ.get('DEBUG', 'false').lower() == 'true'
app.run(debug=debug_mode, host='0.0.0.0', port=port)
```

```yaml
# app.yaml
env:
  - name: DEBUG
    value: "false"  # ✅ Always false in production
```

#### ❌ DON'T: Enable Debug in Production
```python
app.run(debug=True, host='0.0.0.0', port=port)  # ❌ Security risk
```

---

## 📦 Dependency Management

### 1. Pin Versions

#### ✅ DO: Specify Exact Versions
```txt
# requirements.txt
dash==2.14.2
pandas==2.1.4
plotly==5.18.0
```

#### ⚠️ AVOID: Unpinned or Loose Versions
```txt
# Can cause unexpected breakage
dash>=2.0.0  # ⚠️ Risky
pandas        # ⚠️ Risky
plotly~=5.0   # ⚠️ Risky
```

### 2. Minimize Dependencies
```txt
# Only include what you actually use
dash==2.14.2
pandas==2.1.4

# Don't include unused packages
# numpy==1.24.3  # Not used, commented out
```

---

## 🗄️ Database Connections

### 1. Connection Pooling

#### ✅ DO: Use Connection Pools
```python
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

# Create engine with connection pooling
engine = create_engine(
    connection_string,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True  # Verify connections before use
)
```

#### ❌ DON'T: Create Connection Per Request
```python
# Inefficient - creates new connection each time
def get_data():
    conn = psycopg2.connect(**db_config)  # ❌ Inefficient
    # ... use connection
    conn.close()
```

### 2. Proper SSL Configuration

#### ✅ DO: Require SSL for Production
```python
DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'port': int(os.environ.get('DB_PORT', '5432')),
    'sslmode': 'require'  # ✅ Always use SSL
}
```

---

## 🏗️ Application Structure

### 1. Separation of Concerns

#### ✅ DO: Organize Code into Modules
```
my-app/
├── app.py              # Main app entry point
├── config.py           # Configuration
├── database.py         # Database connections
├── models.py           # Data models
├── routes/             # API routes
│   ├── __init__.py
│   ├── api.py
│   └── views.py
├── utils.py            # Utility functions
├── app.yaml            # Databricks config
└── requirements.txt    # Dependencies
```

#### ❌ DON'T: Put Everything in One File
```python
# app.py with 2000+ lines of code ❌
```

### 2. Configuration Management

#### ✅ DO: Centralize Configuration
```python
# config.py
import os

class Config:
    """Application configuration"""
    # App settings
    DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'
    PORT = int(os.environ.get('DATABRICKS_APP_PORT', '8080'))
    HOST = '0.0.0.0'

    # Database settings
    DB_HOST = os.environ.get('DB_HOST')
    DB_NAME = os.environ.get('DB_NAME')
    DB_USER = os.environ.get('DB_USER')
    DB_PASSWORD = os.environ.get('DB_PASSWORD')
    DB_PORT = int(os.environ.get('DB_PORT', '5432'))

# app.py
from config import Config

app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG)
```

---

## 🔍 Monitoring & Logging

### 1. Structured Logging

#### ✅ DO: Use Proper Logging
```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Use throughout your app
logger.info("App started on port %s", port)
logger.error("Database connection failed: %s", error)
```

#### ❌ DON'T: Use Print Statements
```python
print("App started")  # ❌ Not structured
print(f"Error: {error}")  # ❌ No log levels
```

### 2. Health Check Endpoints

#### ✅ DO: Implement Health Checks
```python
# For all frameworks, add a health check endpoint
@app.route('/health')  # Flask
def health():
    return {'status': 'healthy', 'timestamp': datetime.now().isoformat()}

@app.get('/health')  # FastAPI
async def health():
    return {'status': 'healthy', 'timestamp': datetime.now().isoformat()}
```

---

## 🚀 Performance Optimization

### 1. Caching

#### ✅ DO: Cache Expensive Operations
```python
from functools import lru_cache

@lru_cache(maxsize=100)
def get_expensive_data(query_id):
    # Expensive database query or computation
    return result
```

### 2. Async When Appropriate

#### ✅ DO: Use Async for I/O-Bound Operations
```python
# FastAPI example
@app.get("/data")
async def get_data():
    # Async database query
    result = await db.fetch_data()
    return result
```

---

## 📝 app.yaml Best Practices

### Complete Example
```yaml
# Full app.yaml with all best practices

# Command - how to start your app
command:
  - python
  - app.py

# Environment variables
env:
  # App configuration
  - name: APP_TITLE
    value: "Production App"

  - name: DEBUG
    value: "false"  # ALWAYS false in production

  # Database using secrets (RECOMMENDED)
  - name: DB_HOST
    value: "db.example.com"

  - name: DB_NAME
    value: "production_db"

  - name: DB_USER
    valueFrom:
      secretKeyRef:
        name: database-credentials
        key: username

  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: database-credentials
        key: password

  - name: DB_PORT
    value: "5432"

  # API keys using secrets
  - name: API_KEY
    valueFrom:
      secretKeyRef:
        name: api-credentials
        key: key

# Resource limits (optional but recommended)
# resources:
#   requests:
#     memory: "512Mi"
#     cpu: "500m"
#   limits:
#     memory: "1Gi"
#     cpu: "1000m"
```

---

## ✅ Pre-Deployment Checklist

### Before deploying to production:

- [ ] Port uses `DATABRICKS_APP_PORT` environment variable
- [ ] Host is set to `0.0.0.0`
- [ ] Debug mode is disabled (`DEBUG=false`)
- [ ] All secrets use Databricks Secrets (not hardcoded)
- [ ] Dependencies are pinned to specific versions
- [ ] Database connections use SSL
- [ ] Health check endpoint implemented
- [ ] Logging configured properly
- [ ] Error handling implemented
- [ ] app.yaml is properly configured
- [ ] Code tested locally first
- [ ] Environment variables documented
- [ ] No credentials in git repository

---

## 🐛 Common Issues & Solutions

### Issue: 502 Bad Gateway
**Symptoms**: App deployed but returns 502 error

**Root Causes**:
1. App not using `DATABRICKS_APP_PORT`
2. App binding to `localhost` instead of `0.0.0.0`
3. App failing to start due to missing dependencies
4. App crashing on startup

**Solutions**:
```python
# Fix 1: Use environment variable for port
port = int(os.environ.get('DATABRICKS_APP_PORT', '8080'))

# Fix 2: Bind to 0.0.0.0
app.run(host='0.0.0.0', port=port)

# Fix 3: Check app logs
databricks apps logs <app-name>

# Fix 4: Verify all dependencies in requirements.txt
pip install -r requirements.txt
```

### Issue: Connection Timeout
**Symptoms**: Database or external API timeouts

**Solutions**:
1. Verify network connectivity
2. Check firewall rules
3. Implement connection pooling
4. Add retry logic with exponential backoff

### Issue: App Running Slow
**Symptoms**: Slow response times

**Solutions**:
1. Implement caching
2. Use connection pooling
3. Optimize database queries
4. Profile code to find bottlenecks

---

## 📚 Additional Resources

- [Databricks Apps Documentation](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/)
- [Databricks Secrets Guide](https://docs.databricks.com/aws/en/security/secrets/)
- [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)

---

**Last Updated**: 2025-11-22
