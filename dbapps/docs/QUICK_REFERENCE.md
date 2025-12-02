# Databricks Apps - Quick Reference Card

## 🚨 The Two Critical Rules (Avoid 502 Errors!)

### 1️⃣ Use DATABRICKS_APP_PORT
```python
# ✅ ALWAYS DO THIS
port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))

# ❌ NEVER DO THIS
port = 8080  # Hardcoded port causes 502 errors!
```

### 2️⃣ Bind to 0.0.0.0
```python
# ✅ ALWAYS DO THIS
host = '0.0.0.0'

# ❌ NEVER DO THIS
host = 'localhost'  # or '127.0.0.1' - Databricks proxy can't connect!
```

---

## ⚡ Framework Quick Reference

### Dash
```python
import os
import dash

app = dash.Dash(__name__)

if __name__ == '__main__':
    port = int(os.environ.get('DATABRICKS_APP_PORT', '8080'))
    app.run_server(debug=False, host='0.0.0.0', port=port)
```

### Streamlit (app.yaml)
```yaml
command:
  - streamlit
  - run
  - app.py
  - --server.address
  - "0.0.0.0"
```

### Flask
```python
import os
from flask import Flask

app = Flask(__name__)

if __name__ == '__main__':
    port = int(os.environ.get('DATABRICKS_APP_PORT', '8080'))
    app.run(host='0.0.0.0', port=port, debug=False)
```

### FastAPI
```python
import os
from fastapi import FastAPI
import uvicorn

app = FastAPI()

if __name__ == "__main__":
    port = int(os.environ.get('DATABRICKS_APP_PORT', '8080'))
    uvicorn.run(app, host="0.0.0.0", port=port)
```

### Gradio
```python
import os
import gradio as gr

demo = gr.Interface(...)

if __name__ == "__main__":
    port = int(os.environ.get('DATABRICKS_APP_PORT', '8080'))
    demo.launch(server_name="0.0.0.0", server_port=port, share=False)
```

---

## 🔒 Secrets (app.yaml)

### Using Databricks Secrets
```yaml
env:
  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: database-credentials  # Secret scope name
        key: password               # Secret key
```

### Creating Secrets (CLI)
```bash
# Create secret scope
databricks secrets create-scope database-credentials

# Add secret
databricks secrets put-secret database-credentials password
```

---

## 📦 Common app.yaml Templates

### Minimal app.yaml
```yaml
command:
  - python
  - app.py

env:
  - name: DEBUG
    value: "false"
```

### With Database
```yaml
command:
  - python
  - app.py

env:
  - name: DEBUG
    value: "false"

  - name: DB_HOST
    value: "your-db-host.com"

  - name: DB_NAME
    value: "mydb"

  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: db-creds
        key: password
```

---

## 🐛 Troubleshooting

### 502 Bad Gateway
```bash
# Check 1: Verify port configuration
grep -n "DATABRICKS_APP_PORT" app.py

# Check 2: Verify host binding
grep -n "0.0.0.0" app.py

# Check 3: View logs
databricks apps logs <app-name>

# Check 4: Verify app is running
databricks apps get <app-name>
```

### App Won't Start
```bash
# Check logs
databricks apps logs <app-name>

# Common issues:
# - Missing dependencies (check requirements.txt)
# - Missing environment variables (check app.yaml)
# - Python syntax errors (check logs)
```

---

## 🚀 Deployment Commands

### Create & Deploy
```bash
# Create app
databricks apps create my-app --description "My awesome app"

# Deploy app
databricks apps deploy my-app --source-code-path .

# View app
databricks apps get my-app

# View logs
databricks apps logs my-app

# Delete app
databricks apps delete my-app
```

### Using Databricks Asset Bundles
```bash
# Deploy
databricks bundle deploy

# Run
databricks bundle run my-app
```

---

## 📊 Database Connection Template

```python
import os
import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'port': int(os.environ.get('DB_PORT', '5432')),
    'sslmode': 'require'
}

# Connect
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor(cursor_factory=RealDictCursor)

# Query
cursor.execute("SELECT * FROM mytable")
results = cursor.fetchall()

# Close
cursor.close()
conn.close()
```

---

## ✅ Pre-Deploy Checklist (Short Version)

```
[ ] Port uses DATABRICKS_APP_PORT
[ ] Host is 0.0.0.0
[ ] Debug = false
[ ] No hardcoded secrets
[ ] Dependencies in requirements.txt
[ ] app.yaml exists
[ ] Tested locally
```

---

## 📚 Where to Find Help

| Resource | Location |
|----------|----------|
| Templates | `dbapps/templates/<framework>/` |
| Best Practices | `dbapps/docs/BEST_PRACTICES.md` |
| Deployment Checklist | `dbapps/docs/DEPLOYMENT_CHECKLIST.md` |
| Official Docs | https://docs.databricks.com/apps/ |

---

## 💡 Pro Tips

1. **Test locally first**: Set `DATABRICKS_APP_PORT=8080` and run locally
2. **Use health checks**: Add `/health` endpoint to verify app is running
3. **Check logs often**: `databricks apps logs <app-name>`
4. **Pin dependencies**: Use exact versions in requirements.txt
5. **Never commit secrets**: Use `.gitignore` and Databricks Secrets

---

**Print this page and keep it handy!**

Last Updated: 2025-11-22
