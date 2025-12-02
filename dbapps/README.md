# Databricks Apps Scaffolding
**Production-Ready Templates for Databricks Apps Deployment**

---

## 📋 Overview

This scaffolding provides production-ready templates for deploying various types of applications on Databricks Apps. Each template follows Databricks best practices and includes proper port configuration, environment variable handling, and deployment configurations.

## 🚨 Critical Requirements for All Databricks Apps

### **1. Port Configuration**
```python
# ✅ CORRECT - Use DATABRICKS_APP_PORT environment variable
port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))
app.run(host='0.0.0.0', port=port)

# ❌ WRONG - Hardcoded port will cause 502 Bad Gateway errors
app.run(host='0.0.0.0', port=8080)
```

### **2. Host Binding**
```python
# ✅ CORRECT - Bind to 0.0.0.0 for Databricks proxy
host='0.0.0.0'

# ❌ WRONG - localhost or 127.0.0.1 won't work
host='localhost'  # or host='127.0.0.1'
```

### **3. Environment Variables**
- Use `DATABRICKS_APP_PORT` for the application port
- Store sensitive data in Databricks Secrets
- Never hardcode credentials or API keys

---

## 📁 Available Templates

| Framework | Use Case | Location |
|-----------|----------|----------|
| **Streamlit** | Data apps, dashboards, ML demos | `templates/streamlit/` |
| **Dash** | Interactive dashboards, analytics | `templates/dash/` |
| **Gradio** | ML model interfaces, demos | `templates/gradio/` |
| **Flask** | RESTful APIs, web apps | `templates/flask/` |
| **FastAPI** | Modern async APIs, microservices | `templates/fastapi/` |

---

## 🚀 Quick Start

### 1. Choose Your Template
```bash
# Navigate to the template directory
cd dbapps/templates/<framework-name>

# Example: For a Dash app
cd dbapps/templates/dash
```

### 2. Copy Template to Your Project
```bash
# Copy all files to your project directory
cp -r dbapps/templates/dash/* /path/to/your/project/
```

### 3. Customize Your App
- Edit `app.py` to implement your business logic
- Update `app.yaml` with your environment variables
- Modify `requirements.txt` for your dependencies

### 4. Deploy to Databricks
```bash
# Using Databricks CLI
databricks apps create my-app --description "My awesome app"
databricks apps deploy my-app --source-code-path .

# Or using Databricks Asset Bundles
databricks bundle deploy
```

---

## 📖 Template Structure

Each template contains:

```
<framework-name>/
├── app.py              # Main application file
├── app.yaml            # Databricks app configuration
└── requirements.txt    # Python dependencies
```

### app.py
- ✅ Proper port configuration using `DATABRICKS_APP_PORT`
- ✅ Host binding to `0.0.0.0`
- ✅ Environment variable support
- ✅ Framework-specific best practices

### app.yaml
- ✅ Command configuration for starting the app
- ✅ Environment variable definitions
- ✅ Comments explaining critical settings

### requirements.txt
- ✅ Framework-specific dependencies
- ✅ Optional dependencies (commented out)
- ✅ Version pinning for reproducibility

---

## 🔧 Common Configurations

### Database Connection Example
```python
# In your app.py
import os

DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'port': int(os.environ.get('DB_PORT', '5432'))
}
```

```yaml
# In your app.yaml
env:
  - name: DB_HOST
    value: "your-database-host.com"
  - name: DB_NAME
    value: "your_database"
  - name: DB_USER
    value: "your_username"
  - name: DB_PASSWORD
    value: "your_password"  # Use Databricks Secrets in production!
  - name: DB_PORT
    value: "5432"
```

### Using Databricks Secrets (Production)
```yaml
# app.yaml with secrets
env:
  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: db-credentials
        key: password
```

---

## 🐛 Troubleshooting

### 502 Bad Gateway Error
**Cause**: App not listening on correct port or host

**Solution**:
1. Ensure you're using `DATABRICKS_APP_PORT` environment variable
2. Verify host is set to `0.0.0.0`
3. Check app.yaml command is correct

```python
# Fix in app.py
port = int(os.environ.get('DATABRICKS_APP_PORT', '8080'))
app.run(host='0.0.0.0', port=port)
```

### App Won't Start
**Cause**: Missing dependencies or environment variables

**Solution**:
1. Check all dependencies are in `requirements.txt`
2. Verify environment variables in `app.yaml`
3. Review app logs in Databricks

### Connection Timeout
**Cause**: Database or external service not accessible

**Solution**:
1. Verify network connectivity from Databricks
2. Check firewall rules
3. Ensure credentials are correct

---

## 📚 Best Practices

### 1. Security
- ✅ Use Databricks Secrets for sensitive data
- ✅ Never commit credentials to git
- ✅ Set `DEBUG=false` in production
- ✅ Use HTTPS for external connections

### 2. Performance
- ✅ Pin dependency versions in requirements.txt
- ✅ Use connection pooling for databases
- ✅ Implement caching where appropriate
- ✅ Monitor app resource usage

### 3. Deployment
- ✅ Test locally before deploying
- ✅ Use version control (git)
- ✅ Document environment variables
- ✅ Implement health check endpoints

### 4. Development Workflow
```bash
# 1. Develop locally
python app.py

# 2. Test with environment variables
export DATABRICKS_APP_PORT=8080
python app.py

# 3. Deploy to Databricks
databricks apps deploy my-app --source-code-path .

# 4. Monitor logs
databricks apps logs my-app
```

---

## 📖 Additional Resources

### Official Documentation
- [Databricks Apps Documentation](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/)
- [Databricks Apps Best Practices](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/best-practices)
- [Environment Variables](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/environment-variables)

### Framework-Specific Guides
- [Streamlit Tutorial](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/tutorial-streamlit)
- [Dash Documentation](https://dash.plotly.com/)
- [Gradio Documentation](https://www.gradio.app/docs)
- [Flask Documentation](https://flask.palletsprojects.com/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)

---

## 🤝 Contributing

Found an issue or want to add a new template?

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

## 📝 License

These templates are provided as-is for use with Databricks Apps.

---

**Last Updated**: 2025-11-22
**Version**: 1.0.0
