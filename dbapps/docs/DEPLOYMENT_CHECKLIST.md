# Databricks Apps Deployment Checklist

## 📋 Pre-Deployment Checklist

Use this checklist before deploying any app to Databricks Apps to avoid common issues like 502 errors, security vulnerabilities, and performance problems.

---

## ✅ Code Configuration

### Port Configuration (CRITICAL)
- [ ] App uses `DATABRICKS_APP_PORT` environment variable
  ```python
  port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))
  ```
- [ ] NO hardcoded ports in code (no `port=8080`, `port=8000`, etc.)
- [ ] Port variable is converted to integer
- [ ] Fallback port for local development (8080 recommended)

### Host Binding (CRITICAL)
- [ ] App binds to `0.0.0.0` (NOT `localhost` or `127.0.0.1`)
  ```python
  app.run(host='0.0.0.0', port=port)
  ```
- [ ] Verified in all framework-specific run commands

### Framework-Specific Checks

#### For Dash Apps:
- [ ] `app.run_server(debug=False, host='0.0.0.0', port=port)`

#### For Streamlit Apps:
- [ ] `app.yaml` includes `--server.address 0.0.0.0`
- [ ] Command: `streamlit run app.py --server.address 0.0.0.0`

#### For Flask Apps:
- [ ] `app.run(host='0.0.0.0', port=port, debug=False)`

#### For FastAPI Apps:
- [ ] `uvicorn.run(app, host="0.0.0.0", port=port)`

#### For Gradio Apps:
- [ ] `demo.launch(server_name="0.0.0.0", server_port=port)`

---

## 🔒 Security

### Debug Mode
- [ ] Debug mode disabled in production (`DEBUG=false`)
- [ ] Debug mode controlled by environment variable (not hardcoded)
- [ ] Verified in all framework run commands

### Secrets & Credentials
- [ ] NO hardcoded passwords in code
- [ ] NO hardcoded API keys in code
- [ ] NO credentials in `app.yaml` values
- [ ] All secrets use `secretKeyRef` in `app.yaml`
  ```yaml
  env:
    - name: DB_PASSWORD
      valueFrom:
        secretKeyRef:
          name: database-credentials
          key: password
  ```
- [ ] NO `.env` files committed to git
- [ ] `.gitignore` includes sensitive files

### SSL/TLS
- [ ] Database connections use SSL (`sslmode: require`)
- [ ] External API calls use HTTPS (not HTTP)

---

## 📦 Dependencies

### requirements.txt
- [ ] All dependencies listed
- [ ] Versions pinned (e.g., `dash==2.14.2`, not `dash>=2.0`)
- [ ] NO development-only dependencies in production requirements
- [ ] Tested `pip install -r requirements.txt` successfully

### Python Version
- [ ] Python version specified (if needed)
- [ ] Compatible with Databricks runtime

---

## 🗄️ Database Configuration

### Connection Settings
- [ ] Database host from environment variable
- [ ] Database name from environment variable
- [ ] Database credentials from Databricks Secrets
- [ ] Database port from environment variable (with default)
- [ ] SSL mode configured (`sslmode: require`)

### Connection Management
- [ ] Connection pooling implemented (for high-traffic apps)
- [ ] Connections properly closed
- [ ] Error handling for connection failures
- [ ] Retry logic implemented (optional but recommended)

---

## 📄 app.yaml Configuration

### Command
- [ ] Correct command to start app
- [ ] All command arguments included
- [ ] Command tested locally

### Environment Variables
- [ ] All required environment variables defined
- [ ] Sensitive values use `secretKeyRef`
- [ ] Non-sensitive values use `value`
- [ ] NO default values for secrets in code (should fail if not set)

### Example Structure:
```yaml
command:
  - python
  - app.py

env:
  - name: APP_TITLE
    value: "My App"
  - name: DEBUG
    value: "false"
  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: db-creds
        key: password
```

---

## 🔍 Code Quality

### Error Handling
- [ ] Try-catch blocks for external calls
- [ ] Graceful error messages for users
- [ ] Errors logged (not just printed)
- [ ] Database errors handled properly

### Logging
- [ ] Logging configured (not using print statements)
- [ ] Appropriate log levels used
- [ ] Sensitive data NOT logged
- [ ] Structured logging format

### Health Checks
- [ ] `/health` endpoint implemented
- [ ] Returns appropriate status code
- [ ] Includes basic system info (optional)

---

## 🧪 Testing

### Local Testing
- [ ] App runs successfully locally
- [ ] Tested with `DATABRICKS_APP_PORT` environment variable
  ```bash
  export DATABRICKS_APP_PORT=8080
  python app.py
  ```
- [ ] All features work as expected
- [ ] Database connections successful
- [ ] No errors in console/logs

### Environment Variable Testing
- [ ] Tested with all environment variables set
- [ ] Tested with missing optional variables
- [ ] Verified fallback values work

---

## 📝 Documentation

### Code Documentation
- [ ] README.md exists and is up to date
- [ ] Environment variables documented
- [ ] Deployment instructions included
- [ ] Setup instructions clear

### Comments
- [ ] Complex logic commented
- [ ] Configuration choices explained
- [ ] TODOs removed or documented

---

## 🚀 Deployment

### Pre-Deploy
- [ ] Code committed to git
- [ ] Branch pushed to remote
- [ ] No uncommitted changes
- [ ] `.gitignore` properly configured

### Databricks Secrets
- [ ] All secrets created in Databricks
- [ ] Secret scope exists
- [ ] Secret keys match `app.yaml` references
- [ ] Secrets tested

### Deploy Commands
- [ ] Deployment command ready
  ```bash
  databricks apps create <app-name> --description "App description"
  databricks apps deploy <app-name> --source-code-path .
  ```
- [ ] App name chosen (must be unique)

---

## ✅ Post-Deployment

### Verification
- [ ] App accessible via URL
- [ ] NO 502 errors
- [ ] Health check endpoint responding
- [ ] Main functionality works
- [ ] Database connections successful

### Monitoring
- [ ] App logs reviewed
  ```bash
  databricks apps logs <app-name>
  ```
- [ ] No errors in logs
- [ ] Performance acceptable
- [ ] Resource usage monitored

### Rollback Plan
- [ ] Previous version available
- [ ] Rollback procedure documented
- [ ] Know how to redeploy previous version

---

## 🐛 Common Issues Quick Reference

### 502 Bad Gateway
**Checklist**:
- [ ] Using `DATABRICKS_APP_PORT`?
- [ ] Binding to `0.0.0.0`?
- [ ] App actually starting (check logs)?
- [ ] Dependencies installed?

### Connection Timeout
**Checklist**:
- [ ] Database host correct?
- [ ] Firewall rules allow connection?
- [ ] Credentials correct?
- [ ] Network connectivity from Databricks?

### App Not Starting
**Checklist**:
- [ ] Check logs for errors
- [ ] All dependencies in requirements.txt?
- [ ] Environment variables set?
- [ ] Python syntax errors?

---

## 📊 Framework-Specific Checklists

### Dash Apps
- [ ] `import os` at top of file
- [ ] Port from `DATABRICKS_APP_PORT`
- [ ] `app.run_server(debug=False, host='0.0.0.0', port=port)`
- [ ] `suppress_callback_exceptions=True` (if using callbacks)

### Streamlit Apps
- [ ] Command includes `--server.address 0.0.0.0`
- [ ] `STREAMLIT_GATHER_USAGE_STATS=false` in app.yaml
- [ ] `STREAMLIT_SERVER_HEADLESS=true` in app.yaml

### FastAPI Apps
- [ ] `uvicorn.run(app, host="0.0.0.0", port=port)`
- [ ] Health check endpoint at `/health`
- [ ] OpenAPI docs accessible at `/docs`

### Flask Apps
- [ ] `app.run(host='0.0.0.0', port=port, debug=False)`
- [ ] Health check route defined
- [ ] Static files configured (if needed)

### Gradio Apps
- [ ] `demo.launch(server_name="0.0.0.0", server_port=port)`
- [ ] `share=False` (important for security)

---

## 🎯 Priority Levels

### 🔴 CRITICAL (Will cause deployment failure)
- Port configuration
- Host binding
- Command in app.yaml
- Required dependencies

### 🟡 HIGH (Security/Performance issues)
- Debug mode disabled
- Secrets properly configured
- SSL enabled
- Dependency versions pinned

### 🟢 MEDIUM (Best practices)
- Logging configured
- Health checks
- Error handling
- Documentation

---

## 📋 Quick Checklist Summary

Print this and check off before each deployment:

```
PRE-DEPLOYMENT ESSENTIALS:
[ ] Uses DATABRICKS_APP_PORT
[ ] Binds to 0.0.0.0
[ ] Debug mode disabled
[ ] No hardcoded secrets
[ ] Dependencies pinned
[ ] app.yaml configured
[ ] Tested locally
[ ] Secrets created in Databricks
[ ] Code committed to git

POST-DEPLOYMENT VERIFICATION:
[ ] App accessible
[ ] No 502 errors
[ ] Logs reviewed
[ ] Functionality verified
```

---

**Last Updated**: 2025-11-22
**Version**: 1.0.0
