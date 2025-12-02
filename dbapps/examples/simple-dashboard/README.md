# Simple Dashboard Example

A complete, working example of a Dash dashboard deployed on Databricks Apps.

## ✨ Features

- 📊 Interactive dashboard with charts
- 📈 Real-time metric cards
- 🎨 Bootstrap styling
- ✅ Production-ready configuration
- ✅ Proper port configuration (no 502 errors!)

## 🚀 Quick Start

### 1. Local Testing
```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
export DATABRICKS_APP_PORT=8080
python app.py

# Open browser to http://localhost:8080
```

### 2. Deploy to Databricks
```bash
# Create app
databricks apps create simple-dashboard --description "Simple Dashboard Example"

# Deploy app
databricks apps deploy simple-dashboard --source-code-path .

# View app
databricks apps get simple-dashboard

# Check logs
databricks apps logs simple-dashboard
```

## 📂 Files

- `app.py` - Main application with proper port configuration
- `app.yaml` - Databricks deployment configuration
- `requirements.txt` - Python dependencies
- `README.md` - This file

## 🔧 Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABRICKS_APP_PORT` | 8080 | Port to run the app on (set by Databricks) |
| `APP_TITLE` | "Simple Dashboard" | Application title |
| `DEBUG` | "false" | Debug mode (should be false in production) |

### Customization

Edit `app.yaml` to change environment variables:

```yaml
env:
  - name: APP_TITLE
    value: "My Custom Dashboard"
```

## 📊 What This Example Demonstrates

1. **Proper Port Configuration**
   - Uses `DATABRICKS_APP_PORT` environment variable
   - Avoids hardcoded ports
   - Works in both local and Databricks environments

2. **Host Binding**
   - Binds to `0.0.0.0` for Databricks proxy compatibility
   - NOT `localhost` or `127.0.0.1`

3. **Environment Variables**
   - Reads configuration from environment
   - Provides sensible defaults
   - Secure and flexible

4. **Production Ready**
   - Debug mode disabled in production
   - Proper logging
   - Clean code structure

## 🎓 Learning Points

### The Critical Port Configuration
```python
# This is the key to avoiding 502 errors!
port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))
app.run_server(debug=False, host='0.0.0.0', port=port)
```

### Why This Works
1. `DATABRICKS_APP_PORT` is set by Databricks Apps (typically 8000)
2. Falls back to `PORT` for other cloud platforms
3. Defaults to `8080` for local development
4. Host `0.0.0.0` allows Databricks proxy to connect

### Common Mistakes to Avoid
```python
# ❌ DON'T DO THIS - Will cause 502 errors
app.run_server(debug=True, host='0.0.0.0', port=8080)
app.run_server(debug=True, host='localhost', port=8080)

# ✅ DO THIS - Works perfectly
port = int(os.environ.get('DATABRICKS_APP_PORT', '8080'))
app.run_server(debug=False, host='0.0.0.0', port=port)
```

## 🐛 Troubleshooting

### 502 Bad Gateway Error
- Check that `DATABRICKS_APP_PORT` is being used
- Verify host is `0.0.0.0`
- Review app logs: `databricks apps logs simple-dashboard`

### App Not Starting
- Verify all dependencies are installed
- Check for Python syntax errors
- Review environment variables in `app.yaml`

## 📚 Next Steps

1. Customize the dashboard with your own data
2. Add database connections (see templates for examples)
3. Deploy to production
4. Monitor with Databricks Apps logs

## 🔗 Related Resources

- [Main Templates](../../templates/)
- [Best Practices](../../docs/BEST_PRACTICES.md)
- [Deployment Checklist](../../docs/DEPLOYMENT_CHECKLIST.md)
- [Quick Reference](../../docs/QUICK_REFERENCE.md)

---

**This example is production-ready and can be used as a starting point for your own Databricks Apps!**
