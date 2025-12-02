# 🚀 Deploy Now - Quick Instructions

Since the Databricks CLI cannot be configured in this environment, here are your deployment options:

---

## ⚡ Option 1: Run from Your Local Machine (Recommended)

### Prerequisites:
- Databricks CLI installed: `pip install databricks-cli`
- Configured with your workspace: `databricks configure --token`

### Commands to Run:

```bash
# 1. Clone/pull the repo
cd /path/to/lakebase-training-app
git fetch origin
git checkout claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp
git pull origin claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp

# 2. Deploy using the script
./deploy_commands.sh

# OR deploy manually:
databricks bundle validate
databricks bundle deploy --target dev
```

---

## 🌐 Option 2: Use Databricks Repos (Easiest!)

This is the fastest method if your workspace is connected to GitHub:

### Steps:

1. **Open Databricks Workspace**
   - Go to: https://e2-demo-field-eng.cloud.databricks.com/

2. **Navigate to Repos**
   - Click on **Repos** in the sidebar
   - Navigate to: `/Repos/suryasai.turaga@databricks.com/lakebase-training-app`

3. **Pull the Fixed Code**
   - Click the **Git** dropdown menu
   - Select **Pull**
   - Choose branch: `claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp`
   - Click **Pull**

4. **Redeploy the App**
   - Go to **Databricks Apps** in the sidebar
   - Find `lakebase-training-app`
   - Click **⋮** (three dots) → **Update** or **Redeploy**
   - Confirm the deployment

5. **Verify**
   - Visit: https://lakebase-training-app-1602460480284688.aws.databricksapps.com
   - Should load without 502 error!

---

## 📱 Option 3: Use Databricks UI Directly

### Steps:

1. **Go to Databricks Apps**
   - Navigate to: https://e2-demo-field-eng.cloud.databricks.com/
   - Click **Apps** in the sidebar

2. **Find Your App**
   - Locate `lakebase-training-app`

3. **Update Source**
   - Click **Settings** or **Configure**
   - Update source to point to:
     - **Repo**: `/Repos/suryasai.turaga@databricks.com/lakebase-training-app`
     - **Branch**: `claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp`
   - OR
     - **Workspace Path**: `/Workspace/Users/suryasai.turaga@databricks.com/.bundle/lakebase-training-app/dev/files`

4. **Deploy**
   - Click **Deploy** or **Update**
   - Wait for deployment to complete

---

## 💻 Option 4: Use Databricks CLI from Databricks Notebook

If you have access to a Databricks notebook:

```python
# In a Databricks notebook

# Install CLI
%pip install databricks-cli

# Deploy using system commands
import subprocess

# Pull latest code
subprocess.run([
    'git', 'clone', '--branch',
    'claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp',
    'https://github.com/suryasai87/lakebase-training-app.git'
])

# Change directory and deploy
%sh
cd lakebase-training-app
databricks bundle deploy --target dev
```

---

## 🔑 If You Need to Configure Databricks CLI

On your local machine:

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure with your workspace
databricks configure --token

# When prompted, enter:
# Databricks Host: https://e2-demo-field-eng.cloud.databricks.com/
# Token: [Your personal access token]

# Verify configuration
databricks workspace ls /

# Now you can deploy
cd /path/to/lakebase-training-app
git checkout claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp
databricks bundle deploy --target dev
```

### To get a Personal Access Token:
1. Go to: https://e2-demo-field-eng.cloud.databricks.com/
2. Click your email (top right) → **User Settings**
3. Go to **Developer** → **Access Tokens**
4. Click **Generate New Token**
5. Give it a name (e.g., "CLI Deployment")
6. Set lifetime (or leave default)
7. Click **Generate**
8. **Copy the token** (you won't see it again!)

---

## ✅ After Deployment - Verify

### 1. Check App is Running
```bash
databricks apps get lakebase-training-app
```

### 2. Check Logs
```bash
databricks apps logs lakebase-training-app --follow
```

Look for:
```
Starting Databricks Lakebase Training Dashboard...
Host: 0.0.0.0
Port: 8000  ← Should be 8000, NOT 8080!
```

### 3. Access the App
Visit: **https://lakebase-training-app-1602460480284688.aws.databricksapps.com**

Expected:
- ✅ No 502 Bad Gateway error
- ✅ Dashboard loads successfully
- ✅ All features working

---

## 🐛 If You Still Get 502

1. **Check the logs**:
   ```bash
   databricks apps logs lakebase-training-app | grep "Port:"
   ```
   Should show "Port: 8000"

2. **Verify the code was updated**:
   - Check `dash_app.py` line 776
   - Should see: `port = int(os.environ.get('DATABRICKS_APP_PORT', ...))`

3. **Restart the app**:
   ```bash
   databricks apps restart lakebase-training-app
   ```

4. **Check environment**:
   ```bash
   databricks apps get lakebase-training-app
   ```
   Look for `DATABRICKS_APP_PORT` in environment variables

---

## 📞 Quick Commands Reference

```bash
# Deploy
databricks bundle deploy --target dev

# Check status
databricks apps get lakebase-training-app

# View logs
databricks apps logs lakebase-training-app --follow

# Restart
databricks apps restart lakebase-training-app

# List all apps
databricks apps list

# Get app URL
databricks apps get lakebase-training-app | grep url
```

---

## 🎯 Recommended: Use Option 2 (Databricks Repos)

**This is the fastest and easiest method!**

1. Go to Databricks workspace
2. Repos → your repo → Git → Pull
3. Select branch: `claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp`
4. Apps → lakebase-training-app → Redeploy
5. Done! ✨

---

**All the fixes are ready and committed. Just pull and redeploy using any method above!**

Branch: `claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp`
Status: ✅ Ready to deploy
