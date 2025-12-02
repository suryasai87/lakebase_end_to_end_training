# 🚀 Redeployment Guide - lakebase-training-app

## ✅ Changes Made (Already Committed)

Your app has been fixed and committed to branch: `claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp`

**Critical Fix**: Updated port configuration to use `DATABRICKS_APP_PORT` instead of hardcoded port 8080.

---

## 📋 Redeployment Options

Choose the method that works best for your setup:

### **Option 1: Using Databricks Asset Bundles (Recommended)**

This is the fastest method if you have the Databricks CLI installed.

```bash
# Step 1: Ensure you're on the correct branch
git checkout claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp

# Step 2: Deploy using bundles
databricks bundle deploy --target dev

# Step 3: Monitor the deployment
databricks apps get lakebase-training-app

# Step 4: View logs
databricks apps logs lakebase-training-app --follow
```

---

### **Option 2: Using Databricks Apps CLI Directly**

If you know the app name:

```bash
# Step 1: Check if app exists
databricks apps list | grep lakebase-training

# Step 2: Deploy/Update the app
databricks apps deploy lakebase-training-app \
  --source-code-path /Workspace/Users/suryasai.turaga@databricks.com/.bundle/lakebase-training-app/dev/files

# Alternative: If the app doesn't exist, create it first
databricks apps create lakebase-training-app \
  --description "Lakebase Training Dashboard - Fixed 502 Error"

# Then deploy
databricks apps deploy lakebase-training-app --source-code-path .

# Step 3: Get app URL
databricks apps get lakebase-training-app
```

---

### **Option 3: Using Databricks Repos (GitHub Integration)**

If your workspace is connected to GitHub:

```bash
# Step 1: In Databricks Workspace, go to Repos
# Navigate to: /Repos/suryasai.turaga@databricks.com/lakebase-training-app

# Step 2: Pull the latest changes
# Click "Pull" or use Git menu → Pull changes
# Select branch: claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp

# Step 3: Redeploy the app
# If using Databricks Apps:
databricks apps deploy lakebase-training-app \
  --source-code-path /Repos/suryasai.turaga@databricks.com/lakebase-training-app
```

---

### **Option 4: Using Databricks Workspace UI**

If you prefer the UI:

1. **Navigate to Databricks Workspace**
   - Go to: https://e2-demo-field-eng.cloud.databricks.com/ (your workspace URL)

2. **Update Files in Workspace**
   - Navigate to: `/Workspace/Users/suryasai.turaga@databricks.com/.bundle/lakebase-training-app/dev/files`
   - Or use Repos to sync from GitHub

3. **Redeploy App**
   - Go to Databricks Apps section
   - Find `lakebase-training-app`
   - Click "Redeploy" or "Update"
   - Point to the source directory

4. **Verify Deployment**
   - Check app status
   - Visit: https://lakebase-training-app-1602460480284688.aws.databricksapps.com

---

### **Option 5: Automated Deployment Script**

Use the included deployment script:

```bash
# From your local machine with Databricks CLI configured

# Step 1: Checkout the fixed branch
git checkout claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp

# Step 2: Run deployment script
python deploy.py

# Or use the bundle deployment
databricks bundle deploy
```

---

## 🔍 Verify the Fix

After redeployment, verify the fix is working:

### 1. Check App Status
```bash
databricks apps get lakebase-training-app
```

Expected output should show:
- Status: RUNNING
- No errors in status

### 2. Check App Logs
```bash
databricks apps logs lakebase-training-app --follow
```

Look for:
```
Starting Databricks Lakebase Training Dashboard - Dash Application with Framer Motion UI
Host: 0.0.0.0
Port: 8000  # Should be 8000, not 8080!
```

### 3. Access the App
Visit: https://lakebase-training-app-1602460480284688.aws.databricksapps.com

You should see:
- ✅ No 502 Bad Gateway error
- ✅ Dashboard loads successfully
- ✅ App is responsive

---

## 🐛 Troubleshooting

### If you still get 502 error:

1. **Check logs for the actual port**
   ```bash
   databricks apps logs lakebase-training-app | grep "Port:"
   ```
   Should show "Port: 8000" (or whatever DATABRICKS_APP_PORT is set to)

2. **Verify the code was updated**
   ```bash
   # In the workspace, check dash_app.py line 773
   # Should see: port = int(os.environ.get('DATABRICKS_APP_PORT', ...))
   ```

3. **Check environment variables**
   ```bash
   databricks apps get lakebase-training-app
   ```
   Look for DATABRICKS_APP_PORT in the environment section

4. **Restart the app**
   ```bash
   databricks apps restart lakebase-training-app
   ```

### If deployment fails:

1. **Check app exists**
   ```bash
   databricks apps list
   ```

2. **Check permissions**
   - Ensure you have permission to deploy apps
   - Check workspace access

3. **Check bundle configuration**
   ```bash
   databricks bundle validate
   ```

---

## 📊 What Changed

### Files Modified:
1. **dash_app.py** (line 773)
   ```python
   # Before:
   app.run_server(debug=True, host='0.0.0.0', port=8080)

   # After:
   port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))
   app.run_server(debug=True, host='0.0.0.0', port=port)
   ```

2. **dash_app_simple.py** (line 204)
   - Same fix as above

### Why This Fixes the 502 Error:
- Databricks Apps sets `DATABRICKS_APP_PORT` environment variable (typically to 8000)
- The reverse proxy expects the app to listen on that port
- Hardcoded port 8080 caused a mismatch → 502 error
- Now the app dynamically reads the correct port → no more 502!

---

## ✅ Post-Deployment Checklist

After redeployment, verify:

- [ ] App status is RUNNING (not ERROR or STOPPED)
- [ ] No 502 error when accessing the URL
- [ ] Dashboard loads and displays correctly
- [ ] Can navigate between tabs
- [ ] Database connection works (if configured)
- [ ] No errors in app logs

---

## 🎯 Quick Commands Reference

```bash
# Deploy
databricks bundle deploy

# Check status
databricks apps get lakebase-training-app

# View logs
databricks apps logs lakebase-training-app

# Restart app
databricks apps restart lakebase-training-app

# List all apps
databricks apps list

# Delete app (if needed)
databricks apps delete lakebase-training-app
```

---

## 📞 Need Help?

If you encounter issues:

1. **Check the logs first**
   ```bash
   databricks apps logs lakebase-training-app
   ```

2. **Verify the fix is deployed**
   - Check that dash_app.py has the updated port configuration
   - Look for "DATABRICKS_APP_PORT" in the code

3. **Review the scaffolding docs**
   - See: `dbapps/docs/DEPLOYMENT_CHECKLIST.md`
   - See: `dbapps/docs/TROUBLESHOOTING.md`

---

## 🎉 Success Indicators

When successful, you should see:
- App URL accessible without 502 error
- Dashboard displays correctly
- Logs show: "Starting... on 0.0.0.0:8000" (or the correct DATABRICKS_APP_PORT)
- App status: RUNNING

---

**Your fixed code is ready to deploy! Choose the method that works best for you above.**

**Branch**: `claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp`
**Commits**: 2 (port fix + scaffolding)
**Status**: ✅ Ready to deploy

---

Last Updated: 2025-11-22
