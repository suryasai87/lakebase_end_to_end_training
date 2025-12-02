#!/bin/bash
# Databricks App Deployment Commands
# ====================================
# Run these commands on your local machine or in a Databricks environment
# with the Databricks CLI properly configured

set -e  # Exit on error

echo "════════════════════════════════════════════════════════════"
echo "  Databricks App Deployment - lakebase-training-app"
echo "  Branch: claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp"
echo "════════════════════════════════════════════════════════════"
echo ""

# Step 1: Ensure we're on the correct branch
echo "Step 1: Checking out the fixed branch..."
git fetch origin
git checkout claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp
git pull origin claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp

echo "✅ On branch: $(git branch --show-current)"
echo ""

# Step 2: Show what changed
echo "Step 2: Changes in this deployment:"
echo "   📝 Commit c1a9260: Add redeployment guide and scripts"
echo "   📝 Commit 871ee2f: Add comprehensive Databricks Apps scaffolding"
echo "   📝 Commit 0be16fb: Fix port configuration (CRITICAL FIX)"
echo ""
echo "   Key changes:"
echo "   • dash_app.py: Uses DATABRICKS_APP_PORT (line 776)"
echo "   • dash_app_simple.py: Uses DATABRICKS_APP_PORT (line 207)"
echo ""

# Step 3: Validate bundle configuration
echo "Step 3: Validating bundle configuration..."
if databricks bundle validate; then
    echo "✅ Bundle configuration is valid"
else
    echo "❌ Bundle validation failed"
    echo "   Check databricks.yml for errors"
    exit 1
fi
echo ""

# Step 4: Deploy using Databricks Asset Bundles
echo "Step 4: Deploying to Databricks..."
echo "   Target: default"
echo "   App: lakebase-training-app"
echo ""

if databricks bundle deploy --target default; then
    echo ""
    echo "✅ DEPLOYMENT SUCCESSFUL!"
else
    echo ""
    echo "❌ Deployment failed"
    echo ""
    echo "Troubleshooting steps:"
    echo "1. Check you have permission to deploy apps"
    echo "2. Verify Databricks CLI is configured: databricks auth login"
    echo "3. Check workspace quota limits"
    echo "4. View bundle configuration: cat databricks.yml"
    exit 1
fi
echo ""

# Step 5: Get app status
echo "Step 5: Checking app status..."
sleep 3  # Give it a moment to start
databricks apps get lakebase-training-app || echo "⚠️  Could not get app status"
echo ""

# Step 6: Show logs
echo "Step 6: Recent app logs..."
echo "   Looking for: 'Port: 8000' (NOT port 8080)"
echo ""
databricks apps logs lakebase-training-app --lines 50 || echo "⚠️  Could not fetch logs"
echo ""

# Success message
echo "════════════════════════════════════════════════════════════"
echo "  ✨ DEPLOYMENT COMPLETE!"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "🌐 Access your app at:"
echo "   https://lakebase-training-app-1602460480284688.aws.databricksapps.com"
echo ""
echo "✅ What to verify:"
echo "   • No 502 Bad Gateway error"
echo "   • Dashboard loads successfully"
echo "   • Check logs show 'Port: 8000' (NOT 8080)"
echo ""
echo "📊 Monitor logs:"
echo "   databricks apps logs lakebase-training-app --follow"
echo ""
echo "🔍 Check app status:"
echo "   databricks apps get lakebase-training-app"
echo ""
echo "════════════════════════════════════════════════════════════"
