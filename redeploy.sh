#!/bin/bash
# Redeployment Script for lakebase-training-app
# ===============================================
# This script helps you redeploy the fixed app to Databricks

set -e  # Exit on error

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   Databricks App Redeployment - lakebase-training-app         ║"
echo "║   Fixed: 502 Bad Gateway Error                                 ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ Databricks CLI not found!"
    echo ""
    echo "Please install it first:"
    echo "  pip install databricks-cli"
    echo ""
    echo "Or configure authentication:"
    echo "  databricks configure --token"
    echo ""
    exit 1
fi

echo "✅ Databricks CLI found"
echo ""

# Check if we're on the correct branch
CURRENT_BRANCH=$(git branch --show-current)
EXPECTED_BRANCH="claude/debug-502-deployment-error-01QNx6HU8cGwMcmT5eSJrGDp"

echo "Current branch: $CURRENT_BRANCH"
if [ "$CURRENT_BRANCH" != "$EXPECTED_BRANCH" ]; then
    echo "⚠️  You're not on the fixed branch!"
    echo ""
    echo "Switching to: $EXPECTED_BRANCH"
    git checkout "$EXPECTED_BRANCH" || {
        echo "❌ Failed to switch branch"
        exit 1
    }
fi

echo "✅ On correct branch with fixes"
echo ""

# Show what was fixed
echo "📋 Changes in this deployment:"
echo "   • Fixed dash_app.py to use DATABRICKS_APP_PORT"
echo "   • Fixed dash_app_simple.py to use DATABRICKS_APP_PORT"
echo "   • Added comprehensive scaffolding for future deployments"
echo ""

# Deployment options
echo "🚀 Choose deployment method:"
echo ""
echo "1. Deploy using Databricks Asset Bundles (Recommended)"
echo "2. Deploy using Apps CLI directly"
echo "3. Show manual deployment instructions"
echo "4. Exit"
echo ""

read -p "Enter choice (1-4): " choice

case $choice in
    1)
        echo ""
        echo "📦 Deploying using Databricks Asset Bundles..."
        echo ""

        # Validate bundle first
        echo "Validating bundle configuration..."
        if databricks bundle validate; then
            echo "✅ Bundle configuration valid"
            echo ""

            # Deploy
            echo "Deploying to Databricks..."
            if databricks bundle deploy --target dev; then
                echo ""
                echo "✅ Deployment successful!"
                echo ""
                echo "Checking app status..."
                databricks apps get lakebase-training-app || true
                echo ""
                echo "🌐 Access your app at:"
                echo "   https://lakebase-training-app-1602460480284688.aws.databricksapps.com"
                echo ""
                echo "📊 View logs:"
                echo "   databricks apps logs lakebase-training-app --follow"
            else
                echo "❌ Deployment failed"
                exit 1
            fi
        else
            echo "❌ Bundle validation failed"
            exit 1
        fi
        ;;

    2)
        echo ""
        echo "🔧 Deploying using Apps CLI..."
        echo ""

        APP_NAME="lakebase-training-app"

        # Check if app exists
        if databricks apps get "$APP_NAME" &> /dev/null; then
            echo "App exists, updating..."
            if databricks apps deploy "$APP_NAME" --source-code-path .; then
                echo ""
                echo "✅ Deployment successful!"
                echo ""
                echo "Checking app status..."
                databricks apps get "$APP_NAME"
                echo ""
                echo "🌐 Access your app at:"
                echo "   https://lakebase-training-app-1602460480284688.aws.databricksapps.com"
            else
                echo "❌ Deployment failed"
                exit 1
            fi
        else
            echo "App doesn't exist. Creating..."
            if databricks apps create "$APP_NAME" --description "Lakebase Training Dashboard - Fixed 502"; then
                echo "✅ App created"
                echo ""
                echo "Deploying code..."
                if databricks apps deploy "$APP_NAME" --source-code-path .; then
                    echo ""
                    echo "✅ Deployment successful!"
                else
                    echo "❌ Deployment failed"
                    exit 1
                fi
            else
                echo "❌ Failed to create app"
                exit 1
            fi
        fi
        ;;

    3)
        echo ""
        echo "📖 Manual Deployment Instructions:"
        echo ""
        echo "Method 1: Using Databricks CLI"
        echo "────────────────────────────────"
        echo "databricks bundle deploy --target dev"
        echo ""
        echo "Method 2: Using Apps CLI"
        echo "────────────────────────"
        echo "databricks apps deploy lakebase-training-app --source-code-path ."
        echo ""
        echo "Method 3: Using Databricks Repos"
        echo "─────────────────────────────────"
        echo "1. Go to Databricks Workspace → Repos"
        echo "2. Navigate to: /Repos/suryasai.turaga@databricks.com/lakebase-training-app"
        echo "3. Click Git → Pull"
        echo "4. Select branch: $EXPECTED_BRANCH"
        echo "5. Redeploy the app from Databricks Apps UI"
        echo ""
        echo "Method 4: Using Workspace Files"
        echo "────────────────────────────────"
        echo "1. Go to Databricks Workspace"
        echo "2. Navigate to: /Workspace/Users/suryasai.turaga@databricks.com/.bundle/lakebase-training-app/dev/files"
        echo "3. Upload the fixed files: dash_app.py, dash_app_simple.py"
        echo "4. Redeploy from Databricks Apps UI"
        echo ""
        echo "For detailed instructions, see: REDEPLOYMENT_GUIDE.md"
        ;;

    4)
        echo ""
        echo "👋 Exiting without deployment"
        exit 0
        ;;

    *)
        echo ""
        echo "❌ Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   Next Steps                                                   ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "1. Verify the app is accessible:"
echo "   https://lakebase-training-app-1602460480284688.aws.databricksapps.com"
echo ""
echo "2. Check for 502 errors (should be gone!)"
echo ""
echo "3. View logs:"
echo "   databricks apps logs lakebase-training-app --follow"
echo ""
echo "4. Look for this in logs:"
echo "   'Starting... on 0.0.0.0:8000' (NOT port 8080!)"
echo ""
echo "✨ Your app should now be working without 502 errors!"
echo ""
