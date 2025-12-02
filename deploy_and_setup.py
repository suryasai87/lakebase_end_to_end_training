#!/usr/bin/env python3
"""
Deploy and Setup Script for Lakebase End-to-End Training App
This script:
1. Sets up the database schema with audit trail
2. Populates sample data
3. Deploys the app to Databricks Apps
"""

import os
import sys
import subprocess
import time

# Lakebase connection configuration
LAKEBASE_CONFIG = {
    'host': 'instance-868832b3-5ee5-4d06-a412-b5d13e28d853.database.cloud.databricks.com',
    'database': 'databricks_postgres',
    'user': 'token',
    'port': 5432,
    'sslmode': 'require'
}

def get_oauth_token():
    """Get OAuth token from Databricks SDK"""
    try:
        from databricks import sdk
        workspace_client = sdk.WorkspaceClient()
        token = workspace_client.config.oauth_token().access_token
        print(f"✅ OAuth token obtained (expires in ~1 hour)")
        return token
    except Exception as e:
        print(f"❌ Failed to get OAuth token: {e}")
        return None

def setup_database(token):
    """Set up the database schema with audit trail"""
    import psycopg
    from psycopg.rows import dict_row

    print("\n" + "=" * 80)
    print("Setting Up Lakebase Database with Audit Trail")
    print("=" * 80)

    conn_string = (
        f"dbname={LAKEBASE_CONFIG['database']} "
        f"user={LAKEBASE_CONFIG['user']} "
        f"password={token} "
        f"host={LAKEBASE_CONFIG['host']} "
        f"port={LAKEBASE_CONFIG['port']} "
        f"sslmode={LAKEBASE_CONFIG['sslmode']}"
    )

    try:
        conn = psycopg.connect(conn_string)
        cursor = conn.cursor()

        # Read and execute setup SQL
        with open('setup_database.sql', 'r') as f:
            sql_script = f.read()

        print("\n📝 Creating schema, tables, audit log, and triggers...")

        # Split by statements and execute
        statements = sql_script.split(';')
        for stmt in statements:
            stmt = stmt.strip()
            if stmt and not stmt.startswith('--'):
                try:
                    cursor.execute(stmt)
                    conn.commit()
                except Exception as e:
                    if 'already exists' in str(e).lower() or 'duplicate' in str(e).lower():
                        conn.rollback()
                        continue
                    print(f"   ⚠️ Statement warning: {str(e)[:100]}")
                    conn.rollback()

        # Verify setup
        cursor.execute("""
            SELECT
                (SELECT COUNT(*) FROM ecommerce.users) as users,
                (SELECT COUNT(*) FROM ecommerce.products) as products,
                (SELECT COUNT(*) FROM ecommerce.orders) as orders,
                (SELECT COUNT(*) FROM ecommerce.audit_log) as audit_entries
        """)
        result = cursor.fetchone()

        print(f"\n✅ Database setup completed!")
        print(f"   - Users: {result[0]}")
        print(f"   - Products: {result[1]}")
        print(f"   - Orders: {result[2]}")
        print(f"   - Audit Log Entries: {result[3]}")

        # Verify triggers
        cursor.execute("""
            SELECT trigger_name, event_manipulation
            FROM information_schema.triggers
            WHERE trigger_schema = 'ecommerce'
            ORDER BY trigger_name
        """)
        triggers = cursor.fetchall()
        print(f"\n📋 Triggers installed: {len(triggers)}")
        for t in triggers:
            print(f"   - {t[0]} ({t[1]})")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return False

def deploy_to_workspace():
    """Deploy app files to Databricks workspace"""
    print("\n" + "=" * 80)
    print("Deploying App to Databricks Workspace")
    print("=" * 80)

    workspace_path = "/Workspace/Users/suryasai.turaga@databricks.com/lakebase-end-to-end-training"

    # Create workspace directory
    print(f"\n1. Creating workspace directory: {workspace_path}")
    result = subprocess.run(
        f'databricks workspace mkdirs "{workspace_path}"',
        shell=True, capture_output=True, text=True
    )

    # Upload files
    files_to_upload = [
        "dash_app.py",
        "app.yaml",
        "requirements.txt",
        "setup_database.sql",
        "README.md"
    ]

    print(f"\n2. Uploading application files...")
    for file in files_to_upload:
        if os.path.exists(file):
            result = subprocess.run(
                f'databricks workspace import "{workspace_path}/{file}" --file "{file}" --overwrite',
                shell=True, capture_output=True, text=True
            )
            if result.returncode == 0:
                print(f"   ✓ {file} uploaded")
            else:
                print(f"   ⚠️ {file}: {result.stderr[:100] if result.stderr else 'uploaded with warnings'}")

    return workspace_path

def create_or_update_app(workspace_path):
    """Create or update the Databricks App"""
    print("\n" + "=" * 80)
    print("Creating/Updating Databricks App")
    print("=" * 80)

    app_name = "lakebase-end-to-end-training"

    # Check if app exists
    result = subprocess.run(
        f'databricks apps get {app_name}',
        shell=True, capture_output=True, text=True
    )

    if result.returncode == 0:
        print(f"\n📱 App '{app_name}' already exists. Updating deployment...")
        # Deploy update
        result = subprocess.run(
            f'databricks apps deploy {app_name} --source-code-path "{workspace_path}"',
            shell=True, capture_output=True, text=True
        )
    else:
        print(f"\n📱 Creating new app '{app_name}'...")
        result = subprocess.run(
            f'databricks apps create {app_name} --description "End-to-End Lakebase Training Dashboard" --source-code-path "{workspace_path}"',
            shell=True, capture_output=True, text=True
        )

    if result.returncode == 0:
        print(f"✅ App deployment initiated!")
        app_url = f"https://{app_name}-1602460480284688.aws.databricksapps.com"
        print(f"\n🌐 App URL: {app_url}")
        return app_url
    else:
        print(f"⚠️ Deployment result: {result.stdout or result.stderr}")
        return None

def main():
    print("\n" + "=" * 80)
    print("Lakebase End-to-End Training App - Deployment Script")
    print("=" * 80)

    # Step 1: Get OAuth token
    print("\n📋 Step 1: Getting OAuth token...")
    token = get_oauth_token()
    if not token:
        print("❌ Cannot proceed without OAuth token")
        return False

    # Step 2: Setup database
    print("\n📋 Step 2: Setting up database...")
    if not setup_database(token):
        print("⚠️ Database setup had issues, but continuing with deployment...")

    # Step 3: Deploy to workspace
    print("\n📋 Step 3: Deploying to workspace...")
    workspace_path = deploy_to_workspace()

    # Step 4: Create/Update app
    print("\n📋 Step 4: Creating/Updating app...")
    app_url = create_or_update_app(workspace_path)

    # Summary
    print("\n" + "=" * 80)
    print("Deployment Summary")
    print("=" * 80)
    print(f"\n✅ Workspace files: {workspace_path}")
    if app_url:
        print(f"✅ App URL: {app_url}")
    print("\n⏳ Note: App may take 1-2 minutes to start after deployment")
    print("   Check status with: databricks apps get lakebase-end-to-end-training")

    return True

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
