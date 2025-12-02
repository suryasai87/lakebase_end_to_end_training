#!/bin/bash
# Run database setup with OAuth token from Databricks CLI

# Get token
TOKEN=$(databricks auth token --host https://fe-vm-hls-amer.cloud.databricks.com 2>/dev/null | /opt/homebrew/opt/python@3.9/bin/python3.9 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")

if [ -z "$TOKEN" ]; then
    echo "Failed to get OAuth token"
    exit 1
fi

echo "Token obtained successfully"

# Export as PGPASSWORD
export PGPASSWORD="$TOKEN"

# Run setup script
cd /tmp/lakebase_end_to_end_training
/opt/homebrew/opt/python@3.9/bin/python3.9 setup_lakebase_tables.py
