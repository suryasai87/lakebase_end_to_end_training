#!/usr/bin/env python3
"""
Databricks App Deployment Script for Lakebase Training Dashboard
"""

import os
import sys
import json
import subprocess
from pathlib import Path

def run_command(cmd, capture_output=True):
    """Run a shell command and return output"""
    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=capture_output,
        text=True
    )
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return None
    return result.stdout

def deploy_to_workspace():
    """Deploy app files to Databricks workspace"""

    print("=" * 80)
    print("Databricks Lakebase Training App Deployment")
    print("=" * 80)

    # Get current directory
    app_dir = Path.cwd()
    workspace_path = "/Workspace/Users/suryasai.turaga@databricks.com/lakebase-training-app"

    print(f"\n1. Creating workspace directory: {workspace_path}")
    run_command(f'databricks workspace mkdirs "{workspace_path}"')

    print(f"\n2. Uploading application files...")

    # Upload files
    files_to_upload = [
        "app.py",
        "app.yaml",
        "requirements.txt",
        "README.md"
    ]

    for file in files_to_upload:
        file_path = app_dir / file
        if file_path.exists():
            print(f"   Uploading {file}...")
            result = run_command(
                f'databricks workspace import "{workspace_path}/{file}" '
                f'--file "{file_path}" --overwrite'
            )
            if result is not None:
                print(f"   ✓ {file} uploaded successfully")
        else:
            print(f"   ⚠ Warning: {file} not found")

    print(f"\n3. App files deployed to workspace!")
    print(f"\n   Workspace Location: {workspace_path}")
    print(f"   GitHub Repository: https://github.com/suryasai87/lakebase-training-app")

    print("\n" + "=" * 80)
    print("Deployment Summary")
    print("=" * 80)
    print("\nDue to the workspace app limit (300 apps), the files have been uploaded")
    print("to the workspace. You can:")
    print("\n1. Create the app manually from the Databricks UI:")
    print(f"   - Navigate to: {workspace_path}")
    print("   - Use the Databricks Apps interface to create a new app")
    print("   - Point to the uploaded files")
    print("\n2. Or delete an existing unused app and run:")
    print("   databricks apps create lakebase-training-dashboard")

    print("\n3. Alternative: Use Databricks Notebooks or Repos:")
    print("   - Import from GitHub: https://github.com/suryasai87/lakebase-training-app")

    return True

if __name__ == "__main__":
    try:
        success = deploy_to_workspace()
        if success:
            print("\n✅ Deployment completed successfully!")
            sys.exit(0)
        else:
            print("\n❌ Deployment failed")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ Deployment error: {e}")
        sys.exit(1)
