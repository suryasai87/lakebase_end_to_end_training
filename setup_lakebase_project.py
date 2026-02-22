#!/usr/bin/env python3
"""
Lakebase Autoscaling Project Setup Script

Creates a Lakebase Autoscaling project with branches and compute endpoints
using the Databricks w.postgres SDK.

Usage:
    python setup_lakebase_project.py --project-id training-app
    python setup_lakebase_project.py --project-id training-app --create-dev-branch
    python setup_lakebase_project.py --project-id training-app --info
    python setup_lakebase_project.py --project-id training-app --resize --min-cu 1.0 --max-cu 4.0
"""

import argparse
import json
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Branch,
    BranchSpec,
    Duration,
    Endpoint,
    EndpointSpec,
    EndpointType,
    FieldMask,
    Project,
    ProjectSpec,
)


def get_workspace_client():
    """Initialize and return a Databricks WorkspaceClient."""
    try:
        w = WorkspaceClient()
        user = w.current_user.me()
        print(f"Authenticated as: {user.user_name}")
        return w
    except Exception as e:
        print(f"Failed to authenticate: {e}")
        print("Ensure Databricks CLI is configured: databricks configure")
        sys.exit(1)


def create_project(w, project_id, pg_version="17"):
    """Create a new Lakebase Autoscaling project."""
    print(f"\nCreating Lakebase Autoscaling project: {project_id}")
    print(f"  PostgreSQL version: {pg_version}")
    print("  This is a long-running operation, please wait...")

    try:
        operation = w.postgres.create_project(
            project=Project(
                spec=ProjectSpec(
                    display_name=f"Lakebase Training: {project_id}",
                    pg_version=pg_version,
                )
            ),
            project_id=project_id,
        )
        result = operation.wait()
        print(f"\n  Project created: {result.name}")
        print(f"  State: {result.status.state}")

        # The project auto-creates a 'production' branch with default compute
        print("  Auto-created: production branch + default compute endpoint")
        return result
    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            print(f"  Project '{project_id}' already exists, fetching details...")
            return w.postgres.get_project(name=f"projects/{project_id}")
        raise


def create_dev_branch(w, project_id, branch_id="development", ttl_days=7):
    """Create a development branch from production."""
    print(f"\nCreating branch '{branch_id}' from production...")
    print(f"  TTL: {ttl_days} days")

    try:
        operation = w.postgres.create_branch(
            parent=f"projects/{project_id}",
            branch=Branch(
                spec=BranchSpec(
                    source_branch=f"projects/{project_id}/branches/production",
                    ttl=Duration(seconds=ttl_days * 86400),
                )
            ),
            branch_id=branch_id,
        )
        result = operation.wait()
        print(f"  Branch created: {result.name}")
        return result
    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            print(f"  Branch '{branch_id}' already exists")
            return w.postgres.get_branch(
                name=f"projects/{project_id}/branches/{branch_id}"
            )
        raise


def resize_compute(w, project_id, branch_id="production", min_cu=0.5, max_cu=4.0):
    """Resize the compute endpoint's autoscaling range."""
    print(f"\nResizing compute on {project_id}/{branch_id}: {min_cu}-{max_cu} CU")

    # Validate constraint: max - min <= 8
    if max_cu - min_cu > 8:
        print(f"  ERROR: Autoscaling range cannot exceed 8 CU (got {max_cu - min_cu})")
        sys.exit(1)

    endpoints = list(
        w.postgres.list_endpoints(
            parent=f"projects/{project_id}/branches/{branch_id}"
        )
    )
    if not endpoints:
        print("  ERROR: No compute endpoints found")
        sys.exit(1)

    endpoint = endpoints[0]
    print(f"  Endpoint: {endpoint.name}")

    operation = w.postgres.update_endpoint(
        name=endpoint.name,
        endpoint=Endpoint(
            name=endpoint.name,
            spec=EndpointSpec(
                autoscaling_limit_min_cu=min_cu,
                autoscaling_limit_max_cu=max_cu,
            ),
        ),
        update_mask=FieldMask(
            field_mask=[
                "spec.autoscaling_limit_min_cu",
                "spec.autoscaling_limit_max_cu",
            ]
        ),
    )
    result = operation.wait()
    print(f"  Resized: {min_cu}-{max_cu} CU")
    return result


def get_connection_info(w, project_id, branch_id="production"):
    """Get connection details for a Lakebase Autoscaling endpoint."""
    print(f"\nConnection info for {project_id}/{branch_id}:")

    # Get endpoint
    endpoints = list(
        w.postgres.list_endpoints(
            parent=f"projects/{project_id}/branches/{branch_id}"
        )
    )
    if not endpoints:
        print("  ERROR: No compute endpoints found")
        return None

    endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
    host = endpoint.status.hosts.host

    # Generate credential
    cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
    username = w.current_user.me().user_name

    # Get compute details
    spec = endpoint.spec
    min_cu = getattr(spec, "autoscaling_limit_min_cu", "N/A")
    max_cu = getattr(spec, "autoscaling_limit_max_cu", "N/A")

    info = {
        "project_id": project_id,
        "branch_id": branch_id,
        "endpoint_name": endpoints[0].name,
        "host": host,
        "port": 5432,
        "database": "databricks_postgres",
        "username": username,
        "sslmode": "require",
        "autoscaling_min_cu": min_cu,
        "autoscaling_max_cu": max_cu,
        "token_expires_in": "1 hour",
    }

    print(f"  Host:     {host}")
    print(f"  Port:     5432")
    print(f"  Database: databricks_postgres")
    print(f"  Username: {username}")
    print(f"  SSL:      require")
    print(f"  Compute:  {min_cu}-{max_cu} CU (autoscaling)")
    print(f"  Token:    Generated (valid 1 hour)")
    print(f"\n  Connection string:")
    print(f'  "host={host} dbname=databricks_postgres user={username} password=<token> sslmode=require"')
    print(f"\n  Environment variables for app.yaml:")
    print(f'  LAKEBASE_PROJECT_ID="{project_id}"')
    print(f'  LAKEBASE_BRANCH_ID="{branch_id}"')

    return info


def list_projects(w):
    """List all Lakebase Autoscaling projects."""
    print("\nLakebase Autoscaling Projects:")
    projects = list(w.postgres.list_projects())
    if not projects:
        print("  No projects found")
        return

    for p in projects:
        state = getattr(p.status, "state", "UNKNOWN") if p.status else "UNKNOWN"
        print(f"  - {p.name} (state: {state})")

        # List branches
        try:
            branches = list(w.postgres.list_branches(parent=p.name))
            for b in branches:
                b_state = getattr(b.status, "state", "UNKNOWN") if b.status else "UNKNOWN"
                print(f"    - {b.name} (state: {b_state})")
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(
        description="Lakebase Autoscaling Project Setup"
    )
    parser.add_argument(
        "--project-id",
        default="training-app",
        help="Project ID (default: training-app)",
    )
    parser.add_argument(
        "--branch-id",
        default="production",
        help="Branch ID (default: production)",
    )
    parser.add_argument(
        "--create",
        action="store_true",
        help="Create a new project",
    )
    parser.add_argument(
        "--create-dev-branch",
        action="store_true",
        help="Create a development branch",
    )
    parser.add_argument(
        "--resize",
        action="store_true",
        help="Resize compute endpoint",
    )
    parser.add_argument("--min-cu", type=float, default=0.5, help="Min CU (default: 0.5)")
    parser.add_argument("--max-cu", type=float, default=4.0, help="Max CU (default: 4.0)")
    parser.add_argument(
        "--info",
        action="store_true",
        help="Show connection info",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all projects",
    )
    parser.add_argument(
        "--pg-version",
        default="17",
        help="PostgreSQL version (default: 17)",
    )

    args = parser.parse_args()
    w = get_workspace_client()

    if args.list:
        list_projects(w)
        return

    if args.create:
        create_project(w, args.project_id, args.pg_version)

    if args.create_dev_branch:
        create_dev_branch(w, args.project_id)

    if args.resize:
        resize_compute(w, args.project_id, args.branch_id, args.min_cu, args.max_cu)

    if args.info:
        get_connection_info(w, args.project_id, args.branch_id)

    # Default: create + info if no flags specified
    if not any([args.create, args.create_dev_branch, args.resize, args.info, args.list]):
        create_project(w, args.project_id, args.pg_version)
        get_connection_info(w, args.project_id, args.branch_id)


if __name__ == "__main__":
    main()
