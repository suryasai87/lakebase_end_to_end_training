"""
Databricks Lakebase Autoscaling End-to-End Training Dashboard
A comprehensive training application showcasing Lakebase Autoscaling features:
- Project/Branch/Endpoint resource hierarchy
- Autoscaling compute (0.5-112 CU) with scale-to-zero
- OAuth credential generation via w.postgres SDK
- Create Order with Transaction Handling
- Real-Time Insert Verification Panel
- Audit Trail with PostgreSQL Triggers
- Bulk CSV Import
"""

import os
import json
import subprocess
import socket
import dash
from dash import dcc, html, Input, Output, State, callback, dash_table
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import plotly.express as px
import plotly.graph_objects as go
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import pandas as pd
from datetime import datetime
import time
import base64
import io
from databricks.sdk import WorkspaceClient

# ========================================
# Lakebase Autoscaling Configuration
# ========================================
LAKEBASE_PROJECT_ID = os.getenv('LAKEBASE_PROJECT_ID', '')
LAKEBASE_BRANCH_ID = os.getenv('LAKEBASE_BRANCH_ID', 'production')
PGDATABASE = os.getenv('PGDATABASE', 'databricks_postgres')
PGPORT = os.getenv('PGPORT', '5432')

# Legacy support: if PGHOST is set directly, use it (for provisioned or pre-resolved endpoints)
PGHOST_OVERRIDE = os.getenv('PGHOST', '')
PGUSER_OVERRIDE = os.getenv('PGUSER', '')

# ========================================
# Lakebase Autoscaling Credential Manager
# ========================================
_workspace_client = None
_cached_token = None
_cached_host = None
_cached_username = None
_cached_endpoint_name = None
_last_token_refresh = 0
TOKEN_REFRESH_INTERVAL = 3000  # 50 minutes (tokens expire in 1 hour)

def _get_workspace_client():
    """Get or create the WorkspaceClient singleton."""
    global _workspace_client
    if _workspace_client is None:
        _workspace_client = WorkspaceClient()
    return _workspace_client


def _resolve_hostname(hostname):
    """Resolve hostname with macOS DNS workaround using dig."""
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        pass
    try:
        result = subprocess.run(
            ["dig", "+short", hostname],
            capture_output=True, text=True, timeout=5,
        )
        ips = result.stdout.strip().split('\n')
        for ip in ips:
            if ip and not ip.startswith(';') and '.' in ip:
                return ip
    except Exception:
        pass
    return None


def _discover_endpoint():
    """Discover the Lakebase Autoscaling endpoint host and username."""
    global _cached_host, _cached_username, _cached_endpoint_name

    w = _get_workspace_client()

    # If LAKEBASE_PROJECT_ID is set, discover the endpoint name for credential generation
    if LAKEBASE_PROJECT_ID:
        try:
            endpoints = list(w.postgres.list_endpoints(
                parent=f"projects/{LAKEBASE_PROJECT_ID}/branches/{LAKEBASE_BRANCH_ID}"
            ))
            if endpoints:
                _cached_endpoint_name = endpoints[0].name
                endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
                _cached_host = PGHOST_OVERRIDE or endpoint.status.hosts.host
                _cached_username = PGUSER_OVERRIDE or w.current_user.me().user_name
                min_cu = getattr(endpoint.spec, 'autoscaling_limit_min_cu', 'N/A')
                max_cu = getattr(endpoint.spec, 'autoscaling_limit_max_cu', 'N/A')
                print(f"Autoscaling endpoint: {_cached_host} ({min_cu}-{max_cu} CU)")
                return
            else:
                print(f"Warning: No endpoints found for project={LAKEBASE_PROJECT_ID}")
        except Exception as e:
            print(f"Warning: Autoscaling discovery failed: {e}")

    # Fallback: use explicit PGHOST (provisioned or pre-resolved)
    if PGHOST_OVERRIDE:
        _cached_host = PGHOST_OVERRIDE
        _cached_username = PGUSER_OVERRIDE or w.current_user.me().user_name
        _cached_endpoint_name = None
        print(f"Using explicit host: {_cached_host}")
        return

    raise ValueError(
        "LAKEBASE_PROJECT_ID or PGHOST must be set. "
        "Run: python setup_lakebase_project.py --project-id <your-project> --info"
    )


def _generate_credential():
    """Generate a fresh OAuth token for the Lakebase endpoint."""
    global _cached_token, _last_token_refresh

    now = time.time()
    if _cached_token and (now - _last_token_refresh) < TOKEN_REFRESH_INTERVAL:
        return _cached_token

    print("Generating Lakebase credential...")
    w = _get_workspace_client()

    if _cached_endpoint_name:
        # Autoscaling mode: use w.postgres.generate_database_credential
        cred = w.postgres.generate_database_credential(endpoint=_cached_endpoint_name)
        _cached_token = cred.token
    else:
        # Legacy/provisioned mode: use workspace OAuth token
        _cached_token = w.config.oauth_token().access_token

    _last_token_refresh = time.time()
    print("Credential generated successfully")
    return _cached_token


def get_db_connection(retries=3):
    """Get a fresh database connection with retry for scale-to-zero cold starts."""
    if not _cached_host:
        _discover_endpoint()

    token = _generate_credential()
    host = _cached_host
    username = _cached_username

    # Resolve IP for macOS DNS workaround
    hostaddr = _resolve_hostname(host)

    conn_params = {
        "host": host,
        "dbname": PGDATABASE,
        "user": username,
        "password": token,
        "port": PGPORT,
        "sslmode": "require",
    }
    if hostaddr:
        conn_params["hostaddr"] = hostaddr

    for attempt in range(retries):
        try:
            return psycopg.connect(
                **conn_params,
                row_factory=dict_row,
                connect_timeout=10,
            )
        except Exception as e:
            if attempt < retries - 1:
                wait = (attempt + 1) * 2
                print(f"Connection attempt {attempt + 1} failed ({e}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


# Initialize endpoint discovery at startup
try:
    _discover_endpoint()
except Exception as e:
    print(f"Warning: Endpoint discovery failed at startup: {e}")
    print("Will retry on first connection attempt.")


# ========================================
# Database Connection Manager
# ========================================
class LakebaseConnection:
    """Manage Lakebase Autoscaling connections with credential refresh and retry."""

    def __init__(self):
        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """Establish connection to Lakebase Autoscaling endpoint."""
        self.connection = get_db_connection()
        self.cursor = self.connection.cursor()
        return True

    def execute_query(self, query, params=None):
        """Execute a query and return results."""
        if not self.connection or not self.cursor:
            raise Exception("Database connection not established")
        try:
            self.cursor.execute(query, params)
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            elif 'RETURNING' in query.upper():
                result = self.cursor.fetchall()
                self.connection.commit()
                return result
            else:
                self.connection.commit()
                return self.cursor.rowcount
        except Exception as e:
            if self.connection:
                try:
                    self.connection.rollback()
                except Exception:
                    pass
            raise e

    def close(self):
        """Close database connection."""
        try:
            if self.cursor:
                self.cursor.close()
        except Exception:
            pass
        try:
            if self.connection:
                self.connection.close()
        except Exception:
            pass
        self.cursor = None
        self.connection = None

# Helper function to convert JSONB for DataTable display
def convert_for_datatable(df):
    """Convert JSONB columns to strings for DataTable display."""
    if df.empty:
        return df
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df

# ========================================
# Initialize Dash App with Bootstrap and custom CSS
# ========================================
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css'
    ],
    suppress_callback_exceptions=True,
    title="Lakebase End-to-End Training"
)

# Custom CSS for animations
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            @keyframes fadeIn {
                from { opacity: 0; transform: translateY(20px); }
                to { opacity: 1; transform: translateY(0); }
            }

            @keyframes pulse {
                0% { box-shadow: 0 0 0 0 rgba(102, 126, 234, 0.4); }
                70% { box-shadow: 0 0 0 10px rgba(102, 126, 234, 0); }
                100% { box-shadow: 0 0 0 0 rgba(102, 126, 234, 0); }
            }

            .animate-fade-in {
                animation: fadeIn 0.6s ease-out;
            }

            .animate-pulse {
                animation: pulse 2s infinite;
            }

            .metric-card {
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                border-radius: 12px;
                padding: 24px;
                background: white;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }

            .metric-card:hover {
                transform: translateY(-5px);
                box-shadow: 0 10px 25px rgba(0, 0, 0, 0.15);
            }

            .chart-container {
                background: white;
                border-radius: 12px;
                padding: 20px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                margin-bottom: 20px;
            }

            .form-container {
                background: white;
                border-radius: 12px;
                padding: 30px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }

            .live-indicator {
                display: inline-block;
                width: 10px;
                height: 10px;
                background-color: #28a745;
                border-radius: 50%;
                margin-right: 8px;
                animation: pulse 1.5s infinite;
            }

            .nav-tabs .nav-link.active {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white !important;
                border: none;
            }

            .btn-primary {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                border: none;
            }

            .btn-primary:hover {
                background: linear-gradient(135deg, #5a6fd6 0%, #6a4190 100%);
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# ========================================
# App Layout
# ========================================
app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.H1([
                html.I(className="fas fa-database me-3"),
                "Lakebase End-to-End Training"
            ], className="text-primary mb-2"),
            html.P("Advanced Databricks Lakebase Dashboard with Audit Trail, Orders & Bulk Import",
                   className="text-muted lead"),
            html.Hr()
        ])
    ], className="mt-4 mb-4 animate-fade-in"),

    # Connection status
    dbc.Row([
        dbc.Col([
            html.Div(id="connection-status")
        ])
    ], className="mb-3"),

    # Navigation Tabs
    dbc.Tabs([
        dbc.Tab(label="Dashboard", tab_id="dashboard", className="p-3"),
        dbc.Tab(label="Data Entry", tab_id="data-entry", className="p-3"),
        dbc.Tab(label="Create Order", tab_id="create-order", className="p-3"),
        dbc.Tab(label="Bulk Import", tab_id="bulk-import", className="p-3"),
        dbc.Tab(label="Audit Trail", tab_id="audit-trail", className="p-3"),
        dbc.Tab(label="Query Builder", tab_id="query-builder", className="p-3"),
    ], id="tabs", active_tab="dashboard", className="mb-4"),

    # Tab Content
    html.Div(id="tab-content"),

    # Auto-refresh interval
    dcc.Interval(id="interval-component", interval=30000, n_intervals=0),

], fluid=True, className="pb-5")

# ========================================
# Dashboard Content
# ========================================
def get_dashboard_content():
    return html.Div([
        # Metrics Row
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Div([
                        html.I(className="fas fa-users fa-2x text-primary mb-2"),
                        html.H3(id="metric-users", className="mb-0"),
                        html.P("Total Users", className="text-muted mb-0")
                    ], className="text-center")
                ], className="metric-card animate-fade-in")
            ], width=3),
            dbc.Col([
                html.Div([
                    html.Div([
                        html.I(className="fas fa-box fa-2x text-success mb-2"),
                        html.H3(id="metric-products", className="mb-0"),
                        html.P("Products", className="text-muted mb-0")
                    ], className="text-center")
                ], className="metric-card animate-fade-in")
            ], width=3),
            dbc.Col([
                html.Div([
                    html.Div([
                        html.I(className="fas fa-shopping-cart fa-2x text-info mb-2"),
                        html.H3(id="metric-orders", className="mb-0"),
                        html.P("Orders", className="text-muted mb-0")
                    ], className="text-center")
                ], className="metric-card animate-fade-in")
            ], width=3),
            dbc.Col([
                html.Div([
                    html.Div([
                        html.I(className="fas fa-dollar-sign fa-2x text-warning mb-2"),
                        html.H3(id="metric-revenue", className="mb-0"),
                        html.P("Revenue", className="text-muted mb-0")
                    ], className="text-center")
                ], className="metric-card animate-fade-in")
            ], width=3),
        ], className="mb-4"),

        # Charts Row
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H5("Product Inventory", className="mb-3"),
                    dcc.Graph(id="product-inventory-chart", style={'height': '300px'})
                ], className="chart-container animate-fade-in")
            ], width=6),
            dbc.Col([
                html.Div([
                    html.H5("Recent Orders", className="mb-3"),
                    html.Div(id="recent-orders-table")
                ], className="chart-container animate-fade-in")
            ], width=6),
        ]),

        # Real-time Activity
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H5([
                        html.Span(className="live-indicator"),
                        "Real-Time Database Activity"
                    ], className="mb-3"),
                    html.Div(id="recent-inserts-table")
                ], className="chart-container animate-fade-in")
            ], width=12),
        ]),
    ])

# ========================================
# Data Entry Content
# ========================================
def get_data_entry_content():
    return html.Div([
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H4("Add New Product", className="mb-4"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Product Name *"),
                            dbc.Input(id="product-name", type="text", placeholder="Enter product name"),
                        ], width=6),
                        dbc.Col([
                            dbc.Label("Price *"),
                            dbc.Input(id="product-price", type="number", placeholder="0.00", step="0.01"),
                        ], width=6),
                    ], className="mb-3"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Description"),
                            dbc.Textarea(id="product-description", placeholder="Product description"),
                        ], width=12),
                    ], className="mb-3"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Stock Quantity"),
                            dbc.Input(id="product-stock", type="number", placeholder="0", value=0),
                        ], width=4),
                        dbc.Col([
                            dbc.Label("Category"),
                            dbc.Select(id="product-category", options=[
                                {"label": "Electronics", "value": "Electronics"},
                                {"label": "Accessories", "value": "Accessories"},
                                {"label": "Books", "value": "Books"},
                                {"label": "Clothing", "value": "Clothing"},
                                {"label": "Other", "value": "Other"},
                            ]),
                        ], width=4),
                        dbc.Col([
                            dbc.Label("Tags (comma-separated)"),
                            dbc.Input(id="product-tags", type="text", placeholder="tag1, tag2"),
                        ], width=4),
                    ], className="mb-3"),
                    dbc.Button("Add Product", id="submit-product", color="primary", className="mt-3"),
                    html.Div(id="product-form-feedback", className="mt-3"),
                ], className="form-container")
            ], width=6),
            dbc.Col([
                html.Div([
                    html.H4("Add New User", className="mb-4"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Email *"),
                            dbc.Input(id="user-email", type="email", placeholder="user@example.com"),
                        ], width=6),
                        dbc.Col([
                            dbc.Label("Username *"),
                            dbc.Input(id="user-username", type="text", placeholder="username"),
                        ], width=6),
                    ], className="mb-3"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Full Name"),
                            dbc.Input(id="user-fullname", type="text", placeholder="John Doe"),
                        ], width=6),
                        dbc.Col([
                            dbc.Label("Role"),
                            dbc.Select(id="user-role", options=[
                                {"label": "Customer", "value": "customer"},
                                {"label": "Admin", "value": "admin"},
                                {"label": "Vendor", "value": "vendor"},
                            ], value="customer"),
                        ], width=6),
                    ], className="mb-3"),
                    dbc.Button("Add User", id="submit-user", color="primary", className="mt-3"),
                    html.Div(id="user-form-feedback", className="mt-3"),
                ], className="form-container")
            ], width=6),
        ])
    ])

# ========================================
# Create Order Content
# ========================================
def get_create_order_content():
    return html.Div([
        html.Div([
            html.H4("Create New Order", className="mb-4"),
            html.P("Demonstrates multi-table transactions with foreign key relationships", className="text-muted"),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Select Customer *"),
                    dcc.Dropdown(id="order-customer", placeholder="Select a customer"),
                ], width=6),
                dbc.Col([
                    dbc.Label("Payment Method"),
                    dbc.Select(id="order-payment", options=[
                        {"label": "Credit Card", "value": "credit_card"},
                        {"label": "PayPal", "value": "paypal"},
                        {"label": "Bank Transfer", "value": "bank_transfer"},
                    ], value="credit_card"),
                ], width=6),
            ], className="mb-3"),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Shipping Address (JSON)"),
                    dbc.Textarea(id="order-address", value='{"street": "123 Main St", "city": "Denver", "state": "CO", "zip": "80202"}', rows=3),
                ], width=12),
            ], className="mb-3"),
            html.H5("Order Items", className="mt-4 mb-3"),
            html.Div(id="order-items-container", children=[
                dbc.Row([
                    dbc.Col([dcc.Dropdown(id="order-product-1", placeholder="Select product")], width=6),
                    dbc.Col([dbc.Input(id="order-qty-1", type="number", placeholder="Qty", value=1, min=1)], width=3),
                    dbc.Col([html.Div(id="order-subtotal-1")], width=3),
                ], className="mb-2"),
            ]),
            dbc.Button("Create Order", id="submit-order", color="primary", className="mt-3"),
            html.Div(id="order-form-feedback", className="mt-3"),
        ], className="form-container")
    ])

# ========================================
# Bulk Import Content
# ========================================
def get_bulk_import_content():
    return html.Div([
        html.Div([
            html.H4("Bulk Import Products", className="mb-4"),
            html.P("Upload a CSV file to batch insert products", className="text-muted"),
            dcc.Upload(
                id="upload-csv",
                children=html.Div([
                    html.I(className="fas fa-cloud-upload-alt fa-3x mb-3"),
                    html.Br(),
                    "Drag and Drop or ",
                    html.A("Select a CSV File", className="text-primary")
                ]),
                style={
                    'width': '100%', 'height': '150px', 'lineHeight': '30px',
                    'borderWidth': '2px', 'borderStyle': 'dashed', 'borderRadius': '12px',
                    'textAlign': 'center', 'padding': '30px', 'cursor': 'pointer'
                },
            ),
            html.Div(id="upload-preview", className="mt-4"),
            dbc.Button("Download CSV Template", id="download-template", color="secondary", className="mt-3 me-2"),
            dcc.Download(id="download-template-file"),
            html.Div(id="bulk-import-feedback", className="mt-3"),
        ], className="form-container")
    ])

# ========================================
# Audit Trail Content
# ========================================
def get_audit_trail_content():
    return html.Div([
        html.Div([
            html.H4([
                html.Span(className="live-indicator"),
                "Audit Trail - Database Change Log"
            ], className="mb-4"),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Filter by Table"),
                    dbc.Select(id="audit-table-filter", options=[
                        {"label": "All Tables", "value": "all"},
                        {"label": "Products", "value": "products"},
                        {"label": "Users", "value": "users"},
                        {"label": "Orders", "value": "orders"},
                        {"label": "Order Items", "value": "order_items"},
                    ], value="all"),
                ], width=3),
                dbc.Col([
                    dbc.Label("Filter by Operation"),
                    dbc.Select(id="audit-operation-filter", options=[
                        {"label": "All Operations", "value": "all"},
                        {"label": "INSERT", "value": "INSERT"},
                        {"label": "UPDATE", "value": "UPDATE"},
                        {"label": "DELETE", "value": "DELETE"},
                    ], value="all"),
                ], width=3),
                dbc.Col([
                    dbc.Label("Auto-refresh"),
                    dbc.Select(id="audit-refresh-rate", options=[
                        {"label": "5 seconds", "value": "5000"},
                        {"label": "10 seconds", "value": "10000"},
                        {"label": "30 seconds", "value": "30000"},
                        {"label": "Off", "value": "0"},
                    ], value="10000"),
                ], width=3),
                dbc.Col([
                    dbc.Label(" "),
                    dbc.Button("Refresh Now", id="audit-refresh-btn", color="primary", className="d-block"),
                ], width=3),
            ], className="mb-4"),
            html.Div(id="audit-trail-table"),
            dcc.Interval(id="audit-interval", interval=10000, n_intervals=0),
        ], className="form-container")
    ])

# ========================================
# Query Builder Content
# ========================================
def get_query_builder_content():
    return html.Div([
        html.Div([
            html.H4("SQL Query Builder", className="mb-4"),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Sample Queries"),
                    dbc.Select(id="sample-queries", options=[
                        {"label": "-- Select a query --", "value": ""},
                        {"label": "All Users", "value": "users"},
                        {"label": "All Products", "value": "products"},
                        {"label": "Recent Orders", "value": "orders"},
                        {"label": "Audit Log Summary", "value": "audit_summary"},
                        {"label": "Recent Inserts", "value": "recent_inserts"},
                    ]),
                ], width=6),
            ], className="mb-3"),
            dbc.Label("Custom SQL Query"),
            dbc.Textarea(id="custom-query", rows=5, placeholder="SELECT * FROM ecommerce.products LIMIT 10;"),
            dbc.Button("Execute Query", id="execute-query", color="primary", className="mt-3 me-2"),
            dbc.Button("Export CSV", id="download-csv", color="secondary", className="mt-3", disabled=True),
            html.Div(id="query-results", className="mt-4"),
        ], className="form-container")
    ])

# ========================================
# Callbacks
# ========================================

# Tab content switcher
@app.callback(
    Output("tab-content", "children"),
    Input("tabs", "active_tab")
)
def render_tab_content(active_tab):
    if active_tab == "dashboard":
        return get_dashboard_content()
    elif active_tab == "data-entry":
        return get_data_entry_content()
    elif active_tab == "create-order":
        return get_create_order_content()
    elif active_tab == "bulk-import":
        return get_bulk_import_content()
    elif active_tab == "audit-trail":
        return get_audit_trail_content()
    elif active_tab == "query-builder":
        return get_query_builder_content()
    return html.Div("Tab not found")

# Connection status
@app.callback(
    Output("connection-status", "children"),
    Input("interval-component", "n_intervals")
)
def update_connection_status(n):
    try:
        with LakebaseConnection() as db:
            db.execute_query("SELECT 1")
        host_info = _cached_host or "discovering..."
        mode = "Autoscaling" if LAKEBASE_PROJECT_ID else "Provisioned"
        project_info = f" | Project: {LAKEBASE_PROJECT_ID}/{LAKEBASE_BRANCH_ID}" if LAKEBASE_PROJECT_ID else ""
        return dbc.Alert([
            html.I(className="fas fa-check-circle me-2"),
            f"Connected to Lakebase ({mode}): {host_info} / {PGDATABASE}{project_info}"
        ], color="success", className="mb-0")
    except Exception as e:
        return dbc.Alert([
            html.I(className="fas fa-exclamation-triangle me-2"),
            f"Connection Error: {str(e)[:100]}"
        ], color="danger", className="mb-0")

# Update dashboard metrics
@app.callback(
    [Output("metric-users", "children"),
     Output("metric-products", "children"),
     Output("metric-orders", "children"),
     Output("metric-revenue", "children")],
    Input("interval-component", "n_intervals")
)
def update_metrics(n):
    try:
        with LakebaseConnection() as db:
            users = db.execute_query("SELECT COUNT(*) as count FROM ecommerce.users")[0]['count']
            products = db.execute_query("SELECT COUNT(*) as count FROM ecommerce.products")[0]['count']
            orders = db.execute_query("SELECT COUNT(*) as count FROM ecommerce.orders")[0]['count']
            revenue = db.execute_query(
                "SELECT COALESCE(SUM(total_amount), 0) as revenue FROM ecommerce.orders WHERE status='completed'"
            )[0]['revenue']
            return f"{users:,}", f"{products:,}", f"{orders:,}", f"${float(revenue or 0):,.2f}"
    except Exception as e:
        print(f"Error updating metrics: {e}")
        return "?", "?", "?", "?"

# Update product inventory chart
@app.callback(
    Output("product-inventory-chart", "figure"),
    Input("interval-component", "n_intervals")
)
def update_inventory_chart(n):
    try:
        with LakebaseConnection() as db:
            results = db.execute_query("""
                SELECT name, stock_quantity, category
                FROM ecommerce.products
                ORDER BY stock_quantity DESC
                LIMIT 10
            """)
            if results:
                df = pd.DataFrame(results)
                fig = px.bar(df, x='name', y='stock_quantity', color='category',
                            labels={'stock_quantity': 'Stock', 'name': 'Product'})
                fig.update_layout(plot_bgcolor='white', paper_bgcolor='white',
                                xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor='#f0f0f0'))
                return fig
    except Exception as e:
        print(f"Error updating chart: {e}")
    return go.Figure()

# Update recent orders table
@app.callback(
    Output("recent-orders-table", "children"),
    Input("interval-component", "n_intervals")
)
def update_orders_table(n):
    try:
        with LakebaseConnection() as db:
            results = db.execute_query("""
                SELECT o.order_id, u.username, o.order_date, o.status, o.total_amount
                FROM ecommerce.orders o
                JOIN ecommerce.users u ON o.user_id = u.user_id
                ORDER BY o.order_date DESC LIMIT 5
            """)
            if results:
                df = pd.DataFrame(results)
                df = convert_for_datatable(df)
                df['order_date'] = pd.to_datetime(df['order_date']).dt.strftime('%Y-%m-%d %H:%M')
                df['total_amount'] = df['total_amount'].apply(lambda x: f"${float(x):.2f}" if x else "$0.00")
                return dash_table.DataTable(
                    data=df.to_dict('records'),
                    columns=[{"name": i.replace('_', ' ').title(), "id": i} for i in df.columns],
                    style_cell={'textAlign': 'left', 'padding': '10px'},
                    style_header={'backgroundColor': '#667eea', 'color': 'white', 'fontWeight': 'bold'},
                    style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'}],
                )
    except Exception as e:
        print(f"Error updating orders: {e}")
    return html.P("No orders found", className="text-muted")

# Update recent inserts
@app.callback(
    Output("recent-inserts-table", "children"),
    Input("interval-component", "n_intervals")
)
def update_recent_inserts(n):
    try:
        with LakebaseConnection() as db:
            # Check if audit_log exists
            results = db.execute_query("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'ecommerce' AND table_name = 'audit_log'
            """)
            if not results:
                return html.P("Audit log table not found. Add data to see activity.", className="text-muted")

            results = db.execute_query("""
                SELECT table_name, operation, record_id, created_at
                FROM ecommerce.audit_log
                ORDER BY created_at DESC LIMIT 10
            """)
            if results:
                df = pd.DataFrame(results)
                df = convert_for_datatable(df)
                df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
                return dash_table.DataTable(
                    data=df.to_dict('records'),
                    columns=[{"name": i.replace('_', ' ').title(), "id": i} for i in df.columns],
                    style_cell={'textAlign': 'left', 'padding': '10px'},
                    style_header={'backgroundColor': '#28a745', 'color': 'white', 'fontWeight': 'bold'},
                    style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'}],
                )
    except Exception as e:
        print(f"Error updating inserts: {e}")
    return html.P("No recent activity", className="text-muted")

# Add product callback
@app.callback(
    Output("product-form-feedback", "children"),
    Input("submit-product", "n_clicks"),
    [State("product-name", "value"),
     State("product-description", "value"),
     State("product-price", "value"),
     State("product-stock", "value"),
     State("product-category", "value"),
     State("product-tags", "value")],
    prevent_initial_call=True
)
def add_product(n_clicks, name, description, price, stock, category, tags):
    if not name or not price:
        return dbc.Alert("Please fill in required fields (Name and Price)", color="danger")
    try:
        tags_array = [tag.strip() for tag in tags.split(',')] if tags else []
        with LakebaseConnection() as db:
            db.execute_query("""
                INSERT INTO ecommerce.products (name, description, price, stock_quantity, category, tags)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (name, description, price, stock or 0, category, tags_array))
        return dbc.Alert("Product added successfully!", color="success")
    except Exception as e:
        return dbc.Alert(f"Error: {str(e)}", color="danger")

# Add user callback
@app.callback(
    Output("user-form-feedback", "children"),
    Input("submit-user", "n_clicks"),
    [State("user-email", "value"),
     State("user-username", "value"),
     State("user-fullname", "value"),
     State("user-role", "value")],
    prevent_initial_call=True
)
def add_user(n_clicks, email, username, fullname, role):
    if not email or not username:
        return dbc.Alert("Please fill in required fields (Email and Username)", color="danger")
    try:
        metadata = json.dumps({"role": role})
        with LakebaseConnection() as db:
            db.execute_query("""
                INSERT INTO ecommerce.users (email, username, full_name, metadata)
                VALUES (%s, %s, %s, %s::jsonb)
            """, (email, username, fullname, metadata))
        return dbc.Alert("User added successfully!", color="success")
    except Exception as e:
        return dbc.Alert(f"Error: {str(e)}", color="danger")

# Load customers for order dropdown
@app.callback(
    Output("order-customer", "options"),
    Input("tabs", "active_tab")
)
def load_customers(active_tab):
    if active_tab == "create-order":
        try:
            with LakebaseConnection() as db:
                results = db.execute_query("SELECT user_id, username, email FROM ecommerce.users ORDER BY username")
                return [{"label": f"{r['username']} ({r['email']})", "value": r['user_id']} for r in results]
        except Exception as e:
            print(f"Error loading customers: {e}")
    return []

# Load products for order dropdown
@app.callback(
    Output("order-product-1", "options"),
    Input("tabs", "active_tab")
)
def load_products(active_tab):
    if active_tab == "create-order":
        try:
            with LakebaseConnection() as db:
                results = db.execute_query("SELECT product_id, name, price FROM ecommerce.products ORDER BY name")
                return [{"label": f"{r['name']} (${r['price']})", "value": r['product_id']} for r in results]
        except Exception as e:
            print(f"Error loading products: {e}")
    return []

# Create order callback
@app.callback(
    Output("order-form-feedback", "children"),
    Input("submit-order", "n_clicks"),
    [State("order-customer", "value"),
     State("order-payment", "value"),
     State("order-address", "value"),
     State("order-product-1", "value"),
     State("order-qty-1", "value")],
    prevent_initial_call=True
)
def create_order(n_clicks, customer_id, payment, address, product_id, qty):
    if not customer_id or not product_id:
        return dbc.Alert("Please select a customer and at least one product", color="danger")
    try:
        with LakebaseConnection() as db:
            # Get product price
            product = db.execute_query("SELECT price FROM ecommerce.products WHERE product_id = %s", (product_id,))
            if not product:
                return dbc.Alert("Product not found", color="danger")
            price = float(product[0]['price'])
            total = price * int(qty or 1)

            # Create order
            order = db.execute_query("""
                INSERT INTO ecommerce.orders (user_id, status, total_amount, shipping_address, payment_method)
                VALUES (%s, 'pending', %s, %s::jsonb, %s)
                RETURNING order_id
            """, (customer_id, total, address, payment))

            order_id = order[0]['order_id']

            # Add order item
            db.execute_query("""
                INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price)
                VALUES (%s, %s, %s, %s)
            """, (order_id, product_id, qty or 1, price))

        return dbc.Alert(f"Order #{order_id} created successfully! Total: ${total:.2f}", color="success")
    except Exception as e:
        return dbc.Alert(f"Error: {str(e)}", color="danger")

# Audit trail callback
@app.callback(
    Output("audit-trail-table", "children"),
    [Input("audit-refresh-btn", "n_clicks"),
     Input("audit-interval", "n_intervals")],
    [State("audit-table-filter", "value"),
     State("audit-operation-filter", "value")]
)
def update_audit_trail(n_clicks, n_intervals, table_filter, operation_filter):
    try:
        with LakebaseConnection() as db:
            # Check if audit_log exists
            check = db.execute_query("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'ecommerce' AND table_name = 'audit_log'
            """)
            if not check:
                return html.Div([
                    dbc.Alert("Audit log table not found. Creating it now...", color="warning"),
                    html.P("The audit_log table and triggers need to be created.")
                ])

            query = "SELECT * FROM ecommerce.audit_log WHERE 1=1"
            params = []
            if table_filter and table_filter != "all":
                query += " AND table_name = %s"
                params.append(table_filter)
            if operation_filter and operation_filter != "all":
                query += " AND operation = %s"
                params.append(operation_filter)
            query += " ORDER BY created_at DESC LIMIT 50"

            results = db.execute_query(query, tuple(params) if params else None)
            if results:
                df = pd.DataFrame(results)
                df = convert_for_datatable(df)
                if 'created_at' in df.columns:
                    df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
                return dash_table.DataTable(
                    data=df.to_dict('records'),
                    columns=[{"name": i.replace('_', ' ').title(), "id": i} for i in df.columns],
                    style_cell={'textAlign': 'left', 'padding': '10px', 'maxWidth': '200px', 'overflow': 'hidden', 'textOverflow': 'ellipsis'},
                    style_header={'backgroundColor': '#667eea', 'color': 'white', 'fontWeight': 'bold'},
                    style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'}],
                    page_size=15,
                    tooltip_data=[{col: {'value': str(row[col]), 'type': 'markdown'} for col in df.columns} for row in df.to_dict('records')],
                    tooltip_duration=None
                )
    except Exception as e:
        print(f"Error updating audit trail: {e}")
        return dbc.Alert(f"Error loading audit trail: {str(e)}", color="danger")
    return html.P("No audit records found", className="text-muted")

# Sample query loader
@app.callback(
    Output("custom-query", "value"),
    Input("sample-queries", "value")
)
def load_sample_query(query_type):
    queries = {
        "users": "SELECT * FROM ecommerce.users ORDER BY created_at DESC LIMIT 20;",
        "products": "SELECT * FROM ecommerce.products ORDER BY created_at DESC LIMIT 20;",
        "orders": "SELECT o.*, u.username FROM ecommerce.orders o JOIN ecommerce.users u ON o.user_id = u.user_id ORDER BY o.order_date DESC LIMIT 20;",
        "audit_summary": "SELECT table_name, operation, COUNT(*) as count, MAX(created_at) as last_activity FROM ecommerce.audit_log GROUP BY table_name, operation ORDER BY last_activity DESC;",
        "recent_inserts": "SELECT table_name, operation, record_id, new_data->>'name' as item_name, created_at FROM ecommerce.audit_log WHERE operation = 'INSERT' ORDER BY created_at DESC LIMIT 20;"
    }
    return queries.get(query_type, "")

# Execute query callback
@app.callback(
    [Output("query-results", "children"),
     Output("download-csv", "disabled")],
    Input("execute-query", "n_clicks"),
    State("custom-query", "value"),
    prevent_initial_call=True
)
def execute_query(n_clicks, query):
    if not query:
        return dbc.Alert("Please enter a query", color="warning"), True
    try:
        with LakebaseConnection() as db:
            results = db.execute_query(query)
            if results:
                df = pd.DataFrame(results)
                df = convert_for_datatable(df)
                return html.Div([
                    dbc.Alert(f"Query returned {len(df)} rows", color="success"),
                    dash_table.DataTable(
                        data=df.to_dict('records'),
                        columns=[{"name": str(i), "id": str(i)} for i in df.columns],
                        style_cell={'textAlign': 'left', 'padding': '10px'},
                        style_header={'backgroundColor': '#667eea', 'color': 'white', 'fontWeight': 'bold'},
                        style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'}],
                        page_size=15,
                        export_format="csv"
                    )
                ]), False
            else:
                return dbc.Alert("Query executed successfully but returned no results", color="info"), True
    except Exception as e:
        return dbc.Alert(f"Query error: {str(e)}", color="danger"), True

# Download CSV template
@app.callback(
    Output("download-template-file", "data"),
    Input("download-template", "n_clicks"),
    prevent_initial_call=True
)
def download_template(n_clicks):
    template = "name,description,price,stock_quantity,category,tags\nSample Product,A sample product description,29.99,100,Electronics,\"sample,product\""
    return dict(content=template, filename="products_template.csv")

# ========================================
# Run the app
# ========================================
if __name__ == '__main__':
    port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))
    print(f"Starting app on port {port}")
    if LAKEBASE_PROJECT_ID:
        print(f"Lakebase Autoscaling: project={LAKEBASE_PROJECT_ID}, branch={LAKEBASE_BRANCH_ID}")
    else:
        print(f"Lakebase config: host={PGHOST_OVERRIDE or 'auto-discover'}, db={PGDATABASE}")
    app.run_server(debug=True, host='0.0.0.0', port=port)
