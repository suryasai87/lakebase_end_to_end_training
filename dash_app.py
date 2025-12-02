"""
Databricks Lakebase End-to-End Training Dashboard - Enhanced Version
A comprehensive training application showcasing advanced Lakebase integration features:
- Create Order with Transaction Handling
- Real-Time Insert Verification Panel
- Audit Trail with PostgreSQL Triggers
- Bulk CSV Import
"""

import os
import dash
from dash import dcc, html, Input, Output, State, callback, dash_table
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import plotly.express as px
import plotly.graph_objects as go
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
import pandas as pd
from datetime import datetime
import json
import time
import base64
import io
from databricks import sdk

# ========================================
# OAuth Token Management
# ========================================
workspace_client = sdk.WorkspaceClient()
postgres_password = None
last_password_refresh = 0
connection_pool = None

def refresh_oauth_token():
    """Refresh OAuth token if expired."""
    global postgres_password, last_password_refresh
    if postgres_password is None or time.time() - last_password_refresh > 900:
        print("Refreshing PostgreSQL OAuth token")
        try:
            postgres_password = workspace_client.config.oauth_token().access_token
            last_password_refresh = time.time()
            print("OAuth token refreshed successfully")
        except Exception as e:
            print(f"Failed to refresh OAuth token: {str(e)}")
            raise

# ========================================
# Database Configuration
# ========================================
def get_connection_pool():
    """Get or create the connection pool."""
    global connection_pool
    if connection_pool is None:
        refresh_oauth_token()
        conn_string = (
            f"dbname={os.getenv('PGDATABASE')} "
            f"user={os.getenv('PGUSER')} "
            f"password={postgres_password} "
            f"host={os.getenv('PGHOST')} "
            f"port={os.getenv('PGPORT')} "
            f"sslmode={os.getenv('PGSSLMODE', 'require')} "
            f"application_name={os.getenv('PGAPPNAME', 'lakebase-training-app')}"
        )
        connection_pool = ConnectionPool(conn_string, min_size=2, max_size=10)
        print("Connection pool created successfully")
    return connection_pool

def get_connection():
    """Get a connection from the pool."""
    global connection_pool

    # Recreate pool if token expired
    if postgres_password is None or time.time() - last_password_refresh > 900:
        if connection_pool:
            connection_pool.close()
            connection_pool = None

    return get_connection_pool().connection()

# ========================================
# Database Connection Manager
# ========================================
class LakebaseConnection:
    """Manage Lakebase database connections with OAuth token refresh"""

    def __init__(self):
        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """Establish connection to Lakebase"""
        try:
            self.connection = get_connection()
            self.cursor = self.connection.cursor(row_factory=dict_row)
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def execute_query(self, query, params=None):
        """Execute a query and return results"""
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
            self.connection.rollback()
            raise e

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

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

# Custom CSS for Framer Motion-like animations
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

            @keyframes slideInRight {
                from { opacity: 0; transform: translateX(100px); }
                to { opacity: 1; transform: translateX(0); }
            }

            @keyframes scaleIn {
                from { opacity: 0; transform: scale(0.8); }
                to { opacity: 1; transform: scale(1); }
            }

            @keyframes pulse {
                0% { box-shadow: 0 0 0 0 rgba(102, 126, 234, 0.4); }
                70% { box-shadow: 0 0 0 10px rgba(102, 126, 234, 0); }
                100% { box-shadow: 0 0 0 0 rgba(102, 126, 234, 0); }
            }

            .animate-fade-in {
                animation: fadeIn 0.6s ease-out;
            }

            .animate-slide-in {
                animation: slideInRight 0.8s ease-out;
            }

            .animate-scale-in {
                animation: scaleIn 0.5s ease-out;
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
                box-shadow: 0 12px 24px rgba(0, 0, 0, 0.15);
            }

            .nav-tab {
                transition: all 0.3s ease;
                border-radius: 8px;
                padding: 12px 24px;
                margin: 0 8px;
            }

            .nav-tab:hover {
                background: #f0f0f0;
                transform: scale(1.05);
            }

            .form-control, .form-select {
                transition: all 0.3s ease;
                border-radius: 8px;
            }

            .form-control:focus, .form-select:focus {
                transform: scale(1.02);
                box-shadow: 0 0 0 3px rgba(66, 153, 225, 0.5);
            }

            .btn {
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                border-radius: 8px;
            }

            .btn:hover {
                transform: translateY(-2px);
                box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
            }

            .btn:active {
                transform: translateY(0);
            }

            body {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }

            .main-container {
                background: white;
                border-radius: 16px;
                margin: 20px;
                padding: 32px;
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            }

            .header-container {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 32px;
                border-radius: 12px;
                margin-bottom: 32px;
                animation: fadeIn 0.8s ease-out;
            }

            .chart-container {
                background: white;
                border-radius: 12px;
                padding: 24px;
                margin: 16px 0;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
                transition: all 0.3s ease;
            }

            .chart-container:hover {
                box-shadow: 0 8px 24px rgba(0, 0, 0, 0.15);
            }

            .audit-log-item {
                border-left: 4px solid #667eea;
                padding: 12px;
                margin: 8px 0;
                background: #f8f9fa;
                border-radius: 0 8px 8px 0;
                transition: all 0.3s ease;
            }

            .audit-log-item:hover {
                background: #e9ecef;
                transform: translateX(5px);
            }

            .audit-insert { border-left-color: #28a745; }
            .audit-update { border-left-color: #ffc107; }
            .audit-delete { border-left-color: #dc3545; }

            .live-indicator {
                display: inline-block;
                width: 10px;
                height: 10px;
                background: #28a745;
                border-radius: 50%;
                margin-right: 8px;
                animation: pulse 2s infinite;
            }

            .order-item-row {
                background: #f8f9fa;
                padding: 12px;
                border-radius: 8px;
                margin: 8px 0;
            }

            .upload-zone {
                border: 2px dashed #667eea;
                border-radius: 12px;
                padding: 40px;
                text-align: center;
                background: #f8f9fa;
                transition: all 0.3s ease;
                cursor: pointer;
            }

            .upload-zone:hover {
                background: #e9ecef;
                border-color: #764ba2;
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
# Layout Components
# ========================================

# Header Component
header = html.Div([
    html.Div([
        html.H1([
            html.I(className="fas fa-database me-3"),
            "Databricks Lakebase End-to-End Training"
        ], className="mb-2"),
        html.P(
            "Advanced training application with audit trails, bulk imports, and real-time verification",
            className="mb-0 opacity-75"
        )
    ], className="header-container")
], className="animate-fade-in")

# Navigation Tabs - Enhanced with new tabs
tabs = html.Div([
    dbc.Tabs([
        dbc.Tab(label="📊 Dashboard", tab_id="dashboard", className="nav-tab"),
        dbc.Tab(label="📝 Data Entry", tab_id="data-entry", className="nav-tab"),
        dbc.Tab(label="🛒 Create Order", tab_id="create-order", className="nav-tab"),
        dbc.Tab(label="📤 Bulk Import", tab_id="bulk-import", className="nav-tab"),
        dbc.Tab(label="📋 Audit Trail", tab_id="audit-trail", className="nav-tab"),
        dbc.Tab(label="🔍 Query Builder", tab_id="query-builder", className="nav-tab"),
        dbc.Tab(label="🔮 Vector Search", tab_id="vector-search", className="nav-tab"),
    ], id="tabs", active_tab="dashboard", className="mb-4")
], className="animate-slide-in")

# Dashboard Tab Content - Enhanced with Real-Time Insert Panel
dashboard_content = html.Div([
    # Metrics Cards
    html.Div([
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.I(className="fas fa-users fa-2x text-primary mb-3"),
                    html.H3(id="metric-users", children="Loading...", className="mb-1"),
                    html.P("Total Users", className="text-muted mb-0")
                ], className="metric-card text-center animate-scale-in")
            ], width=3),
            dbc.Col([
                html.Div([
                    html.I(className="fas fa-box fa-2x text-success mb-3"),
                    html.H3(id="metric-products", children="Loading...", className="mb-1"),
                    html.P("Total Products", className="text-muted mb-0")
                ], className="metric-card text-center animate-scale-in")
            ], width=3),
            dbc.Col([
                html.Div([
                    html.I(className="fas fa-shopping-cart fa-2x text-info mb-3"),
                    html.H3(id="metric-orders", children="Loading...", className="mb-1"),
                    html.P("Total Orders", className="text-muted mb-0")
                ], className="metric-card text-center animate-scale-in")
            ], width=3),
            dbc.Col([
                html.Div([
                    html.I(className="fas fa-dollar-sign fa-2x text-warning mb-3"),
                    html.H3(id="metric-revenue", children="Loading...", className="mb-1"),
                    html.P("Total Revenue", className="text-muted mb-0")
                ], className="metric-card text-center animate-scale-in")
            ], width=3),
        ], className="mb-4")
    ]),

    # Charts Row
    dbc.Row([
        dbc.Col([
            html.Div([
                html.H4("📦 Product Inventory", className="mb-3"),
                dcc.Graph(id="product-inventory-chart")
            ], className="chart-container animate-fade-in")
        ], width=6),
        dbc.Col([
            html.Div([
                html.H4("📈 Revenue Trend", className="mb-3"),
                dcc.Graph(id="revenue-trend-chart")
            ], className="chart-container animate-fade-in")
        ], width=6),
    ], className="mb-4"),

    # Real-Time Insert Verification Panel (NEW FEATURE)
    dbc.Row([
        dbc.Col([
            html.Div([
                html.H4([
                    html.Span(className="live-indicator"),
                    "Real-Time Database Activity"
                ], className="mb-3"),
                html.P("Shows the last 10 database inserts across all tables", className="text-muted"),
                html.Div(id="recent-inserts-table")
            ], className="chart-container animate-fade-in")
        ], width=6),
        dbc.Col([
            html.Div([
                html.H4("🛒 Recent Orders", className="mb-3"),
                html.Div(id="recent-orders-table")
            ], className="chart-container animate-fade-in")
        ], width=6),
    ]),

    # Auto-refresh interval (5 seconds for real-time feel)
    dcc.Interval(id='interval-component', interval=5000, n_intervals=0)
])

# Data Entry Tab Content
data_entry_content = html.Div([
    dbc.Tabs([
        dbc.Tab(label="Add Product", tab_id="add-product"),
        dbc.Tab(label="Add User", tab_id="add-user"),
    ], id="data-entry-tabs", active_tab="add-product", className="mb-4"),

    html.Div(id="data-entry-form-container")
])

# Product Form
product_form = html.Div([
    dbc.Row([
        dbc.Col([
            dbc.Label("Product Name *"),
            dbc.Input(id="product-name", type="text", placeholder="Enter product name"),
        ], width=6),
        dbc.Col([
            dbc.Label("Category *"),
            dbc.Select(id="product-category", options=[
                {"label": "Electronics", "value": "Electronics"},
                {"label": "Accessories", "value": "Accessories"},
                {"label": "Books", "value": "Books"},
                {"label": "Clothing", "value": "Clothing"},
                {"label": "Other", "value": "Other"},
            ]),
        ], width=6),
    ], className="mb-3"),

    dbc.Row([
        dbc.Col([
            dbc.Label("Price *"),
            dbc.Input(id="product-price", type="number", placeholder="0.00", min=0, step=0.01),
        ], width=4),
        dbc.Col([
            dbc.Label("Stock Quantity"),
            dbc.Input(id="product-stock", type="number", placeholder="0", value=0, min=0),
        ], width=4),
        dbc.Col([
            dbc.Label("Tags (comma-separated)"),
            dbc.Input(id="product-tags", type="text", placeholder="tag1, tag2, tag3"),
        ], width=4),
    ], className="mb-3"),

    dbc.Row([
        dbc.Col([
            dbc.Label("Description"),
            dbc.Textarea(id="product-description", placeholder="Product description", rows=4),
        ]),
    ], className="mb-3"),

    dbc.Button("Add Product", id="submit-product", color="primary", className="mt-3"),
    html.Div(id="product-form-feedback", className="mt-3")
], className="animate-fade-in p-4")

# User Form
user_form = html.Div([
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
            dbc.Input(id="user-fullname", type="text", placeholder="Full Name"),
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

    dbc.Button("Add User", id="submit-user", color="success", className="mt-3"),
    html.Div(id="user-form-feedback", className="mt-3")
], className="animate-fade-in p-4")

# Create Order Tab Content (NEW FEATURE)
create_order_content = html.Div([
    html.H4("🛒 Create New Order", className="mb-4"),
    html.P("Demonstrates multi-table transactions with foreign key relationships", className="text-muted mb-4"),

    dbc.Row([
        dbc.Col([
            dbc.Label("Select Customer *"),
            dbc.Select(id="order-customer", placeholder="Select a customer"),
        ], width=6),
        dbc.Col([
            dbc.Label("Payment Method *"),
            dbc.Select(id="order-payment", options=[
                {"label": "Credit Card", "value": "credit_card"},
                {"label": "PayPal", "value": "paypal"},
                {"label": "Bank Transfer", "value": "bank_transfer"},
                {"label": "Cash on Delivery", "value": "cod"},
            ], value="credit_card"),
        ], width=6),
    ], className="mb-4"),

    html.H5("Shipping Address", className="mb-3"),
    dbc.Row([
        dbc.Col([
            dbc.Label("Street"),
            dbc.Input(id="order-street", type="text", placeholder="123 Main St"),
        ], width=6),
        dbc.Col([
            dbc.Label("City"),
            dbc.Input(id="order-city", type="text", placeholder="Denver"),
        ], width=3),
        dbc.Col([
            dbc.Label("State"),
            dbc.Input(id="order-state", type="text", placeholder="CO"),
        ], width=1),
        dbc.Col([
            dbc.Label("ZIP"),
            dbc.Input(id="order-zip", type="text", placeholder="80202"),
        ], width=2),
    ], className="mb-4"),

    html.H5("Order Items", className="mb-3"),
    html.Div(id="order-items-container", children=[
        html.Div([
            dbc.Row([
                dbc.Col([
                    dbc.Label("Product"),
                    dbc.Select(id={"type": "order-product", "index": 0}),
                ], width=6),
                dbc.Col([
                    dbc.Label("Quantity"),
                    dbc.Input(id={"type": "order-quantity", "index": 0}, type="number", value=1, min=1),
                ], width=3),
                dbc.Col([
                    dbc.Label("Unit Price"),
                    dbc.Input(id={"type": "order-price", "index": 0}, type="number", disabled=True),
                ], width=3),
            ], className="order-item-row")
        ], id="order-item-0"),
    ]),

    dbc.Button("+ Add Item", id="add-order-item", color="secondary", className="mt-2 me-2"),
    dbc.Button("Create Order", id="submit-order", color="primary", className="mt-2"),

    html.Div(id="order-form-feedback", className="mt-4"),

    # Store for order items count
    dcc.Store(id="order-items-count", data=1),
], className="animate-fade-in p-4")

# Bulk Import Tab Content (NEW FEATURE)
bulk_import_content = html.Div([
    html.H4("📤 Bulk Import Products", className="mb-4"),
    html.P("Upload a CSV file to import multiple products at once", className="text-muted mb-4"),

    dcc.Upload(
        id="upload-csv",
        children=html.Div([
            html.I(className="fas fa-cloud-upload-alt fa-3x text-primary mb-3"),
            html.H5("Drag and Drop or Click to Upload"),
            html.P("Accepts CSV files with columns: name, description, price, stock_quantity, category",
                   className="text-muted small")
        ]),
        className="upload-zone",
        multiple=False
    ),

    html.Div(id="upload-preview", className="mt-4"),

    dbc.Button("Import Products", id="execute-import", color="success", className="mt-3", disabled=True),

    html.Div(id="import-feedback", className="mt-4"),

    # Sample CSV template
    html.Hr(className="my-4"),
    html.H5("📋 CSV Template", className="mb-3"),
    html.P("Download the sample template and fill in your product data:", className="text-muted"),
    dbc.Button("Download Template", id="download-template", color="secondary", className="me-2"),
    dcc.Download(id="download-csv-template"),

    html.Pre('''name,description,price,stock_quantity,category
"Wireless Earbuds","Bluetooth 5.0 earbuds with noise cancellation",79.99,100,Electronics
"USB-C Cable","Fast charging 6ft cable",12.99,500,Accessories
"Python Book","Learn Python in 30 days",34.99,75,Books''',
             className="bg-light p-3 rounded mt-3", style={"fontSize": "12px"}),

    # Store for uploaded data
    dcc.Store(id="uploaded-data-store"),
], className="animate-fade-in p-4")

# Audit Trail Tab Content (NEW FEATURE)
audit_trail_content = html.Div([
    html.H4([
        html.Span(className="live-indicator"),
        "Audit Trail - Database Change Log"
    ], className="mb-4"),
    html.P("Real-time tracking of all INSERT, UPDATE, and DELETE operations", className="text-muted mb-4"),

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
        ], width=4),
        dbc.Col([
            dbc.Label("Filter by Operation"),
            dbc.Select(id="audit-operation-filter", options=[
                {"label": "All Operations", "value": "all"},
                {"label": "INSERT", "value": "INSERT"},
                {"label": "UPDATE", "value": "UPDATE"},
                {"label": "DELETE", "value": "DELETE"},
            ], value="all"),
        ], width=4),
        dbc.Col([
            dbc.Label("Refresh Rate"),
            dbc.Select(id="audit-refresh-rate", options=[
                {"label": "5 seconds", "value": "5000"},
                {"label": "10 seconds", "value": "10000"},
                {"label": "30 seconds", "value": "30000"},
                {"label": "Manual", "value": "0"},
            ], value="5000"),
        ], width=4),
    ], className="mb-4"),

    dbc.Button("Refresh Now", id="refresh-audit", color="primary", className="mb-4"),

    html.Div(id="audit-log-container"),

    dcc.Interval(id='audit-interval', interval=5000, n_intervals=0),
], className="animate-fade-in p-4")

# Query Builder Tab Content
query_builder_content = html.Div([
    dbc.Row([
        dbc.Col([
            dbc.Label("Sample Queries"),
            dbc.Select(
                id="sample-queries",
                options=[
                    {"label": "Top selling products", "value": "top_products"},
                    {"label": "User purchase history", "value": "user_history"},
                    {"label": "Low stock alert", "value": "low_stock"},
                    {"label": "Revenue by category", "value": "revenue_category"},
                    {"label": "Audit log summary", "value": "audit_summary"},
                    {"label": "Recent inserts", "value": "recent_inserts"},
                ],
                value="top_products"
            ),
        ], width=12),
    ], className="mb-3"),

    dbc.Row([
        dbc.Col([
            dbc.Label("Custom SQL Query"),
            dbc.Textarea(id="custom-query", placeholder="Enter SQL query...", rows=8),
        ]),
    ], className="mb-3"),

    dbc.Button("Execute Query", id="execute-query", color="primary", className="me-2"),
    dbc.Button("Download CSV", id="download-csv", color="secondary", disabled=True),

    html.Div(id="query-results", className="mt-4"),
    dcc.Download(id="download-dataframe-csv")
], className="animate-fade-in p-4")

# Vector Search Tab Content
vector_search_content = html.Div([
    dbc.Row([
        dbc.Col([
            dbc.Label("Search Query"),
            dbc.Input(id="vector-search-query", type="text", placeholder="e.g., laptop for AI development"),
        ], width=8),
        dbc.Col([
            dbc.Label("Number of Results"),
            dbc.Input(id="vector-top-k", type="number", value=5, min=1, max=20),
        ], width=4),
    ], className="mb-3"),

    dbc.Row([
        dbc.Col([
            dbc.Label("Search Type"),
            dbc.RadioItems(
                id="vector-search-type",
                options=[
                    {"label": "Semantic", "value": "semantic"},
                    {"label": "Hybrid", "value": "hybrid"},
                    {"label": "Traditional", "value": "traditional"},
                ],
                value="semantic",
                inline=True
            ),
        ]),
    ], className="mb-3"),

    dbc.Button("Search", id="execute-vector-search", color="primary", className="mb-4"),

    html.Div(id="vector-search-results")
], className="animate-fade-in p-4")

# Main Layout
app.layout = html.Div([
    header,
    html.Div([
        tabs,
        html.Div(id="tab-content")
    ], className="main-container")
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
        return dashboard_content
    elif active_tab == "data-entry":
        return data_entry_content
    elif active_tab == "create-order":
        return create_order_content
    elif active_tab == "bulk-import":
        return bulk_import_content
    elif active_tab == "audit-trail":
        return audit_trail_content
    elif active_tab == "query-builder":
        return query_builder_content
    elif active_tab == "vector-search":
        return vector_search_content
    return html.Div("Tab not found")

# Data entry form switcher
@app.callback(
    Output("data-entry-form-container", "children"),
    Input("data-entry-tabs", "active_tab")
)
def render_data_entry_form(active_tab):
    if active_tab == "add-product":
        return product_form
    elif active_tab == "add-user":
        return user_form
    return html.Div()

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

            return f"{users:,}", f"{products:,}", f"{orders:,}", f"${float(revenue):,.2f}"
    except Exception as e:
        return "Error", "Error", "Error", "Error"

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
                fig = px.bar(
                    df,
                    x='name',
                    y='stock_quantity',
                    color='category',
                    title="",
                    labels={'stock_quantity': 'Stock Quantity', 'name': 'Product'},
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig.update_layout(
                    plot_bgcolor='white',
                    paper_bgcolor='white',
                    font=dict(family="Arial, sans-serif"),
                    xaxis=dict(showgrid=False),
                    yaxis=dict(showgrid=True, gridcolor='#f0f0f0')
                )
                return fig
    except Exception as e:
        pass

    return go.Figure()

# Update revenue trend chart
@app.callback(
    Output("revenue-trend-chart", "figure"),
    Input("interval-component", "n_intervals")
)
def update_revenue_chart(n):
    try:
        with LakebaseConnection() as db:
            results = db.execute_query("""
                SELECT
                    DATE(order_date) as date,
                    SUM(total_amount) as daily_revenue
                FROM ecommerce.orders
                WHERE status = 'completed'
                GROUP BY DATE(order_date)
                ORDER BY date DESC
                LIMIT 30
            """)

            if results:
                df = pd.DataFrame(results)
                fig = px.line(
                    df,
                    x='date',
                    y='daily_revenue',
                    title="",
                    labels={'daily_revenue': 'Revenue ($)', 'date': 'Date'},
                    line_shape='spline'
                )
                fig.update_traces(line_color='#667eea', line_width=3)
                fig.update_layout(
                    plot_bgcolor='white',
                    paper_bgcolor='white',
                    font=dict(family="Arial, sans-serif"),
                    xaxis=dict(showgrid=False),
                    yaxis=dict(showgrid=True, gridcolor='#f0f0f0')
                )
                return fig
    except Exception as e:
        pass

    return go.Figure()

# Real-Time Insert Verification Panel (NEW CALLBACK)
@app.callback(
    Output("recent-inserts-table", "children"),
    Input("interval-component", "n_intervals")
)
def update_recent_inserts(n):
    try:
        with LakebaseConnection() as db:
            # Query audit log for recent inserts
            results = db.execute_query("""
                SELECT
                    table_name,
                    operation,
                    record_id,
                    new_data->>'name' as item_name,
                    created_at
                FROM ecommerce.audit_log
                ORDER BY created_at DESC
                LIMIT 10
            """)

            if results:
                items = []
                for row in results:
                    op_class = f"audit-{row['operation'].lower()}"
                    icon = "fa-plus-circle text-success" if row['operation'] == 'INSERT' else \
                           "fa-edit text-warning" if row['operation'] == 'UPDATE' else "fa-trash text-danger"

                    time_str = row['created_at'].strftime('%H:%M:%S') if row['created_at'] else ''

                    items.append(
                        html.Div([
                            html.Div([
                                html.I(className=f"fas {icon} me-2"),
                                html.Strong(f"{row['operation']}"),
                                html.Span(f" on {row['table_name']}", className="text-muted"),
                            ]),
                            html.Div([
                                html.Small(f"ID: {row['record_id']} | {row['item_name'] or 'N/A'}", className="text-muted"),
                                html.Small(f" | {time_str}", className="text-muted float-end"),
                            ]),
                        ], className=f"audit-log-item {op_class}")
                    )

                return html.Div(items)
            else:
                return html.Div([
                    html.P("No audit records yet. Add products or users to see activity.", className="text-muted"),
                    html.Small("Tip: The audit trail is populated by database triggers.", className="text-muted")
                ])
    except Exception as e:
        return html.Div([
            html.P("Audit log table not found.", className="text-warning"),
            html.Small(f"Run setup_database.sql to create the audit_log table. Error: {str(e)}", className="text-muted")
        ])

# Update recent orders table
@app.callback(
    Output("recent-orders-table", "children"),
    Input("interval-component", "n_intervals")
)
def update_orders_table(n):
    try:
        with LakebaseConnection() as db:
            results = db.execute_query("""
                SELECT
                    o.order_id,
                    u.username,
                    o.order_date,
                    o.status,
                    o.total_amount
                FROM ecommerce.orders o
                JOIN ecommerce.users u ON o.user_id = u.user_id
                ORDER BY o.order_date DESC
                LIMIT 10
            """)

            if results:
                df = pd.DataFrame(results)
                df['order_date'] = pd.to_datetime(df['order_date']).dt.strftime('%Y-%m-%d %H:%M')
                df['total_amount'] = df['total_amount'].apply(lambda x: f"${float(x):.2f}")

                return dash_table.DataTable(
                    data=df.to_dict('records'),
                    columns=[{"name": i.replace('_', ' ').title(), "id": i} for i in df.columns],
                    style_cell={'textAlign': 'left', 'padding': '12px'},
                    style_header={
                        'backgroundColor': '#667eea',
                        'color': 'white',
                        'fontWeight': 'bold'
                    },
                    style_data_conditional=[
                        {
                            'if': {'row_index': 'odd'},
                            'backgroundColor': '#f9f9f9'
                        }
                    ]
                )
    except Exception as e:
        pass

    return html.Div("No orders found", className="text-muted")

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
                INSERT INTO ecommerce.products
                (name, description, price, stock_quantity, category, tags)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (name, description, price, stock, category, tags_array))

        return dbc.Alert([
            html.I(className="fas fa-check-circle me-2"),
            f"Product '{name}' added successfully! Check the Dashboard to see it in the Real-Time Activity panel."
        ], color="success")
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
                INSERT INTO ecommerce.users
                (email, username, full_name, metadata)
                VALUES (%s, %s, %s, %s::jsonb)
            """, (email, username, fullname, metadata))

        return dbc.Alert([
            html.I(className="fas fa-check-circle me-2"),
            f"User '{username}' added successfully! Check the Dashboard to see it in the Real-Time Activity panel."
        ], color="success")
    except Exception as e:
        return dbc.Alert(f"Error: {str(e)}", color="danger")

# Load customers for order form
@app.callback(
    Output("order-customer", "options"),
    Input("tabs", "active_tab")
)
def load_customers(active_tab):
    if active_tab == "create-order":
        try:
            with LakebaseConnection() as db:
                results = db.execute_query("""
                    SELECT user_id, username, email FROM ecommerce.users ORDER BY username
                """)
                return [{"label": f"{r['username']} ({r['email']})", "value": r['user_id']} for r in results]
        except:
            pass
    return []

# Create Order callback (NEW FEATURE)
@app.callback(
    Output("order-form-feedback", "children"),
    Input("submit-order", "n_clicks"),
    [State("order-customer", "value"),
     State("order-payment", "value"),
     State("order-street", "value"),
     State("order-city", "value"),
     State("order-state", "value"),
     State("order-zip", "value")],
    prevent_initial_call=True
)
def create_order(n_clicks, customer_id, payment_method, street, city, state, zip_code):
    if not customer_id:
        return dbc.Alert("Please select a customer", color="danger")

    try:
        shipping_address = json.dumps({
            "street": street or "",
            "city": city or "",
            "state": state or "",
            "zip": zip_code or ""
        })

        with LakebaseConnection() as db:
            # Create order with RETURNING clause
            result = db.execute_query("""
                INSERT INTO ecommerce.orders (user_id, status, shipping_address, payment_method)
                VALUES (%s, 'pending', %s::jsonb, %s)
                RETURNING order_id
            """, (customer_id, shipping_address, payment_method))

            order_id = result[0]['order_id']

            # Get a sample product for the demo
            products = db.execute_query("""
                SELECT product_id, price FROM ecommerce.products LIMIT 1
            """)

            if products:
                product = products[0]
                # Add order item
                db.execute_query("""
                    INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price)
                    VALUES (%s, %s, 1, %s)
                """, (order_id, product['product_id'], product['price']))

                # Update order total
                db.execute_query("""
                    UPDATE ecommerce.orders
                    SET total_amount = (
                        SELECT SUM(subtotal) FROM ecommerce.order_items WHERE order_id = %s
                    )
                    WHERE order_id = %s
                """, (order_id, order_id))

        return dbc.Alert([
            html.I(className="fas fa-check-circle me-2"),
            f"Order #{order_id} created successfully! ",
            html.A("View in Dashboard", href="#", id="view-order-link")
        ], color="success")
    except Exception as e:
        return dbc.Alert(f"Error creating order: {str(e)}", color="danger")

# Bulk Import - Parse CSV (NEW FEATURE)
@app.callback(
    [Output("upload-preview", "children"),
     Output("execute-import", "disabled"),
     Output("uploaded-data-store", "data")],
    Input("upload-csv", "contents"),
    State("upload-csv", "filename"),
    prevent_initial_call=True
)
def parse_csv(contents, filename):
    if contents is None:
        raise PreventUpdate

    try:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)

        df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))

        # Validate required columns
        required_cols = ['name', 'price']
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            return (
                dbc.Alert(f"Missing required columns: {', '.join(missing)}", color="danger"),
                True,
                None
            )

        # Show preview
        preview = html.Div([
            dbc.Alert(f"✅ Parsed {len(df)} products from {filename}", color="success"),
            html.H5("Preview (first 5 rows):", className="mt-3"),
            dash_table.DataTable(
                data=df.head(5).to_dict('records'),
                columns=[{"name": i, "id": i} for i in df.columns],
                style_cell={'textAlign': 'left', 'padding': '8px'},
                style_header={'backgroundColor': '#667eea', 'color': 'white', 'fontWeight': 'bold'}
            )
        ])

        return preview, False, df.to_json()
    except Exception as e:
        return (
            dbc.Alert(f"Error parsing CSV: {str(e)}", color="danger"),
            True,
            None
        )

# Execute Bulk Import (NEW FEATURE)
@app.callback(
    Output("import-feedback", "children"),
    Input("execute-import", "n_clicks"),
    State("uploaded-data-store", "data"),
    prevent_initial_call=True
)
def execute_bulk_import(n_clicks, json_data):
    if not json_data:
        raise PreventUpdate

    try:
        df = pd.read_json(json_data)
        inserted = 0
        errors = []

        with LakebaseConnection() as db:
            for idx, row in df.iterrows():
                try:
                    db.execute_query("""
                        INSERT INTO ecommerce.products (name, description, price, stock_quantity, category)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        row.get('name'),
                        row.get('description', ''),
                        float(row.get('price', 0)),
                        int(row.get('stock_quantity', 0)),
                        row.get('category', 'Other')
                    ))
                    inserted += 1
                except Exception as e:
                    errors.append(f"Row {idx}: {str(e)}")

        if errors:
            return dbc.Alert([
                html.Div(f"Imported {inserted} products with {len(errors)} errors:"),
                html.Ul([html.Li(e) for e in errors[:5]])
            ], color="warning")

        return dbc.Alert([
            html.I(className="fas fa-check-circle me-2"),
            f"Successfully imported {inserted} products! Check the Dashboard to see them."
        ], color="success")
    except Exception as e:
        return dbc.Alert(f"Import failed: {str(e)}", color="danger")

# Download CSV Template
@app.callback(
    Output("download-csv-template", "data"),
    Input("download-template", "n_clicks"),
    prevent_initial_call=True
)
def download_template(n_clicks):
    template = """name,description,price,stock_quantity,category
"Product Name 1","Description of product 1",29.99,100,Electronics
"Product Name 2","Description of product 2",49.99,50,Accessories
"Product Name 3","Description of product 3",19.99,200,Books"""
    return dict(content=template, filename="product_import_template.csv")

# Audit Trail - Update log (NEW FEATURE)
@app.callback(
    Output("audit-log-container", "children"),
    [Input("audit-interval", "n_intervals"),
     Input("refresh-audit", "n_clicks")],
    [State("audit-table-filter", "value"),
     State("audit-operation-filter", "value")]
)
def update_audit_log(n_intervals, n_clicks, table_filter, operation_filter):
    try:
        with LakebaseConnection() as db:
            query = """
                SELECT
                    audit_id,
                    table_name,
                    operation,
                    record_id,
                    old_data,
                    new_data,
                    created_at,
                    created_by
                FROM ecommerce.audit_log
                WHERE 1=1
            """
            params = []

            if table_filter and table_filter != 'all':
                query += " AND table_name = %s"
                params.append(table_filter)

            if operation_filter and operation_filter != 'all':
                query += " AND operation = %s"
                params.append(operation_filter)

            query += " ORDER BY created_at DESC LIMIT 50"

            results = db.execute_query(query, params if params else None)

            if results:
                items = []
                for row in results:
                    op_class = f"audit-{row['operation'].lower()}"
                    icon = "fa-plus-circle text-success" if row['operation'] == 'INSERT' else \
                           "fa-edit text-warning" if row['operation'] == 'UPDATE' else "fa-trash text-danger"

                    time_str = row['created_at'].strftime('%Y-%m-%d %H:%M:%S') if row['created_at'] else ''

                    # Extract name from new_data if available
                    item_name = None
                    if row['new_data']:
                        try:
                            new_data = row['new_data'] if isinstance(row['new_data'], dict) else json.loads(row['new_data'])
                            item_name = new_data.get('name') or new_data.get('username') or new_data.get('email')
                        except:
                            pass

                    items.append(
                        html.Div([
                            dbc.Row([
                                dbc.Col([
                                    html.Div([
                                        html.I(className=f"fas {icon} me-2"),
                                        html.Strong(f"{row['operation']}"),
                                        html.Span(f" on ", className="text-muted"),
                                        html.Strong(row['table_name']),
                                    ]),
                                    html.Small([
                                        f"Record ID: {row['record_id']}",
                                        f" | {item_name}" if item_name else "",
                                    ], className="text-muted"),
                                ], width=8),
                                dbc.Col([
                                    html.Small(time_str, className="text-muted float-end"),
                                ], width=4),
                            ]),
                        ], className=f"audit-log-item {op_class}")
                    )

                return html.Div([
                    html.P(f"Showing {len(results)} audit records", className="text-muted mb-3"),
                    html.Div(items)
                ])
            else:
                return html.Div([
                    html.I(className="fas fa-info-circle me-2"),
                    "No audit records found. Add, update, or delete data to see the audit trail."
                ], className="text-muted p-4")
    except Exception as e:
        return html.Div([
            dbc.Alert([
                html.I(className="fas fa-exclamation-triangle me-2"),
                f"Could not load audit log. Make sure the audit_log table exists.",
                html.Br(),
                html.Small(f"Error: {str(e)}")
            ], color="warning")
        ])

# Update audit interval based on selection
@app.callback(
    Output("audit-interval", "interval"),
    Input("audit-refresh-rate", "value")
)
def update_audit_interval(rate):
    return int(rate) if int(rate) > 0 else 999999999  # Very high number to effectively disable

# Sample query selector
@app.callback(
    Output("custom-query", "value"),
    Input("sample-queries", "value")
)
def update_sample_query(query_type):
    queries = {
        "top_products": """SELECT p.name, COUNT(oi.order_item_id) as times_ordered,
       SUM(oi.quantity) as total_quantity
FROM ecommerce.products p
JOIN ecommerce.order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.name
ORDER BY times_ordered DESC
LIMIT 10;""",
        "user_history": """SELECT u.username, COUNT(o.order_id) as total_orders,
       SUM(o.total_amount) as lifetime_value
FROM ecommerce.users u
LEFT JOIN ecommerce.orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.username
ORDER BY lifetime_value DESC;""",
        "low_stock": """SELECT name, stock_quantity, category
FROM ecommerce.products
WHERE stock_quantity < 20
ORDER BY stock_quantity ASC;""",
        "revenue_category": """SELECT p.category, SUM(oi.subtotal) as total_revenue
FROM ecommerce.products p
JOIN ecommerce.order_items oi ON p.product_id = oi.product_id
JOIN ecommerce.orders o ON oi.order_id = o.order_id
WHERE o.status = 'completed'
GROUP BY p.category
ORDER BY total_revenue DESC;""",
        "audit_summary": """SELECT table_name, operation, COUNT(*) as count,
       MAX(created_at) as last_activity
FROM ecommerce.audit_log
GROUP BY table_name, operation
ORDER BY last_activity DESC;""",
        "recent_inserts": """SELECT table_name, operation, record_id,
       new_data->>'name' as item_name,
       created_at
FROM ecommerce.audit_log
WHERE operation = 'INSERT'
ORDER BY created_at DESC
LIMIT 20;"""
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

                return html.Div([
                    dbc.Alert(f"✅ Query returned {len(df)} rows", color="success"),
                    dash_table.DataTable(
                        id="query-results-table",
                        data=df.to_dict('records'),
                        columns=[{"name": str(i), "id": str(i)} for i in df.columns],
                        style_cell={'textAlign': 'left', 'padding': '12px'},
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

# ========================================
# Run the app
# ========================================
if __name__ == '__main__':
    port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))
    app.run_server(debug=True, host='0.0.0.0', port=port)
