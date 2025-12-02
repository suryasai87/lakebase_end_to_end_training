"""
Databricks Lakebase End-to-End Training Dashboard
Simple version - displays ecommerce tables from Lakebase
"""

import os
import dash
from dash import dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
import psycopg
from psycopg.rows import dict_row
import pandas as pd
import time
from databricks import sdk

# ========================================
# OAuth Token Management
# ========================================
workspace_client = None
postgres_password = None
last_password_refresh = 0

def get_oauth_token():
    """Get OAuth token from Databricks SDK."""
    global workspace_client, postgres_password, last_password_refresh

    if postgres_password is None or time.time() - last_password_refresh > 900:
        print("Refreshing OAuth token...")
        try:
            if workspace_client is None:
                workspace_client = sdk.WorkspaceClient()
            postgres_password = workspace_client.config.oauth_token().access_token
            last_password_refresh = time.time()
            print("OAuth token refreshed successfully")
        except Exception as e:
            print(f"Failed to get OAuth token: {e}")
            raise
    return postgres_password

# ========================================
# Database Configuration
# ========================================
PGHOST = os.getenv('PGHOST', 'instance-6b59171b-cee8-4acc-9209-6c848ffbfbfe.database.cloud.databricks.com')
PGDATABASE = os.getenv('PGDATABASE', 'lakebasepoc')
# Use the app's service principal ID as username (from the OAuth token)
PGUSER = os.getenv('PGUSER', '1e6260c5-f44b-4d66-bb19-ccd360f98b36')
PGPORT = os.getenv('PGPORT', '5432')

def get_db_connection():
    """Get a fresh database connection."""
    token = get_oauth_token()
    conn_string = (
        f"dbname={PGDATABASE} "
        f"user={PGUSER} "
        f"password={token} "
        f"host={PGHOST} "
        f"port={PGPORT} "
        f"sslmode=require"
    )
    print(f"Connecting to: host={PGHOST}, db={PGDATABASE}, user={PGUSER}")
    return psycopg.connect(conn_string, row_factory=dict_row)

def fetch_table_data(table_name, limit=100):
    """Fetch data from a table."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM ecommerce.{table_name} LIMIT %s", (limit,))
                rows = cur.fetchall()
                return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        print(f"Error fetching {table_name}: {e}")
        return pd.DataFrame({'error': [str(e)]})

def get_table_count(table_name):
    """Get row count for a table."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) as count FROM ecommerce.{table_name}")
                result = cur.fetchone()
                return result['count'] if result else 0
    except Exception as e:
        print(f"Error counting {table_name}: {e}")
        return 0

# ========================================
# Initialize Dash App
# ========================================
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="Lakebase Training Dashboard"
)

# ========================================
# App Layout
# ========================================
app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.H1("Lakebase End-to-End Training", className="text-primary mb-2"),
            html.P("Displaying tables from ecommerce schema", className="text-muted"),
            html.Hr()
        ])
    ], className="mt-4"),

    # Connection Status
    dbc.Row([
        dbc.Col([
            html.Div(id="connection-status", className="mb-4")
        ])
    ]),

    # Refresh Button
    dbc.Row([
        dbc.Col([
            dbc.Button("Refresh Data", id="refresh-btn", color="primary", className="mb-4"),
        ])
    ]),

    # Table Counts
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="users-count", className="card-title text-center"),
                    html.P("Users", className="card-text text-center")
                ])
            ], color="primary", outline=True)
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="products-count", className="card-title text-center"),
                    html.P("Products", className="card-text text-center")
                ])
            ], color="success", outline=True)
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="orders-count", className="card-title text-center"),
                    html.P("Orders", className="card-text text-center")
                ])
            ], color="info", outline=True)
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="order-items-count", className="card-title text-center"),
                    html.P("Order Items", className="card-text text-center")
                ])
            ], color="warning", outline=True)
        ], width=3),
    ], className="mb-4"),

    # Users Table
    dbc.Row([
        dbc.Col([
            html.H3("Users Table", className="mt-4 mb-3"),
            html.Div(id="users-table")
        ])
    ]),

    # Products Table
    dbc.Row([
        dbc.Col([
            html.H3("Products Table", className="mt-4 mb-3"),
            html.Div(id="products-table")
        ])
    ]),

    # Orders Table
    dbc.Row([
        dbc.Col([
            html.H3("Orders Table", className="mt-4 mb-3"),
            html.Div(id="orders-table")
        ])
    ]),

    # Order Items Table
    dbc.Row([
        dbc.Col([
            html.H3("Order Items Table", className="mt-4 mb-3"),
            html.Div(id="order-items-table")
        ])
    ]),

    # Hidden div for triggering initial load
    dcc.Interval(id='initial-load', interval=1000, n_intervals=0, max_intervals=1),

], fluid=True)

# ========================================
# Callbacks
# ========================================
@app.callback(
    [Output("connection-status", "children"),
     Output("users-count", "children"),
     Output("products-count", "children"),
     Output("orders-count", "children"),
     Output("order-items-count", "children"),
     Output("users-table", "children"),
     Output("products-table", "children"),
     Output("orders-table", "children"),
     Output("order-items-table", "children")],
    [Input("refresh-btn", "n_clicks"),
     Input("initial-load", "n_intervals")]
)
def update_all_data(n_clicks, n_intervals):
    """Update all table data."""
    print(f"update_all_data called: n_clicks={n_clicks}, n_intervals={n_intervals}")

    try:
        # Test connection
        print("Testing database connection...")
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                print("Connection successful!")

        status = dbc.Alert("Connected to Lakebase successfully!", color="success")

        # Get counts
        users_count = get_table_count("users")
        products_count = get_table_count("products")
        orders_count = get_table_count("orders")
        order_items_count = get_table_count("order_items")

        print(f"Counts: users={users_count}, products={products_count}, orders={orders_count}, order_items={order_items_count}")

        # Fetch table data
        users_df = fetch_table_data("users")
        products_df = fetch_table_data("products")
        orders_df = fetch_table_data("orders")
        order_items_df = fetch_table_data("order_items")

        # Create tables
        def make_table(df, table_id):
            if df.empty:
                return html.P("No data found", className="text-muted")
            if 'error' in df.columns:
                return dbc.Alert(f"Error: {df['error'].iloc[0]}", color="danger")
            return dash_table.DataTable(
                id=table_id,
                data=df.to_dict('records'),
                columns=[{"name": str(i), "id": str(i)} for i in df.columns],
                style_cell={'textAlign': 'left', 'padding': '10px', 'fontSize': '14px'},
                style_header={'backgroundColor': '#f8f9fa', 'fontWeight': 'bold'},
                style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#f8f9fa'}],
                page_size=10,
                style_table={'overflowX': 'auto'}
            )

        users_table = make_table(users_df, "users-dt")
        products_table = make_table(products_df, "products-dt")
        orders_table = make_table(orders_df, "orders-dt")
        order_items_table = make_table(order_items_df, "order-items-dt")

        return (
            status,
            str(users_count), str(products_count), str(orders_count), str(order_items_count),
            users_table, products_table, orders_table, order_items_table
        )

    except Exception as e:
        print(f"Error in update_all_data: {e}")
        import traceback
        traceback.print_exc()

        error_msg = dbc.Alert(f"Connection Error: {str(e)}", color="danger")
        empty = html.P("Unable to load data", className="text-muted")
        return (error_msg, "?", "?", "?", "?", empty, empty, empty, empty)

# ========================================
# Run the app
# ========================================
if __name__ == '__main__':
    port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))
    print(f"Starting app on port {port}")
    print(f"Database config: host={PGHOST}, db={PGDATABASE}, user={PGUSER}")
    app.run_server(debug=True, host='0.0.0.0', port=port)
