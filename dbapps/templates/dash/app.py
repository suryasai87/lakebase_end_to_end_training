"""
Databricks App Template - Dash Framework
=====================================================
This is a production-ready template for deploying Dash apps on Databricks.

Key Features:
- ✅ Proper port configuration using DATABRICKS_APP_PORT
- ✅ Host binding to 0.0.0.0 for Databricks proxy
- ✅ Environment variable support
- ✅ Bootstrap styling included
- ✅ Ready for database connections

Author: Databricks Template
Date: 2025-11-22
"""

import os
import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc

# ========================================
# Configuration
# ========================================
# Get environment variables with defaults
DEBUG_MODE = os.environ.get('DEBUG', 'False').lower() == 'true'
APP_TITLE = os.environ.get('APP_TITLE', 'Databricks Dash App')

# Database configuration (if needed)
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'database': os.environ.get('DB_NAME', 'mydb'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'port': int(os.environ.get('DB_PORT', '5432'))
}

# ========================================
# Initialize Dash App
# ========================================
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title=APP_TITLE
)

# ========================================
# Layout
# ========================================
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1(APP_TITLE, className="text-center my-4"),
            html.Hr(),
        ])
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Welcome to Your Databricks App!", className="card-title"),
                    html.P(
                        "This is a template for building Dash applications on Databricks. "
                        "Replace this content with your own components.",
                        className="card-text"
                    ),
                ])
            ])
        ])
    ]),

    dbc.Row([
        dbc.Col([
            html.Div(id="output-container", className="mt-4")
        ])
    ])
], fluid=True)

# ========================================
# Callbacks
# ========================================
# Add your callbacks here

# ========================================
# Run the App
# ========================================
if __name__ == '__main__':
    # CRITICAL: Use DATABRICKS_APP_PORT for Databricks Apps deployment
    # This prevents 502 Bad Gateway errors
    port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))

    # CRITICAL: Must bind to 0.0.0.0 for Databricks proxy to work
    host = '0.0.0.0'

    print(f"Starting Dash app on {host}:{port}")
    print(f"Debug mode: {DEBUG_MODE}")

    app.run_server(debug=DEBUG_MODE, host=host, port=port)
