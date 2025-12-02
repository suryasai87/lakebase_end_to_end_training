"""
Simple Dashboard Example - Databricks App
==========================================
A complete, working example of a Dash dashboard deployed on Databricks Apps.

This example demonstrates:
- Proper port configuration using DATABRICKS_APP_PORT
- Environment variable usage
- Database connection (optional)
- Best practices for Databricks deployment
"""

import os
import dash
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from datetime import datetime

# ========================================
# Configuration
# ========================================
APP_TITLE = os.environ.get('APP_TITLE', 'Simple Dashboard')
DEBUG_MODE = os.environ.get('DEBUG', 'false').lower() == 'true'

# ========================================
# Initialize Dash App
# ========================================
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    title=APP_TITLE
)

# ========================================
# Sample Data
# ========================================
def get_sample_data():
    """Generate sample data for demonstration"""
    return pd.DataFrame({
        'Month': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
        'Sales': [4500, 5200, 4800, 6100, 5900, 6400],
        'Profit': [1200, 1400, 1100, 1800, 1600, 1900]
    })

# ========================================
# Layout
# ========================================
app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.H1(APP_TITLE, className="text-center text-primary my-4"),
            html.Hr()
        ])
    ]),

    # Metrics Row
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Total Sales", className="card-title"),
                    html.H2("$32,900", className="text-success"),
                    html.P("↑ 12% from last period", className="text-muted")
                ])
            ], className="shadow-sm")
        ], width=4),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Total Profit", className="card-title"),
                    html.H2("$9,000", className="text-info"),
                    html.P("↑ 15% from last period", className="text-muted")
                ])
            ], className="shadow-sm")
        ], width=4),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Profit Margin", className="card-title"),
                    html.H2("27.4%", className="text-warning"),
                    html.P("↑ 2.1% from last period", className="text-muted")
                ])
            ], className="shadow-sm")
        ], width=4),
    ], className="mb-4"),

    # Chart
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Monthly Performance", className="mb-3"),
                    dcc.Graph(id='performance-chart')
                ])
            ], className="shadow-sm")
        ])
    ], className="mb-4"),

    # Interactive Section
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Interactive Demo", className="mb-3"),
                    dbc.Label("Select Metric to Visualize:"),
                    dbc.RadioItems(
                        id='metric-selector',
                        options=[
                            {'label': 'Sales', 'value': 'Sales'},
                            {'label': 'Profit', 'value': 'Profit'}
                        ],
                        value='Sales',
                        inline=True,
                        className="mb-3"
                    ),
                    dcc.Graph(id='interactive-chart')
                ])
            ], className="shadow-sm")
        ])
    ], className="mb-4"),

    # Footer
    dbc.Row([
        dbc.Col([
            html.Hr(),
            html.P([
                "Deployed on Databricks Apps | ",
                f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            ], className="text-center text-muted")
        ])
    ])
], fluid=True, className="p-4")

# ========================================
# Callbacks
# ========================================
@callback(
    Output('performance-chart', 'figure'),
    Input('metric-selector', 'value')  # Dummy input to trigger on load
)
def update_performance_chart(_):
    df = get_sample_data()
    fig = px.bar(
        df,
        x='Month',
        y=['Sales', 'Profit'],
        barmode='group',
        title='Monthly Sales and Profit',
        labels={'value': 'Amount ($)', 'variable': 'Metric'}
    )
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(family="Arial, sans-serif")
    )
    return fig

@callback(
    Output('interactive-chart', 'figure'),
    Input('metric-selector', 'value')
)
def update_interactive_chart(selected_metric):
    df = get_sample_data()
    fig = px.line(
        df,
        x='Month',
        y=selected_metric,
        title=f'{selected_metric} Trend',
        markers=True,
        labels={selected_metric: f'{selected_metric} ($)'}
    )
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(family="Arial, sans-serif")
    )
    return fig

# ========================================
# Run the App
# ========================================
if __name__ == '__main__':
    # CRITICAL: Use DATABRICKS_APP_PORT for Databricks Apps deployment
    # This is THE key to avoiding 502 Bad Gateway errors!
    port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))

    print(f"=" * 60)
    print(f"Starting {APP_TITLE}")
    print(f"Host: 0.0.0.0")
    print(f"Port: {port}")
    print(f"Debug: {DEBUG_MODE}")
    print(f"=" * 60)

    # CRITICAL: Must bind to 0.0.0.0 for Databricks proxy
    app.run_server(debug=DEBUG_MODE, host='0.0.0.0', port=port)
