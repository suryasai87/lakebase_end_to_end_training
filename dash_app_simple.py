"""
Databricks Lakebase Training Dashboard - Simple Version (No DB Required on Startup)
A comprehensive training application showcasing Lakebase integration with modern UI
"""

import os
import dash
from dash import dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc

# ========================================
# Initialize Dash App with Bootstrap
# ========================================
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css'
    ],
    suppress_callback_exceptions=True,
    title="Lakebase Training Dashboard"
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

            .animate-fade-in {
                animation: fadeIn 0.6s ease-out;
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

            .info-card {
                background: #f8f9fa;
                border-left: 4px solid #667eea;
                padding: 20px;
                border-radius: 8px;
                margin: 20px 0;
            }

            .success-message {
                background: #d4edda;
                border-left: 4px solid #28a745;
                padding: 20px;
                border-radius: 8px;
                margin: 20px 0;
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
# Layout
# ========================================
header = html.Div([
    html.Div([
        html.H1([
            html.I(className="fas fa-database me-3"),
            "Databricks Lakebase Training Dashboard"
        ], className="mb-2"),
        html.P(
            "Welcome to your Lakebase Training Application!",
            className="mb-0 opacity-75"
        )
    ], className="header-container")
], className="animate-fade-in")

content = html.Div([
    html.Div([
        html.H3("🎉 Deployment Successful!", className="mb-3"),
        html.P([
            "Your Databricks Lakebase Training Dashboard is now running! ",
            "To connect to your Lakebase database, you need to complete the database setup."
        ], className="mb-4"),

        html.Div([
            html.H5("📋 Next Steps:", className="mb-3"),
            html.Ol([
                html.Li([
                    html.Strong("Set up your Lakebase database:"),
                    html.Br(),
                    "Run the setup script from a Databricks notebook or SQL editor:",
                    html.Pre([
                        "# In a Databricks notebook:\n",
                        "%pip install psycopg2-binary\n",
                        "%run /Workspace/Users/suryasai.turaga@databricks.com/lakebase-training/setup_and_deploy.py"
                    ], style={'background': '#f5f5f5', 'padding': '10px', 'border-radius': '5px', 'margin-top': '10px'})
                ], className="mb-3"),
                html.Li([
                    html.Strong("Verify the connection:"),
                    html.Br(),
                    "Check that your Lakebase instance is accessible from Databricks."
                ], className="mb-3"),
                html.Li([
                    html.Strong("Restart the app:"),
                    html.Br(),
                    "Once the database is set up, the full dashboard with all features will be available."
                ])
            ])
        ], className="info-card"),

        html.Div([
            html.H5("✨ Features Available After Setup:", className="mb-3"),
            html.Ul([
                html.Li("📊 Real-time Dashboard with animated metric cards"),
                html.Li("📝 Data Entry forms for products and users"),
                html.Li("🔍 Query Builder with sample and custom SQL"),
                html.Li("🔮 Vector Search demonstration"),
                html.Li("🔌 API Testing interface")
            ])
        ], className="info-card"),

        html.Div([
            html.H5("🔗 Quick Links:", className="mb-3"),
            html.Ul([
                html.Li([
                    html.Strong("GitHub Repository: "),
                    html.A("https://github.com/suryasai87/lakebase-training-app",
                           href="https://github.com/suryasai87/lakebase-training-app",
                           target="_blank")
                ]),
                html.Li([
                    html.Strong("Setup Guide: "),
                    "/Workspace/Users/suryasai.turaga@databricks.com/lakebase-training/SETUP_GUIDE.md"
                ]),
                html.Li([
                    html.Strong("Databricks Workspace: "),
                    html.A("https://fe-vm-hls-amer.cloud.databricks.com/",
                           href="https://fe-vm-hls-amer.cloud.databricks.com/",
                           target="_blank")
                ])
            ])
        ], className="info-card"),

        html.Div([
            html.H5("🗄️ Your Lakebase Connection:", className="mb-3"),
            html.P([
                html.Strong("Host: "), "instance-868832b3-5ee5-4d06-a412-b5d13e28d853.database.cloud.databricks.com", html.Br(),
                html.Strong("Database: "), "databricks_postgres", html.Br(),
                html.Strong("Port: "), "5432", html.Br(),
                html.Strong("SSL Mode: "), "require"
            ])
        ], className="info-card"),

        html.Div([
            html.I(className="fas fa-check-circle me-2"),
            html.Strong("Status: "),
            "App is running and ready for database setup!"
        ], className="success-message")

    ], className="animate-fade-in", style={'padding': '20px'})
])

app.layout = html.Div([
    header,
    html.Div([content], className="main-container")
])

# ========================================
# Run the app
# ========================================
if __name__ == '__main__':
    # Get port from environment variable for Databricks Apps compatibility
    # DATABRICKS_APP_PORT is set by Databricks Apps (typically 8000)
    # Falls back to PORT or 8080 for local development
    port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))
    app.run_server(debug=True, host='0.0.0.0', port=port)
