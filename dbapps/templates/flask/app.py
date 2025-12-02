"""
Databricks App Template - Flask Framework
=====================================================
This is a production-ready template for deploying Flask apps on Databricks.

Key Features:
- ✅ Proper port configuration using DATABRICKS_APP_PORT
- ✅ Host binding to 0.0.0.0 for Databricks proxy
- ✅ Environment variable support
- ✅ RESTful API ready

Author: Databricks Template
Date: 2025-11-22
"""

import os
from flask import Flask, render_template_string, jsonify

# ========================================
# Configuration
# ========================================
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
APP_TITLE = os.environ.get('APP_TITLE', 'Databricks Flask App')

# ========================================
# Routes
# ========================================
@app.route('/')
def home():
    """Home page"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>{{ title }}</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                max-width: 800px;
                margin: 50px auto;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
            }
            .container {
                background: white;
                color: #333;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            }
            h1 { color: #667eea; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>{{ title }}</h1>
            <p>Welcome to your Databricks Flask App!</p>
            <p>This is a template for building Flask applications on Databricks.</p>
            <h3>API Endpoints:</h3>
            <ul>
                <li><a href="/api/health">/api/health</a> - Health check</li>
                <li><a href="/api/info">/api/info</a> - App information</li>
            </ul>
        </div>
    </body>
    </html>
    """
    return render_template_string(html, title=APP_TITLE)

@app.route('/api/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'app': APP_TITLE
    })

@app.route('/api/info')
def info():
    """App information endpoint"""
    return jsonify({
        'app_title': APP_TITLE,
        'version': '1.0.0',
        'framework': 'Flask'
    })

# ========================================
# Run the App
# ========================================
if __name__ == '__main__':
    # CRITICAL: Use DATABRICKS_APP_PORT for Databricks Apps deployment
    port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))
    debug = os.environ.get('DEBUG', 'false').lower() == 'true'

    print(f"Starting Flask app on 0.0.0.0:{port}")
    print(f"Debug mode: {debug}")

    # CRITICAL: Must bind to 0.0.0.0
    app.run(host='0.0.0.0', port=port, debug=debug)
