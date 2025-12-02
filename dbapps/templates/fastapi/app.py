"""
Databricks App Template - FastAPI Framework
=====================================================
This is a production-ready template for deploying FastAPI apps on Databricks.

Key Features:
- ✅ Proper port configuration using DATABRICKS_APP_PORT
- ✅ Host binding to 0.0.0.0 for Databricks proxy
- ✅ Environment variable support
- ✅ Modern async API ready
- ✅ Auto-generated OpenAPI docs

Author: Databricks Template
Date: 2025-11-22
"""

import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn

# ========================================
# Configuration
# ========================================
APP_TITLE = os.environ.get('APP_TITLE', 'Databricks FastAPI App')
DEBUG_MODE = os.environ.get('DEBUG', 'false').lower() == 'true'

# ========================================
# Initialize FastAPI
# ========================================
app = FastAPI(
    title=APP_TITLE,
    description="A production-ready FastAPI template for Databricks Apps",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# ========================================
# Routes
# ========================================
@app.get("/", response_class=HTMLResponse)
async def home():
    """Home page with navigation"""
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{APP_TITLE}</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                max-width: 800px;
                margin: 50px auto;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
            }}
            .container {{
                background: white;
                color: #333;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            }}
            h1 {{ color: #667eea; }}
            a {{ color: #667eea; text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>{APP_TITLE}</h1>
            <p>Welcome to your Databricks FastAPI App!</p>
            <p>This is a template for building FastAPI applications on Databricks.</p>
            <h3>API Documentation:</h3>
            <ul>
                <li><a href="/docs">Swagger UI Documentation</a></li>
                <li><a href="/redoc">ReDoc Documentation</a></li>
            </ul>
            <h3>API Endpoints:</h3>
            <ul>
                <li><a href="/health">/health</a> - Health check</li>
                <li><a href="/info">/info</a> - App information</li>
            </ul>
        </div>
    </body>
    </html>
    """
    return html_content

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "app": APP_TITLE
    }

@app.get("/info")
async def app_info():
    """Application information"""
    return {
        "app_title": APP_TITLE,
        "version": "1.0.0",
        "framework": "FastAPI",
        "debug_mode": DEBUG_MODE
    }

# Add your API routes here
# @app.get("/api/your-endpoint")
# async def your_endpoint():
#     return {"message": "Hello from your endpoint"}

# ========================================
# Run the App
# ========================================
if __name__ == "__main__":
    # CRITICAL: Use DATABRICKS_APP_PORT for Databricks Apps deployment
    port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))

    print(f"Starting FastAPI app on 0.0.0.0:{port}")
    print(f"Debug mode: {DEBUG_MODE}")
    print(f"Docs available at: http://0.0.0.0:{port}/docs")

    # CRITICAL: Must bind to 0.0.0.0
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="debug" if DEBUG_MODE else "info"
    )
