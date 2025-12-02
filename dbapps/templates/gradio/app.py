"""
Databricks App Template - Gradio Framework
=====================================================
This is a production-ready template for deploying Gradio apps on Databricks.

Key Features:
- ✅ Proper port configuration using DATABRICKS_APP_PORT
- ✅ Host binding to 0.0.0.0 for Databricks proxy
- ✅ Environment variable support
- ✅ ML model deployment ready

Author: Databricks Template
Date: 2025-11-22
"""

import os
import gradio as gr

# ========================================
# Configuration
# ========================================
APP_TITLE = os.environ.get('APP_TITLE', 'Databricks Gradio App')

# ========================================
# App Functions
# ========================================
def greet(name):
    """Example function - replace with your logic"""
    return f"Hello {name}! Welcome to {APP_TITLE}"

# ========================================
# Gradio Interface
# ========================================
demo = gr.Interface(
    fn=greet,
    inputs=gr.Textbox(label="Enter your name", placeholder="Your name here..."),
    outputs=gr.Textbox(label="Greeting"),
    title=APP_TITLE,
    description="This is a template for building Gradio applications on Databricks. Replace this with your own interface.",
    theme=gr.themes.Soft(),
)

# ========================================
# Run the App
# ========================================
if __name__ == "__main__":
    # CRITICAL: Use DATABRICKS_APP_PORT for Databricks Apps deployment
    port = int(os.environ.get('DATABRICKS_APP_PORT', os.environ.get('PORT', '8080')))

    print(f"Starting Gradio app on 0.0.0.0:{port}")

    # Launch the app
    demo.launch(
        server_name="0.0.0.0",  # CRITICAL: Must bind to 0.0.0.0
        server_port=port,
        share=False,
        show_error=True
    )
