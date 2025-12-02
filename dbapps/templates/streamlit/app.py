"""
Databricks App Template - Streamlit Framework
=====================================================
This is a production-ready template for deploying Streamlit apps on Databricks.

Key Features:
- ✅ Streamlit auto-configures port via DATABRICKS_APP_PORT
- ✅ Environment variable support
- ✅ Database connection ready
- ✅ Session state management

Author: Databricks Template
Date: 2025-11-22
"""

import streamlit as st
import os

# ========================================
# Page Configuration
# ========================================
st.set_page_config(
    page_title=os.environ.get('APP_TITLE', 'Databricks Streamlit App'),
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========================================
# Database Configuration (if needed)
# ========================================
DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'port': int(os.environ.get('DB_PORT', '5432'))
}

# ========================================
# Main App
# ========================================
def main():
    st.title("🚀 Databricks Streamlit App")
    st.markdown("---")

    # Sidebar
    with st.sidebar:
        st.header("Settings")
        st.markdown("Configure your app settings here")

    # Main content
    st.header("Welcome!")
    st.write("""
    This is a template for building Streamlit applications on Databricks.
    Replace this content with your own components.
    """)

    # Example: Display environment info
    with st.expander("Environment Information"):
        st.json({
            "App Title": os.environ.get('APP_TITLE', 'Not Set'),
            "Debug Mode": os.environ.get('DEBUG', 'false'),
            "Database Host": DB_CONFIG['host'] or 'Not Configured'
        })

    # Add your app content here

if __name__ == "__main__":
    main()

# Note: Streamlit automatically uses DATABRICKS_APP_PORT when run via:
# streamlit run app.py --server.address 0.0.0.0
