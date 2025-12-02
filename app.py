# sample_databricks_app.py
"""
Sample Databricks App with Lakebase Integration
This is a complete example application for the training
"""

import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import plotly.express as px
import os
from datetime import datetime
import json

# ========================================
# Database Configuration
# ========================================
LAKEBASE_CONFIG = {
    'host': os.environ.get('LAKEBASE_HOST', 'ep-training-lakebase.us-east-2.aws.neon.tech'),
    'database': os.environ.get('LAKEBASE_DB', 'trainingdb'),
    'user': os.environ.get('LAKEBASE_USER'),
    'password': os.environ.get('LAKEBASE_PASSWORD'),
    'port': 5432,
    'sslmode': 'require'
}

# ========================================
# Database Connection Manager
# ========================================
class LakebaseConnection:
    """Manage Lakebase database connections with context manager support"""
    
    def __init__(self, config=None):
        self.config = config or LAKEBASE_CONFIG
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
            self.connection = psycopg2.connect(**self.config)
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            return True
        except Exception as e:
            st.error(f"Connection failed: {e}")
            return False
    
    def execute_query(self, query, params=None):
        """Execute a query and return results"""
        try:
            self.cursor.execute(query, params)
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
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
# Streamlit Application
# ========================================
def main():
    st.set_page_config(
        page_title="Lakebase Training Dashboard",
        page_icon="🚀",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🚀 Databricks Lakebase Training Dashboard")
    st.markdown("---")
    
    # Sidebar for operations
    with st.sidebar:
        st.header("🔧 Operations")
        
        operation = st.selectbox(
            "Select Operation",
            ["Dashboard", "Data Entry", "Query Builder", "Vector Search", "API Testing"]
        )
        
        st.markdown("---")
        st.markdown("### 📚 Training Resources")
        st.markdown("[📖 Documentation](https://neon.tech/docs)")
        st.markdown("[💻 GitHub Examples](https://github.com/neondatabase)")
        st.markdown("[🎥 Video Tutorials](https://youtube.com/neondatabase)")
    
    # Main content area
    if operation == "Dashboard":
        show_dashboard()
    elif operation == "Data Entry":
        show_data_entry()
    elif operation == "Query Builder":
        show_query_builder()
    elif operation == "Vector Search":
        show_vector_search()
    elif operation == "API Testing":
        show_api_testing()

def show_dashboard():
    """Display main dashboard with metrics and charts"""
    st.header("📊 Dashboard Overview")
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with LakebaseConnection() as db:
        # Get metrics
        try:
            users_count = db.execute_query(
                "SELECT COUNT(*) as count FROM ecommerce.users"
            )[0]['count']
            
            products_count = db.execute_query(
                "SELECT COUNT(*) as count FROM ecommerce.products"
            )[0]['count']
            
            orders_count = db.execute_query(
                "SELECT COUNT(*) as count FROM ecommerce.orders"
            )[0]['count']
            
            revenue = db.execute_query("""
                SELECT COALESCE(SUM(total_amount), 0) as revenue 
                FROM ecommerce.orders 
                WHERE status='completed'
            """)[0]['revenue']
            
            with col1:
                st.metric("Total Users", f"{users_count:,}")
            with col2:
                st.metric("Total Products", f"{products_count:,}")
            with col3:
                st.metric("Total Orders", f"{orders_count:,}")
            with col4:
                st.metric("Revenue", f"${revenue:,.2f}")
            
            # Product inventory chart
            st.subheader("📦 Product Inventory")
            products_df = pd.DataFrame(db.execute_query("""
                SELECT name, stock_quantity, category 
                FROM ecommerce.products 
                ORDER BY stock_quantity DESC 
                LIMIT 10
            """))
            
            if not products_df.empty:
                fig = px.bar(
                    products_df, 
                    x='name', 
                    y='stock_quantity', 
                    color='category',
                    title="Top 10 Products by Stock",
                    labels={'stock_quantity': 'Stock Quantity', 'name': 'Product Name'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Recent orders
            st.subheader("🛒 Recent Orders")
            orders_df = pd.DataFrame(db.execute_query("""
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
            """))
            
            if not orders_df.empty:
                st.dataframe(
                    orders_df,
                    use_container_width=True,
                    hide_index=True
                )
                
        except Exception as e:
            st.error(f"Error loading dashboard: {e}")
            st.info("Please ensure the database tables are created and populated.")

def show_data_entry():
    """Show data entry form"""
    st.header("📝 Data Entry")
    
    tab1, tab2, tab3 = st.tabs(["Add Product", "Add User", "Create Order"])
    
    with tab1:
        with st.form("product_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                name = st.text_input("Product Name*")
                description = st.text_area("Description")
                category = st.selectbox(
                    "Category",
                    ["Electronics", "Accessories", "Books", "Clothing", "Other"]
                )
            
            with col2:
                price = st.number_input("Price*", min_value=0.01, step=0.01)
                stock = st.number_input("Stock Quantity", min_value=0, step=1)
                tags = st.text_input("Tags (comma-separated)")
            
            submitted = st.form_submit_button("Add Product", type="primary")
            
            if submitted:
                if name and price:
                    with LakebaseConnection() as db:
                        try:
                            tags_array = [tag.strip() for tag in tags.split(',')] if tags else []
                            
                            db.execute_query("""
                                INSERT INTO ecommerce.products 
                                (name, description, price, stock_quantity, category, tags)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (name, description, price, stock, category, tags_array))
                            
                            st.success("✅ Product added successfully!")
                            st.balloons()
                        except Exception as e:
                            st.error(f"Error adding product: {e}")
                else:
                    st.error("Please fill in required fields (marked with *)")
    
    with tab2:
        with st.form("user_form"):
            email = st.text_input("Email*")
            username = st.text_input("Username*")
            full_name = st.text_input("Full Name")
            role = st.selectbox("Role", ["customer", "admin", "vendor"])
            
            submitted = st.form_submit_button("Add User", type="primary")
            
            if submitted:
                if email and username:
                    with LakebaseConnection() as db:
                        try:
                            metadata = json.dumps({"role": role})
                            db.execute_query("""
                                INSERT INTO ecommerce.users 
                                (email, username, full_name, metadata)
                                VALUES (%s, %s, %s, %s::jsonb)
                            """, (email, username, full_name, metadata))
                            
                            st.success("✅ User added successfully!")
                        except Exception as e:
                            st.error(f"Error adding user: {e}")
                else:
                    st.error("Please fill in required fields")

def show_query_builder():
    """Interactive query builder"""
    st.header("🔍 Query Builder")
    
    # Predefined queries
    st.subheader("Sample Queries")
    
    queries = {
        "Top selling products": """
            SELECT p.name, COUNT(oi.order_item_id) as times_ordered, 
                   SUM(oi.quantity) as total_quantity
            FROM ecommerce.products p
            JOIN ecommerce.order_items oi ON p.product_id = oi.product_id
            GROUP BY p.product_id, p.name
            ORDER BY times_ordered DESC
            LIMIT 10
        """,
        "User purchase history": """
            SELECT u.username, COUNT(o.order_id) as total_orders,
                   SUM(o.total_amount) as lifetime_value
            FROM ecommerce.users u
            LEFT JOIN ecommerce.orders o ON u.user_id = o.user_id
            GROUP BY u.user_id, u.username
            ORDER BY lifetime_value DESC
        """,
        "Low stock alert": """
            SELECT name, stock_quantity, category
            FROM ecommerce.products
            WHERE stock_quantity < 10
            ORDER BY stock_quantity ASC
        """
    }
    
    selected_query = st.selectbox("Select a sample query", list(queries.keys()))
    
    # Custom query input
    st.subheader("Custom Query")
    custom_query = st.text_area(
        "Enter your SQL query",
        value=queries[selected_query],
        height=150
    )
    
    if st.button("Execute Query", type="primary"):
        with LakebaseConnection() as db:
            try:
                results = db.execute_query(custom_query)
                
                if results:
                    df = pd.DataFrame(results)
                    st.success(f"✅ Query executed successfully. Found {len(df)} rows.")
                    st.dataframe(df, use_container_width=True)
                    
                    # Download button
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download as CSV",
                        data=csv,
                        file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("Query executed successfully but returned no results.")
                    
            except Exception as e:
                st.error(f"Query error: {e}")

def show_vector_search():
    """Vector search demonstration"""
    st.header("🔮 Vector Search with pg_vector")
    
    st.info("""
    This section demonstrates semantic search using pg_vector extension.
    Vector embeddings enable AI-powered search based on meaning rather than exact matches.
    """)
    
    # Search interface
    search_query = st.text_input("Enter your search query", placeholder="e.g., laptop for AI development")
    
    col1, col2 = st.columns(2)
    with col1:
        search_type = st.radio("Search Type", ["Semantic", "Hybrid", "Traditional"])
    with col2:
        top_k = st.slider("Number of results", min_value=1, max_value=20, value=5)
    
    if st.button("Search", type="primary"):
        if search_query:
            # Simulate vector search results
            st.subheader("Search Results")
            
            # In a real implementation, this would call the embedding service
            # and perform actual vector similarity search
            
            sample_results = [
                {
                    "title": "AI Development Laptop Pro",
                    "description": "High-performance laptop with GPU for machine learning",
                    "similarity": 0.95,
                    "price": 1999.99
                },
                {
                    "title": "Data Science Workstation",
                    "description": "Powerful workstation for data analysis and AI",
                    "similarity": 0.87,
                    "price": 2499.99
                },
                {
                    "title": "Machine Learning Book Bundle",
                    "description": "Complete guide to AI and deep learning",
                    "similarity": 0.72,
                    "price": 99.99
                }
            ]
            
            for i, result in enumerate(sample_results[:top_k], 1):
                with st.container():
                    col1, col2, col3 = st.columns([3, 1, 1])
                    with col1:
                        st.markdown(f"**{i}. {result['title']}**")
                        st.text(result['description'])
                    with col2:
                        st.metric("Similarity", f"{result['similarity']:.2%}")
                    with col3:
                        st.metric("Price", f"${result['price']:.2f}")
                    st.divider()
        else:
            st.warning("Please enter a search query")

def show_api_testing():
    """PostgREST API testing interface"""
    st.header("🔌 API Testing")
    
    st.info("Test PostgREST API endpoints for your Lakebase database")
    
    # API endpoint configuration
    api_base = st.text_input("API Base URL", value="http://localhost:3000")
    
    # Request builder
    col1, col2 = st.columns([1, 2])
    
    with col1:
        method = st.selectbox("HTTP Method", ["GET", "POST", "PATCH", "DELETE"])
        endpoint = st.text_input("Endpoint", value="/products")
    
    with col2:
        if method in ["POST", "PATCH"]:
            request_body = st.text_area(
                "Request Body (JSON)",
                value='{\n  "name": "New Product",\n  "price": 99.99\n}',
                height=100
            )
        
        params = st.text_input("Query Parameters", placeholder="e.g., category=eq.Electronics")
    
    if st.button("Send Request", type="primary"):
        url = f"{api_base}{endpoint}"
        if params:
            url += f"?{params}"
        
        st.code(f"{method} {url}", language="http")
        
        # In a real implementation, this would make actual API calls
        st.success("✅ Request sent successfully")
        
        # Sample response
        st.subheader("Response")
        sample_response = {
            "status": 200,
            "data": [
                {
                    "product_id": 1,
                    "name": "Sample Product",
                    "price": 99.99,
                    "category": "Electronics"
                }
            ]
        }
        
        st.json(sample_response)

# ========================================
# Run the application
# ========================================
if __name__ == "__main__":
    main()
