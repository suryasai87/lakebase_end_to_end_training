#!/usr/bin/env python3
"""
Setup and Deploy Script for Databricks Lakebase Training App
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Lakebase connection configuration
TOKEN = 'eyJraWQiOiI2NDZiZWZkNGY5NjYwMTdiNjk1MjRjOTRlMjcxNzljY2YyZmRlZDU1ZGJiMzQ5N2UwZjEwM2EwMzljZjI2ODU3IiwidHlwIjoiYXQrand0IiwiYWxnIjoiUlMyNTYifQ.eyJjbGllbnRfaWQiOiJkYXRhYnJpY2tzLXNlc3Npb24iLCJzY29wZSI6ImlhbS5jdXJyZW50LXVzZXI6cmVhZCBpYW0uZ3JvdXBzOnJlYWQgaWFtLnNlcnZpY2UtcHJpbmNpcGFsczpyZWFkIGlhbS51c2VyczpyZWFkIiwiaWRtIjoiRUFBPSIsImlzcyI6Imh0dHBzOi8vZTItZGVtby1maWVsZC1lbmcuY2xvdWQuZGF0YWJyaWNrcy5jb20vb2lkYyIsImF1ZCI6IjE0NDQ4MjgzMDU4MTA0ODUiLCJzdWIiOiJzdXJ5YXNhaS50dXJhZ2FAZGF0YWJyaWNrcy5jb20iLCJpYXQiOjE3NjM3NDAxMjgsImV4cCI6MTc2Mzc0MzcyOCwianRpIjoiN2VkYTk5ZTAtNWI2NC00NWVjLTkyZmQtYjAzOTlmMmIxNTU2In0.oaSX4wnqF0Je-H781pYT_nRdl99bo9PWcu5TMqcJY0-i8M1UugiN5_EM2i1aWrLjIXdBmlIdF-NdBVeBSUNgOy5z_RNg21b0H8gkzS6Gw0nnv4uqqFizLcjMkstnt9Xp2-h4kmTK7DZ5Jfr4Wl4nQKSHnII3d6uOHp4srUkc3C7HtzBcmzZON1D2Capc-JSEwytU5eGzVpZiXegfdbcjxXHNOcyY_HD6wJYRfMbdJeFqX236mVtVMYSsgyiIDTWvDF9fmYM-Z_IKaB1MFMm2O19-BFqP4MN-Wbt3H2g2U8KdPHTf-85nrd7ylVIFno3iNJwfzbq0NMoGF-G_sZiy4Q'

LAKEBASE_CONFIG = {
    'host': 'instance-868832b3-5ee5-4d06-a412-b5d13e28d853.database.cloud.databricks.com',
    'database': 'databricks_postgres',
    'user': 'token',
    'password': TOKEN,
    'port': 5432,
    'sslmode': 'require'
}

def test_connection():
    """Test database connection"""
    print("=" * 80)
    print("Testing Lakebase Database Connection")
    print("=" * 80)

    try:
        conn = psycopg2.connect(**LAKEBASE_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ Connection successful!")
        print(f"PostgreSQL version: {version[:80]}...")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def setup_database():
    """Create schema and tables"""
    print("\n" + "=" * 80)
    print("Setting Up Database Schema")
    print("=" * 80)

    try:
        conn = psycopg2.connect(**LAKEBASE_CONFIG)
        cursor = conn.cursor()

        # Read SQL setup script
        with open('setup_database.sql', 'r') as f:
            sql_script = f.read()

        # Execute SQL script
        print("\n📝 Creating schema and tables...")
        cursor.execute(sql_script)
        conn.commit()

        # Verify setup
        cursor.execute("""
            SELECT
                'Schema created' as status,
                (SELECT COUNT(*) FROM ecommerce.users) as users,
                (SELECT COUNT(*) FROM ecommerce.products) as products,
                (SELECT COUNT(*) FROM ecommerce.orders) as orders
        """)
        result = cursor.fetchone()

        print(f"✅ Database setup completed!")
        print(f"   - Users: {result[1]}")
        print(f"   - Products: {result[2]}")
        print(f"   - Orders: {result[3]}")

        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return False

def verify_data():
    """Verify data was inserted correctly"""
    print("\n" + "=" * 80)
    print("Verifying Data")
    print("=" * 80)

    try:
        conn = psycopg2.connect(**LAKEBASE_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Check users
        cursor.execute("SELECT username, email FROM ecommerce.users LIMIT 3")
        users = cursor.fetchall()
        print("\n📊 Sample Users:")
        for user in users:
            print(f"   - {user['username']} ({user['email']})")

        # Check products
        cursor.execute("SELECT name, price, category FROM ecommerce.products LIMIT 5")
        products = cursor.fetchall()
        print("\n📦 Sample Products:")
        for product in products:
            print(f"   - {product['name']}: ${float(product['price']):.2f} ({product['category']})")

        # Check orders
        cursor.execute("""
            SELECT o.order_id, u.username, o.status, o.total_amount
            FROM ecommerce.orders o
            JOIN ecommerce.users u ON o.user_id = u.user_id
            LIMIT 3
        """)
        orders = cursor.fetchall()
        print("\n🛒 Sample Orders:")
        for order in orders:
            print(f"   - Order #{order['order_id']}: {order['username']} - ${float(order['total_amount']):.2f} ({order['status']})")

        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Data verification failed: {e}")
        return False

def main():
    """Main setup and deployment process"""
    print("\n" + "=" * 80)
    print("Databricks Lakebase Training App - Setup & Deployment")
    print("=" * 80)

    print("\n📋 Configuration:")
    print(f"   Host: {LAKEBASE_CONFIG['host']}")
    print(f"   Database: {LAKEBASE_CONFIG['database']}")
    print(f"   User: {LAKEBASE_CONFIG['user']}")
    print(f"   Port: {LAKEBASE_CONFIG['port']}")

    # Test connection
    if not test_connection():
        print("\n❌ Setup aborted due to connection failure")
        return False

    # Setup database
    if not setup_database():
        print("\n❌ Setup aborted due to database setup failure")
        return False

    # Verify data
    if not verify_data():
        print("\n⚠️  Warning: Data verification failed")

    # Success message
    print("\n" + "=" * 80)
    print("✅ Setup Completed Successfully!")
    print("=" * 80)
    print("\n🚀 Next Steps:")
    print("   1. Navigate to: https://fe-vm-hls-amer.cloud.databricks.com/")
    print("   2. Go to: Repos → suryasai.turaga@databricks.com → lakebase-training")
    print("   3. Open: dash_app.py")
    print("   4. Click: Run")
    print("   5. Access your dashboard with beautiful Framer Motion animations!")
    print("\n" + "=" * 80)

    return True

if __name__ == '__main__':
    import sys
    success = main()
    sys.exit(0 if success else 1)
