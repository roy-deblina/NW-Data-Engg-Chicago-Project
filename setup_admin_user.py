"""Create admin user for Airflow 3.x using direct database access."""
import sys
import hashlib
from sqlalchemy import create_engine, text
from airflow.configuration import conf

# Get database URL
db_url = conf.get('database', 'sql_alchemy_conn')

try:
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        # Check if admin user exists
        result = conn.execute(
            text("SELECT COUNT(*) as cnt FROM ab_user WHERE username = 'admin'")
        )
        if result.scalar() > 0:
            print("Admin user already exists")
            sys.exit(0)
        
        # Get Admin role ID
        result = conn.execute(
            text("SELECT id FROM ab_role WHERE name = 'Admin'")
        )
        admin_role_id = result.scalar()
        
        if not admin_role_id:
            print("Admin role not found. Creating it...")
            conn.execute(
                text("INSERT INTO ab_role (name) VALUES ('Admin')")
            )
            conn.commit()
            result = conn.execute(
                text("SELECT id FROM ab_role WHERE name = 'Admin'")
            )
            admin_role_id = result.scalar()
        
        # Hash password
        password_hash = hashlib.pbkdf2_hmac('sha256', b'admin', b'salt', 100000).hex()
        
        # Create admin user
        conn.execute(
            text("""
            INSERT INTO ab_user (
                username, email, first_name, last_name, 
                password, is_active, created_on, changed_on
            ) VALUES (
                'admin', 'admin@airflow.local', 'Admin', 'User',
                'pbkdf2:sha256:100000$' || :pwd_hash, true, 
                datetime('now'), datetime('now')
            )
            """),
            {"pwd_hash": password_hash}
        )
        
        conn.commit()
        
        print("Admin user created successfully!")
        print("Username: admin")
        print("Password: admin")
        print("\nAccess Airflow UI at: http://localhost:8080")
        
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
