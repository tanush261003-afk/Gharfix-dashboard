"""
Fixed Database Schema - Handles Existing Tables
✅ FIXES: created_at column error, proper migration handling
"""
import os
import psycopg
from datetime import datetime

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost/gharfix')

def init_db():
    """Initialize database schema with proper migration handling"""
    try:
        conn = psycopg.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Drop existing tables to start fresh (only if needed)
        print("Checking existing tables...")
        
        # Create leads table with all columns including timestamps
        cur.execute('''
            CREATE TABLE IF NOT EXISTS leads (
                customer_id TEXT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                phone TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Check if created_at column exists in leads table
        cur.execute('''
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='leads' AND column_name='created_at'
        ''')
        
        if not cur.fetchone():
            # Add missing columns if table exists without them
            print("Adding missing timestamp columns to leads table...")
            cur.execute('''
                ALTER TABLE leads 
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ''')
        
        # Create lead_events table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS lead_events (
                event_id SERIAL PRIMARY KEY,
                customer_id TEXT REFERENCES leads(customer_id) ON DELETE CASCADE,
                service_name TEXT,
                status TEXT,
                vendor_id TEXT,
                rate_card TEXT,
                submitted_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Check if created_at column exists in lead_events table
        cur.execute('''
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='lead_events' AND column_name='created_at'
        ''')
        
        if not cur.fetchone():
            print("Adding missing timestamp columns to lead_events table...")
            cur.execute('''
                ALTER TABLE lead_events 
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ''')
        
        # Create indices for performance
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_lead_events_customer 
            ON lead_events(customer_id)
        ''')
        
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_lead_events_status 
            ON lead_events(status)
        ''')
        
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_lead_events_service 
            ON lead_events(service_name)
        ''')
        
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_leads_created 
            ON leads(created_at)
        ''')
        
        conn.commit()
        cur.close()
        conn.close()
        
        print("✅ Database initialized successfully")
        return True
    
    except Exception as e:
        print(f"❌ Database initialization error: {e}")
        return False

def get_db():
    """Get database connection"""
    try:
        conn = psycopg.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def insert_or_update_lead(customer_id, first_name, last_name, email, phone):
    """Insert or update lead in database"""
    try:
        conn = get_db()
        if not conn:
            return False
        
        cur = conn.cursor()
        
        cur.execute('''
            INSERT INTO leads (customer_id, first_name, last_name, email, phone)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                updated_at = CURRENT_TIMESTAMP
        ''', (customer_id, first_name, last_name, email, phone))
        
        conn.commit()
        cur.close()
        conn.close()
        return True
    
    except Exception as e:
        print(f"Error inserting/updating lead: {e}")
        return False

def insert_lead_event(customer_id, service_name, status, vendor_id, rate_card, submitted_at=None):
    """Insert lead event with correct column names"""
    try:
        conn = get_db()
        if not conn:
            return False
        
        cur = conn.cursor()
        
        if not submitted_at:
            submitted_at = datetime.now()
        
        cur.execute('''
            INSERT INTO lead_events 
            (customer_id, service_name, status, vendor_id, rate_card, submitted_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (customer_id, service_name, status, vendor_id, rate_card, submitted_at))
        
        conn.commit()
        cur.close()
        conn.close()
        return True
    
    except Exception as e:
        print(f"Error inserting lead event: {e}")
        return False

def get_all_leads():
    """Get all leads"""
    try:
        conn = get_db()
        if not conn:
            return []
        
        cur = conn.cursor()
        cur.execute('SELECT * FROM leads ORDER BY created_at DESC')
        leads = cur.fetchall()
        cur.close()
        conn.close()
        
        return leads
    
    except Exception as e:
        print(f"Error fetching leads: {e}")
        return []

def get_lead_count():
    """Get total lead count"""
    try:
        conn = get_db()
        if not conn:
            return 0
        
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM leads')
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        
        return count
    
    except Exception as e:
        print(f"Error counting leads: {e}")
        return 0

def get_event_count():
    """Get total event count"""
    try:
        conn = get_db()
        if not conn:
            return 0
        
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM lead_events')
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        
        return count
    
    except Exception as e:
        print(f"Error counting events: {e}")
        return 0

if __name__ == '__main__':
    init_db()
