import psycopg
import os
from dotenv import load_dotenv

load_dotenv()

def init_database():
    """Initialize database and create tables"""
    try:
        conn = psycopg.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        # Create lead_events table (for full history/audit trail)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS lead_events (
                id SERIAL PRIMARY KEY,
                event_id VARCHAR(100) UNIQUE,
                customer_id INTEGER,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                status VARCHAR(50),
                submitted_at BIGINT,
                service_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Create lead_current_status table (LATEST status only)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS lead_current_status (
                id SERIAL PRIMARY KEY,
                lead_id INTEGER NOT NULL UNIQUE,
                customer_id INTEGER,
                current_status VARCHAR(50),
                previous_status VARCHAR(50),
                last_updated TIMESTAMP DEFAULT NOW(),
                change_reason VARCHAR(255)
            )
        ''')
        
        # Create leads table (unique customers)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS leads (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER UNIQUE,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Create indexes for performance
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_lead_events_customer_id 
            ON lead_events(customer_id)
        ''')
        
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_lead_events_status 
            ON lead_events(status)
        ''')
        
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_lead_current_status_customer_id 
            ON lead_current_status(customer_id)
        ''')
        
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_lead_current_status_status 
            ON lead_current_status(current_status)
        ''')
        
        conn.commit()
        print("✅ Database initialized successfully!")
        
        # Populate lead_current_status with latest status per lead
        print("📊 Populating lead_current_status with latest statuses...")
        
        cur.execute('''
            INSERT INTO lead_current_status (lead_id, customer_id, current_status, last_updated)
            SELECT 
                customer_id as lead_id,
                customer_id,
                status as current_status,
                to_timestamp(MAX(submitted_at)::double precision / 1000) as last_updated
            FROM lead_events
            GROUP BY customer_id, status
            ON CONFLICT (lead_id) DO UPDATE
            SET current_status = EXCLUDED.current_status,
                last_updated = EXCLUDED.last_updated
        ''')
        
        conn.commit()
        
        # Verify population
        cur.execute('SELECT COUNT(*) FROM lead_current_status')
        count = cur.fetchone()[0]
        print(f"✅ Populated {count} records in lead_current_status")
        
        cur.close()
        conn.close()
        
        return True
    
    except Exception as e:
        print(f"❌ Error initializing database: {str(e)}")
        return False

def get_lead_statistics():
    """Get statistics about leads"""
    try:
        conn = psycopg.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        # Total unique leads
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM lead_current_status')
        total_unique = cur.fetchone()[0]
        
        # Status breakdown
        cur.execute('''
            SELECT current_status, COUNT(*) as count
            FROM lead_current_status
            GROUP BY current_status
            ORDER BY count DESC
        ''')
        status_breakdown = cur.fetchall()
        
        cur.close()
        conn.close()
        
        print("\n📊 Lead Statistics:")
        print(f"Total Unique Leads: {total_unique}")
        print("\nStatus Breakdown:")
        for status, count in status_breakdown:
            print(f"  {status}: {count}")
        
        return {
            'total_unique': total_unique,
            'status_breakdown': dict(status_breakdown)
        }
    
    except Exception as e:
        print(f"❌ Error getting statistics: {str(e)}")
        return None

if __name__ == '__main__':
    init_database()
    get_lead_statistics()
