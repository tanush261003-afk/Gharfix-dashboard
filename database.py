import psycopg
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def init_database():
    """Initialize database - NO 3RD TABLE! Just use lead_events"""
    try:
        conn = psycopg.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        print("🚀 Initializing database...")
        
        # ============ TABLE 1: lead_events ============
        print("📝 Creating lead_events table...")
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
        
        # ============ TABLE 2: leads ============
        print("👥 Creating leads table...")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS leads (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER UNIQUE,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # ============ Create Indexes ============
        print("🔍 Creating indexes...")
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_lead_events_customer_id 
            ON lead_events(customer_id)
        ''')
        
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_lead_events_status 
            ON lead_events(status)
        ''')
        
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_lead_events_submitted_at 
            ON lead_events(submitted_at)
        ''')
        
        conn.commit()
        print("✅ Database tables created successfully!")
        
        # ============ Get Statistics ============
        print("\n📊 Getting database statistics...")
        
        cur.execute('SELECT COUNT(*) FROM lead_events')
        total_events = cur.fetchone()[0]
        print(f"✅ Total Lead Events: {total_events}")
        
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM lead_events')
        unique_customers = cur.fetchone()[0]
        print(f"✅ Unique Customers: {unique_customers}")
        
        cur.execute('''
            SELECT DISTINCT ON (customer_id) status
            FROM lead_events
            ORDER BY customer_id, submitted_at DESC
        ''')
        
        latest_statuses = cur.fetchall()
        status_counts = {}
        for row in latest_statuses:
            status = row[0]
            status_counts[status] = status_counts.get(status, 0) + 1
        
        print(f"\n📊 Status Breakdown (Latest Status Per Customer):")
        for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {status}: {count}")
        
        total_sum = sum(status_counts.values())
        print(f"\n✅ Total Unique Customers: {total_sum}")
        print(f"💾 Total Event Records: {total_events}")
        print(f"\n🎯 Dashboard will show:")
        print(f"  - Total Events: {total_events} (matches Bellevie)")
        print(f"  - Unique Customers: {unique_customers}")
        print(f"  - Status counts: Latest status per customer")
        
        cur.close()
        conn.close()
        
        return True
    
    except Exception as e:
        print(f"❌ Error initializing database: {str(e)}")
        return False

def get_lead_statistics():
    """Get lead statistics"""
    try:
        conn = psycopg.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        # Total events
        cur.execute('SELECT COUNT(*) FROM lead_events')
        total_events = cur.fetchone()[0]
        
        # Unique customers
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM lead_events')
        unique_customers = cur.fetchone()[0]
        
        # Latest status per customer
        cur.execute('''
            SELECT DISTINCT ON (customer_id) status
            FROM lead_events
            ORDER BY customer_id, submitted_at DESC
        ''')
        
        latest_statuses = cur.fetchall()
        status_counts = {}
        for row in latest_statuses:
            status = row[0]
            status_counts[status] = status_counts.get(status, 0) + 1
        
        cur.close()
        conn.close()
        
        return {
            'total_events': total_events,
            'unique_customers': unique_customers,
            'status_breakdown': status_counts
        }
    
    except Exception as e:
        print(f"❌ Error getting statistics: {str(e)}")
        return None

if __name__ == '__main__':
    init_database()
    stats = get_lead_statistics()
    if stats:
        print("\n" + "="*50)
        print("✅ DATABASE READY!")
        print("="*50)
