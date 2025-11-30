import psycopg
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def init_database():
    """Initialize database and create tables"""
    try:
        conn = psycopg.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        print("🚀 Initializing database structure...")
        
        # ============ TABLE 1: lead_events (FULL HISTORY) ============
        print("📝 Creating lead_events table (full history)...")
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
        
        # ============ TABLE 2: lead_latest_status (CURRENT STATE) ============
        print("📊 Creating lead_latest_status table (latest status only)...")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS lead_latest_status (
                id SERIAL PRIMARY KEY,
                lead_id INTEGER NOT NULL UNIQUE,
                customer_id INTEGER,
                current_status VARCHAR(50),
                previous_status VARCHAR(50),
                service_name VARCHAR(100),
                last_updated TIMESTAMP DEFAULT NOW(),
                event_count INTEGER DEFAULT 1
            )
        ''')
        
        # ============ TABLE 3: leads (CUSTOMER INFO) ============
        print("👥 Creating leads table (customer info)...")
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
            CREATE INDEX IF NOT EXISTS idx_lead_latest_status_customer_id 
            ON lead_latest_status(customer_id)
        ''')
        
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_lead_latest_status_status 
            ON lead_latest_status(current_status)
        ''')
        
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_lead_latest_status_lead_id 
            ON lead_latest_status(lead_id)
        ''')
        
        conn.commit()
        print("✅ Database tables created successfully!")
        
        # ============ Populate lead_latest_status ============
        print("\n📊 Populating lead_latest_status with latest statuses...")
        
        cur.execute('''
            INSERT INTO lead_latest_status (lead_id, customer_id, current_status, last_updated, event_count)
            SELECT 
                customer_id as lead_id,
                customer_id,
                status as current_status,
                to_timestamp(MAX(submitted_at)::double precision / 1000) as last_updated,
                COUNT(*) as event_count
            FROM lead_events
            GROUP BY customer_id, status
            ON CONFLICT (lead_id) DO UPDATE
            SET current_status = EXCLUDED.current_status,
                last_updated = EXCLUDED.last_updated,
                event_count = EXCLUDED.event_count
        ''')
        
        conn.commit()
        
        # Get statistics
        print("\n" + "="*50)
        print("📈 DATABASE STATISTICS")
        print("="*50)
        
        cur.execute('SELECT COUNT(*) FROM lead_events')
        total_events = cur.fetchone()[0]
        print(f"✅ Total Lead Events (history): {total_events}")
        
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM lead_latest_status')
        unique_leads = cur.fetchone()[0]
        print(f"✅ Unique Leads: {unique_leads}")
        
        cur.execute('''
            SELECT current_status, COUNT(*) as count
            FROM lead_latest_status
            GROUP BY current_status
            ORDER BY count DESC
        ''')
        
        print(f"\n📊 Status Breakdown (Latest Status Only):")
        status_breakdown = cur.fetchall()
        for status, count in status_breakdown:
            print(f"  {status}: {count}")
        
        print(f"\n✅ Total Unique Leads: {unique_leads}")
        print(f"💾 Total Event Records: {total_events}")
        print(f"\n🎯 Dashboard will show:")
        print(f"  - Total Events: {total_events} (matches Bellevie)")
        print(f"  - Unique Customers: {unique_leads}")
        print(f"  - Status counts: Latest status per lead")
        
        cur.close()
        conn.close()
        
        return True
    
    except Exception as e:
        print(f"❌ Error initializing database: {str(e)}")
        return False

def update_lead_status(customer_id, new_status, service_name=''):
    """Update a lead's status to latest"""
    try:
        conn = psycopg.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        # Get old status
        cur.execute('''
            SELECT current_status 
            FROM lead_latest_status 
            WHERE customer_id = %s
        ''', [customer_id])
        
        result = cur.fetchone()
        old_status = result[0] if result else None
        
        # Update/insert latest status
        cur.execute('''
            INSERT INTO lead_latest_status 
            (lead_id, customer_id, current_status, previous_status, service_name, last_updated)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (lead_id) DO UPDATE
            SET current_status = %s,
                previous_status = %s,
                last_updated = NOW()
        ''', [customer_id, customer_id, new_status, old_status, service_name, new_status, old_status])
        
        conn.commit()
        cur.close()
        conn.close()
        
        return True
    
    except Exception as e:
        print(f"❌ Error updating status: {str(e)}")
        return False

def get_lead_statistics():
    """Get complete statistics about leads"""
    try:
        conn = psycopg.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        # Total events
        cur.execute('SELECT COUNT(*) FROM lead_events')
        total_events = cur.fetchone()[0]
        
        # Unique leads
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM lead_latest_status')
        unique_leads = cur.fetchone()[0]
        
        # Status breakdown
        cur.execute('''
            SELECT current_status, COUNT(*) as count
            FROM lead_latest_status
            GROUP BY current_status
            ORDER BY count DESC
        ''')
        status_breakdown = cur.fetchall()
        
        # Top services
        cur.execute('''
            SELECT service_name, COUNT(*) as count
            FROM lead_latest_status
            WHERE service_name IS NOT NULL AND service_name != ''
            GROUP BY service_name
            ORDER BY count DESC
            LIMIT 20
        ''')
        services = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return {
            'total_events': total_events,
            'unique_leads': unique_leads,
            'status_breakdown': dict(status_breakdown),
            'services': services
        }
    
    except Exception as e:
        print(f"❌ Error getting statistics: {str(e)}")
        return None

if __name__ == '__main__':
    init_database()
    stats = get_lead_statistics()
    if stats:
        print("\n" + "="*50)
        print("✅ SYNC COMPLETE!")
        print("="*50)
