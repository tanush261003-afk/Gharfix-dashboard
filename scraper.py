"""
Gharfix Scraper - Bellevie Lead Data Extraction
✅ CRITICAL FIX: Inserts into leads table FIRST (foreign key requirement)
"""
import os
import json
import time
from datetime import datetime
import requests
import psycopg2
from psycopg2.extras import execute_batch

# Configuration
BELLEVIE_API = os.getenv('BELLEVIE_API', 'https://www.bellevue.live/api/')
BELLEVIE_TOKEN = os.getenv('BELLEVIE_TOKEN', 'your-token-here')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost/gharfix')
BATCH_SIZE = 100

def get_db():
    """Create database connection"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def fetch_leads():
    """Fetch leads from Bellevie API"""
    try:
        headers = {'Authorization': f'Bearer {BELLEVIE_TOKEN}'}
        response = requests.get(f'{BELLEVIE_API}leads/', headers=headers, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching leads: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error fetching leads: {e}")
        return []

def scrape_leads(progress_callback=None):
    """
    Scrape leads from Bellevie and store in database
    
    ✅ KEY FIX: Insert into leads table FIRST (foreign key requirement!)
    Then insert into lead_events table
    """
    try:
        print("🔄 Starting rescrape...")
        
        # Fetch leads from Bellevie
        leads = fetch_leads()
        if not leads:
            print("❌ No leads fetched from Bellevie")
            if progress_callback:
                progress_callback({"current": 0, "total": 0, "message": "No leads to scrape"})
            return {"status": "error", "message": "No leads fetched"}
        
        total_leads = len(leads)
        print(f"📊 Found {total_leads} leads to process")
        
        # Connect to database
        conn = get_db()
        if not conn:
            print("❌ Database connection failed")
            return {"status": "error", "message": "Database connection failed"}
        
        cur = conn.cursor()
        
        total_events = 0
        unique_customers = set()
        
        # Process in batches
        for i in range(0, total_leads, BATCH_SIZE):
            batch = leads[i:i+BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (total_leads + BATCH_SIZE - 1) // BATCH_SIZE
            
            # Report progress
            if progress_callback:
                progress_callback({
                    "current": min(i + BATCH_SIZE, total_leads),
                    "total": total_leads,
                    "message": f"Processing batch {batch_num}/{total_batches}"
                })
            
            leads_to_insert = []
            events_to_insert = []
            
            # Process each lead in batch
            for lead in batch:
                try:
                    customer_id = lead.get('id')
                    first_name = lead.get('first_name', '')
                    last_name = lead.get('last_name', '')
                    
                    if not customer_id:
                        continue
                    
                    # ✅ STEP 1: Prepare leads table INSERT
                    # (We'll insert leads FIRST to satisfy foreign key constraint)
                    leads_to_insert.append((
                        customer_id,
                        first_name,
                        last_name
                    ))
                    
                    # ✅ STEP 2: Prepare lead_events table INSERT
                    # Extract lead events/interactions
                    events = lead.get('interactions', [])
                    if not events:
                        events = [{}]  # At least one event entry
                    
                    for event in events:
                        event_id = event.get('id', f"{customer_id}_{time.time()}")
                        status = event.get('status', lead.get('status', 'UNKNOWN'))
                        submitted_at = event.get('date', lead.get('created_at', datetime.now().isoformat()))
                        service_name = event.get('service', lead.get('service', 'General'))
                        
                        events_to_insert.append((
                            str(event_id),
                            customer_id,
                            first_name,
                            last_name,
                            status,
                            submitted_at,
                            service_name
                        ))
                        
                        total_events += 1
                        unique_customers.add(customer_id)
                
                except Exception as e:
                    print(f"Error processing lead {lead.get('id')}: {e}")
                    continue
            
            # ✅ CRITICAL: Insert into leads FIRST (foreign key requirement!)
            if leads_to_insert:
                try:
                    execute_batch(
                        cur,
                        '''
                        INSERT INTO leads (customer_id, first_name, last_name)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (customer_id) DO UPDATE
                        SET first_name = EXCLUDED.first_name,
                            last_name = EXCLUDED.last_name
                        ''',
                        leads_to_insert,
                        page_size=BATCH_SIZE
                    )
                    conn.commit()
                    print(f"✅ Inserted {len(leads_to_insert)} leads into leads table")
                except Exception as e:
                    print(f"❌ Error inserting leads: {e}")
                    conn.rollback()
            
            # ✅ STEP 2: NOW insert into lead_events (customer exists!)
            if events_to_insert:
                try:
                    execute_batch(
                        cur,
                        '''
                        INSERT INTO lead_events 
                        (event_id, customer_id, first_name, last_name, status, submitted_at, service_name)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO NOTHING
                        ''',
                        events_to_insert,
                        page_size=BATCH_SIZE
                    )
                    conn.commit()
                    print(f"✅ Inserted {len(events_to_insert)} events into lead_events table")
                except Exception as e:
                    print(f"❌ Error inserting events: {e}")
                    conn.rollback()
        
        cur.close()
        conn.close()
        
        message = f"✅ Rescrape complete! {total_events} total events, {len(unique_customers)} unique customers"
        print(message)
        
        if progress_callback:
            progress_callback({
                "current": total_leads,
                "total": total_leads,
                "message": message
            })
        
        return {
            "status": "success",
            "message": message,
            "total_events": total_events,
            "unique_customers": len(unique_customers)
        }
    
    except Exception as e:
        print(f"❌ Fatal error during scrape: {e}")
        return {"status": "error", "message": str(e)}

def test_connection():
    """Test database and API connections"""
    print("🔍 Testing connections...")
    
    # Test database
    conn = get_db()
    if conn:
        print("✅ Database connection: OK")
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM lead_events")
        count = cur.fetchone()[0]
        print(f"   - Lead events in DB: {count}")
        cur.close()
        conn.close()
    else:
        print("❌ Database connection: FAILED")
    
    # Test Bellevie API
    try:
        headers = {'Authorization': f'Bearer {BELLEVIE_TOKEN}'}
        response = requests.head(f'{BELLEVIE_API}leads/', headers=headers, timeout=10)
        if response.status_code in [200, 405]:
            print("✅ Bellevie API connection: OK")
        else:
            print(f"❌ Bellevie API connection: Status {response.status_code}")
    except Exception as e:
        print(f"❌ Bellevie API connection: FAILED ({e})")

if __name__ == '__main__':
    # Test connections
    test_connection()
    
    # Run scrape
    print("\n" + "="*50)
    result = scrape_leads()
    print("="*50)
    print(f"Result: {result}")
