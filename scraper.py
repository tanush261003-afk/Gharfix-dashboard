"""
Bellevie Scraper with Real-Time Sync
✅ FIXES: Deduplication, status tracking, event counting logic
"""
import os
import requests
import json
from datetime import datetime
from database import (
    get_db, insert_or_update_lead, insert_lead_event, 
    update_lead_event_status, init_db
)

BELLEVIE_API_URL = os.getenv('BELLEVIE_API_URL', 'https://api.bellevie.app')
BELLEVIE_API_KEY = os.getenv('BELLEVIE_API_KEY', '')

def fetch_leads_from_bellevie(limit=None):
    """Fetch leads from Bellevie API"""
    try:
        print("🔄 Fetching leads from Bellevie...")
        
        headers = {
            'Authorization': f'Bearer {BELLEVIE_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        # Adjust endpoint based on your Bellevie API structure
        url = f'{BELLEVIE_API_URL}/leads'
        params = {}
        if limit:
            params['limit'] = limit
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        leads = response.json()
        if isinstance(leads, dict) and 'data' in leads:
            leads = leads['data']
        
        print(f"✅ Fetched {len(leads)} leads from Bellevie")
        return leads
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching leads from Bellevie: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing Bellevie response: {e}")
        return []

def sync_leads_to_database(leads):
    """Sync leads to database with deduplication logic"""
    try:
        init_db()  # Ensure tables exist
        
        conn = get_db()
        if not conn:
            print("❌ Database connection failed")
            return 0, 0
        
        cur = conn.cursor()
        new_count = 0
        updated_count = 0
        status_updated = 0
        
        for lead in leads:
            try:
                customer_id = lead.get('id') or lead.get('customer_id')
                first_name = lead.get('first_name', '')
                last_name = lead.get('last_name', '')
                email = lead.get('email', '')
                phone = lead.get('phone', '')
                service = lead.get('service_name', lead.get('service', 'Unknown'))
                status = lead.get('status', 'Unknown')
                vendor = lead.get('vendor', 'N/A')
                rate_card = lead.get('rate_card', 'N/A')
                submitted_at = lead.get('submitted_at')
                
                if not customer_id:
                    print(f"⚠️  Skipping lead without customer_id: {lead}")
                    continue
                
                # Check if lead exists
                cur.execute('SELECT customer_id FROM leads WHERE customer_id = %s', (customer_id,))
                lead_exists = cur.fetchone()
                
                # Insert/update lead
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
                
                if lead_exists:
                    updated_count += 1
                else:
                    new_count += 1
                
                # Check if event with same status already exists
                cur.execute('''
                    SELECT event_id FROM lead_events
                    WHERE customer_id = %s 
                    AND service_name = %s 
                    AND status = %s
                    ORDER BY submitted_at DESC
                    LIMIT 1
                ''', (customer_id, service, status))
                
                event_exists = cur.fetchone()
                
                if not event_exists:
                    # Check if status changed
                    cur.execute('''
                        SELECT status FROM lead_events
                        WHERE customer_id = %s 
                        AND service_name = %s
                        ORDER BY submitted_at DESC
                        LIMIT 1
                    ''', (customer_id, service))
                    
                    last_event = cur.fetchone()
                    if last_event and last_event[0] != status:
                        # Status changed - update last event
                        status_updated += 1
                    
                    # Insert new event
                    cur.execute('''
                        INSERT INTO lead_events 
                        (customer_id, service_name, status, vendor, rate_card, submitted_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    ''', (customer_id, service, status, vendor, rate_card, submitted_at))
                
                conn.commit()
            
            except Exception as e:
                print(f"❌ Error processing lead {customer_id}: {e}")
                conn.rollback()
                continue
        
        cur.close()
        conn.close()
        
        print(f"\n📊 Sync Complete:")
        print(f"   ✅ New leads: {new_count}")
        print(f"   🔄 Updated leads: {updated_count}")
        print(f"   📝 Status changes: {status_updated}")
        
        return new_count, updated_count
    
    except Exception as e:
        print(f"❌ Error syncing leads: {e}")
        return 0, 0

def get_analytics():
    """Get current analytics"""
    try:
        conn = get_db()
        if not conn:
            return None
        
        cur = conn.cursor()
        
        # Total lead events
        cur.execute('SELECT COUNT(*) FROM lead_events')
        total_events = cur.fetchone()[0]
        
        # Unique customers
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM leads')
        unique_customers = cur.fetchone()[0]
        
        # Status distribution
        cur.execute('''
            SELECT status, COUNT(*) as count
            FROM lead_events
            GROUP BY status
            ORDER BY count DESC
        ''')
        status_dist = {row[0]: row[1] for row in cur.fetchall()}
        
        cur.close()
        conn.close()
        
        return {
            'total_events': total_events,
            'unique_customers': unique_customers,
            'status_distribution': status_dist
        }
    
    except Exception as e:
        print(f"❌ Error getting analytics: {e}")
        return None

def full_rescrape():
    """Full rescrape cycle"""
    print("\n🚀 Starting full rescrape...")
    
    # Fetch from Bellevie
    leads = fetch_leads_from_bellevie()
    
    if not leads:
        print("❌ No leads fetched from Bellevie")
        return False
    
    # Sync to database
    new_count, updated_count = sync_leads_to_database(leads)
    
    # Get updated analytics
    analytics = get_analytics()
    if analytics:
        print(f"\n📈 Current Statistics:")
        print(f"   Total Lead Events: {analytics['total_events']}")
        print(f"   Unique Customers: {analytics['unique_customers']}")
        print(f"   Status Distribution: {analytics['status_distribution']}")
    
    print("\n✅ Rescrape completed successfully")
    return True

if __name__ == '__main__':
    full_rescrape()
