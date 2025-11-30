import requests
import psycopg
import os
from dotenv import load_dotenv
import json
from datetime import datetime
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_bellevie_leads():
    """Fetch leads from Bellevie website"""
    try:
        print("🔗 Connecting to Bellevie...")
        
        # Headers with auth
        headers = {
            'Cookie': os.getenv('BELLEVIE_AUTH_COOKIE'),
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Fetch data from Bellevie
        response = requests.get(
            'https://www.belleviegroup.com/leads',
            headers=headers,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"❌ Failed to fetch from Bellevie: {response.status_code}")
            return []
        
        print(f"✅ Connected to Bellevie (Status: {response.status_code})")
        
        # Parse leads from response (adjust selectors based on actual HTML)
        leads = parse_bellevie_html(response.text)
        print(f"📥 Fetched {len(leads)} leads from Bellevie")
        
        return leads
    
    except Exception as e:
        logger.error(f"Error fetching from Bellevie: {str(e)}")
        print(f"❌ Error: {str(e)}")
        return []

def parse_bellevie_html(html_content):
    """Parse leads from Bellevie HTML"""
    from bs4 import BeautifulSoup
    
    leads = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all lead rows (adjust selector based on actual HTML)
        lead_rows = soup.find_all('tr', {'class': 'lead-row'})
        
        for row in lead_rows:
            try:
                # Extract data (adjust selectors based on actual HTML)
                customer_id = row.find('td', {'class': 'lead-id'})
                name = row.find('td', {'class': 'lead-name'})
                status = row.find('td', {'class': 'lead-status'})
                service = row.find('td', {'class': 'lead-service'})
                timestamp = row.find('td', {'class': 'lead-timestamp'})
                
                if customer_id and status:
                    lead = {
                        'customer_id': int(customer_id.text.strip()),
                        'first_name': name.text.split()[0] if name else '',
                        'last_name': name.text.split()[1] if name and len(name.text.split()) > 1 else '',
                        'status': status.text.strip(),
                        'service_name': service.text.strip() if service else '',
                        'submitted_at': int(timestamp.get_text()) if timestamp else int(datetime.now().timestamp() * 1000),
                        'event_id': f"{customer_id.text.strip()}_{timestamp.text.strip()}"
                    }
                    leads.append(lead)
            
            except Exception as e:
                logger.warning(f"Error parsing lead row: {str(e)}")
                continue
        
        return leads
    
    except Exception as e:
        logger.error(f"Error parsing HTML: {str(e)}")
        return []

def sync_with_database(leads):
    """
    Smart sync: 
    1. Add new leads
    2. Update existing leads' latest status
    3. Keep full history
    4. Total count = Bellevie count
    """
    try:
        conn = psycopg.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        if not leads:
            print("❌ No leads to sync")
            return 0
        
        print(f"\n🔄 Syncing {len(leads)} leads with database...")
        
        added = 0
        updated = 0
        
        for lead in leads:
            try:
                # 1. Insert into lead_events (FULL HISTORY)
                cur.execute('''
                    INSERT INTO lead_events 
                    (event_id, customer_id, first_name, last_name, status, submitted_at, service_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                ''', [
                    lead['event_id'],
                    lead['customer_id'],
                    lead['first_name'],
                    lead['last_name'],
                    lead['status'],
                    lead['submitted_at'],
                    lead['service_name']
                ])
                
                # 2. Update lead_latest_status (CURRENT STATE ONLY)
                cur.execute('''
                    SELECT current_status FROM lead_latest_status 
                    WHERE lead_id = %s
                ''', [lead['customer_id']])
                
                old_status_result = cur.fetchone()
                old_status = old_status_result[0] if old_status_result else None
                
                # Insert or update
                cur.execute('''
                    INSERT INTO lead_latest_status 
                    (lead_id, customer_id, current_status, previous_status, service_name, last_updated)
                    VALUES (%s, %s, %s, %s, %s, to_timestamp(%s::double precision / 1000))
                    ON CONFLICT (lead_id) DO UPDATE
                    SET current_status = %s,
                        previous_status = %s,
                        service_name = %s,
                        last_updated = to_timestamp(%s::double precision / 1000)
                ''', [
                    lead['customer_id'],           # lead_id
                    lead['customer_id'],           # customer_id
                    lead['status'],                # current_status
                    old_status,                    # previous_status
                    lead['service_name'],          # service_name
                    lead['submitted_at'],          # last_updated
                    lead['status'],                # UPDATE: current_status
                    old_status,                    # UPDATE: previous_status
                    lead['service_name'],          # UPDATE: service_name
                    lead['submitted_at']           # UPDATE: last_updated
                ])
                
                added += 1
            
            except Exception as e:
                logger.warning(f"Error syncing lead {lead['customer_id']}: {str(e)}")
                continue
        
        conn.commit()
        
        # Get updated counts
        cur.execute('SELECT COUNT(*) FROM lead_events')
        total_events = cur.fetchone()[0]
        
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM lead_latest_status')
        unique_leads = cur.fetchone()[0]
        
        cur.execute('''
            SELECT current_status, COUNT(*) as count
            FROM lead_latest_status
            GROUP BY current_status
            ORDER BY count DESC
        ''')
        
        status_breakdown = cur.fetchall()
        
        cur.close()
        conn.close()
        
        print(f"\n✅ Sync Complete!")
        print(f"📊 Total Events: {total_events}")
        print(f"👥 Unique Customers: {unique_leads}")
        print(f"\n📈 Status Breakdown (Latest Status Only):")
        for status, count in status_breakdown:
            print(f"  {status}: {count}")
        
        return unique_leads
    
    except Exception as e:
        logger.error(f"Error syncing database: {str(e)}")
        print(f"❌ Sync error: {str(e)}")
        return 0

def rescrape_all():
    """Full rescrape: Fetch from Bellevie and sync to database"""
    print("\n" + "="*60)
    print("🚀 STARTING FULL RESCRAPE FROM BELLEVIE")
    print("="*60)
    
    # 1. Fetch from Bellevie
    leads = fetch_bellevie_leads()
    
    if not leads:
        print("❌ No leads fetched from Bellevie")
        return False
    
    # 2. Sync to database
    unique_count = sync_with_database(leads)
    
    if unique_count > 0:
        print("\n" + "="*60)
        print("🎉 RESCRAPE SUCCESSFUL!")
        print("="*60)
        return True
    else:
        print("\n❌ Rescrape failed")
        return False

if __name__ == '__main__':
    rescrape_all()
