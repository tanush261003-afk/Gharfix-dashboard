#!/usr/bin/env python3
"""
INCREMENTAL Scraper - Only fetches NEW leads since last run
Efficient for scheduled runs (every hour)
"""

import requests
import psycopg
import time
from collections import Counter

CONFIG = {
    'DB_URL': 'postgresql://neondb_owner:npg_4htwi0nmEdNv@ep-sweet-union-ahmpxyfe-pooler.c-3.us-east-1.aws.neon.tech:5432/Gharfix-leads?sslmode=require',
    'BELLEVIE_API_URL': 'https://bellevie.life/dapi/marketplace/order-leads/list',
    'COOKIE_FULL': '_ga_M809EE54F9=GS1.1.1736145199.1.0.1736145211.0.0.0; _ga=GA1.1.1433127903.1736145200; bGH_6fJF77c=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbmNyeXB0ZWREYXRhIjoiODEwMDhmMDZjYmIwYjE3Yjk4NmQ3MDQ5ZDI2OTkwMTBmZmM1MTIxMTk5OTU4OGJlZDY5ODQxZTIxYTcyZjY2YjcxZjBjYWFlYmQ2NjVmMWYyZjczMzM0OGU2ZTYyNDhjZjc1YTE2MzI1MjYyNTNmMDU1Mzk4MzQ3YzljYTZmZDdiNDg3M2Q2MTVkYmZhYmEyMTZiZDRhOTY2ZGJlY2JjZWJiZmVmYTgxOWFjMzk2NmY1MDY3ZTRmZDA2YzRlZWY1N2M1NmU3N2QwMDcxNTE0ODQzMGRiYzYwM2IyNTViMWVhZjFmYmE1NjQ0NGZkMTM3M2ZmNzEwYTM2NjllNzUwYjYxMDBiMTE4ZjYyM2ZlMzQ5MzgxMmJiYzJiOGFkMzE4ZDg5ZTBlZTM2NWRjMDY3YmM2NjdiMTRmY2VlOGE2MGRhYzU4NDA3NTY2ZjMxZThiNTBkZGM1Y2E0YTlmOTNjMzBhMzc3Y2Q1NTE5Y2I3MjBhMjEyY2JhMzAxMWI3OTNlMjBiM2I5ODA0Nzg3MWZiOGFiYjI1YWU3MzkwZjhlZmU0NTE2Y2VmNGNiZjQ1MzMyMWRmNjQzZThjMDFhNmRiZDBmNTU1MmUzMmM0MzgwNThiZTNhNTljOGY5YmRmMzgzYTc3ZDY5NDM3YWI2Y2ViMmNhOWM5YjFmZmQ1MjUzNDIiLCJpdiI6IjBweWl1V3h0MXRIbzFnS2giLCJpYXQiOjE3MzYxNDUyNjksImV4cCI6MTc2NzY4MTI2OX0.92SszfLOkDF_rZDvBrNpzHgCKE5_aJau_xg0Ket5R8Q',
    'PAGE_SIZE': 100,
    'MAX_PAGES': 10,  # Only check first 10 pages (1000 leads)
}

def main():
    print("\n" + "="*60)
    print(f"🔄 INCREMENTAL SCRAPER - {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")
    
    start_time = time.time()
    
    # Get last lead ID from database
    last_id = get_last_lead_id()
    print(f"📊 Last lead ID in DB: {last_id}\n")
    
    # Fetch ONLY new leads
    print("📡 Fetching new leads from Bellevie...")
    new_leads = fetch_new_leads_only(last_id)
    print(f"\n✅ New leads found: {len(new_leads)}\n")
    
    if len(new_leads) == 0:
        print("ℹ️  No new leads since last run\n")
        return
    
    # Insert to both tables
    print("💾 Inserting to database...")
    result_leads = insert_to_leads_table(new_leads)
    result_events = insert_to_events_table(new_leads)
    
    print(f"   leads table: {result_leads['inserted']} inserted")
    print(f"   lead_events table: {result_events['inserted']} inserted\n")
    
    duration = int(time.time() - start_time)
    print(f"⏱️  Completed in {duration}s\n")

def get_last_lead_id():
    try:
        conn = psycopg.connect(CONFIG['DB_URL'])
        cur = conn.cursor()
        cur.execute("SELECT MAX(customer_id) FROM leads")
        result = cur.fetchone()
        conn.close()
        return result[0] if result and result[0] else 0
    except:
        return 0

def fetch_new_leads_only(last_id):
    """Fetch ONLY leads with customer_id > last_id"""
    new_leads = []
    page = 1
    headers = {'Cookie': CONFIG['COOKIE_FULL']}
    
    while page <= CONFIG['MAX_PAGES']:
        try:
            print(f"   Page {page}...", end=' ', flush=True)
            response = requests.post(
                CONFIG['BELLEVIE_API_URL'],
                headers=headers,
                data={'pageSize': CONFIG['PAGE_SIZE'], 'pageNumber': page},
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"❌ HTTP {response.status_code}")
                break
            
            result = response.json()
            if 'data' in result and 'data' in result['data']:
                leads = result['data']['data']
            else:
                print("❌ Bad response")
                break
            
            if not leads:
                print("✓ End")
                break
            
            # Filter for NEW leads only
            page_new = [l for l in leads if l.get('customerId', 0) > last_id]
            new_leads.extend(page_new)
            print(f"✓ {len(page_new)} new")
            
            # Stop if no new leads on this page (older leads ahead)
            if len(page_new) == 0:
                print("   ℹ️  Reached old leads, stopping")
                break
            
            page += 1
            time.sleep(0.2)
            
        except Exception as e:
            print(f"❌ {e}")
            break
    
    return new_leads

def insert_to_leads_table(leads):
    inserted = 0
    conn = psycopg.connect(CONFIG['DB_URL'])
    cur = conn.cursor()
    
    for lead in leads:
        try:
            services = lead.get('services', {})
            query = """
                INSERT INTO leads (
                    customer_id, first_name, last_name, mobile_no, email,
                    comment, vendor_id, vendor_name, rate_card_name,
                    category_id, sub_category_id, service_id, service_name,
                    category_name, sub_category_name, lead_double_amount,
                    package_type, status, submitted_at, imported_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
                )
                ON CONFLICT (customer_id) DO UPDATE SET
                    status = EXCLUDED.status, updated_at = NOW()
            """
            values = (
                lead.get('customerId'), (lead.get('firstName') or '')[:255],
                (lead.get('lastName') or '')[:255], (lead.get('mobileNo') or '')[:20],
                (lead.get('email') or '')[:255], (lead.get('comment') or '')[:1000],
                lead.get('vendorId'), (lead.get('vendorName') or '')[:255],
                (services.get('rateCardName') or '')[:255], services.get('categoryId'),
                services.get('subCategoryId'), services.get('serviceId'),
                (services.get('serviceName') or '')[:255], (services.get('categoryName') or '')[:255],
                (services.get('subCategoryName') or '')[:255], lead.get('leadDoubleAmount', False),
                (lead.get('packageType') or '')[:100], (lead.get('status') or '')[:100],
                lead.get('submittedAt'),
            )
            cur.execute(query, values)
            conn.commit()
            inserted += 1
        except Exception as e:
            print(f"   Error: {e}")
    
    conn.close()
    return {'inserted': inserted}

def insert_to_events_table(leads):
    inserted = 0
    conn = psycopg.connect(CONFIG['DB_URL'])
    cur = conn.cursor()
    
    for lead in leads:
        try:
            services = lead.get('services', {})
            event_id = f"{lead.get('customerId')}_{lead.get('submittedAt')}"
            
            query = """
                INSERT INTO lead_events (
                    event_id, customer_id, first_name, last_name, mobile_no, email,
                    comment, vendor_id, vendor_name, rate_card_name,
                    category_id, sub_category_id, service_id, service_name,
                    category_name, sub_category_name, lead_double_amount,
                    package_type, status, submitted_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (event_id) DO NOTHING
            """
            values = (
                event_id, lead.get('customerId'),
                (lead.get('firstName') or '')[:255], (lead.get('lastName') or '')[:255],
                (lead.get('mobileNo') or '')[:20], (lead.get('email') or '')[:255],
                (lead.get('comment') or '')[:1000], lead.get('vendorId'),
                (lead.get('vendorName') or '')[:255], (services.get('rateCardName') or '')[:255],
                services.get('categoryId'), services.get('subCategoryId'), services.get('serviceId'),
                (services.get('serviceName') or '')[:255], (services.get('categoryName') or '')[:255],
                (services.get('subCategoryName') or '')[:255], lead.get('leadDoubleAmount', False),
                (lead.get('packageType') or '')[:100], (lead.get('status') or '')[:100],
                lead.get('submittedAt'),
            )
            cur.execute(query, values)
            conn.commit()
            inserted += 1
        except Exception as e:
            print(f"   Error: {e}")
    
    conn.close()
    return {'inserted': inserted}

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
