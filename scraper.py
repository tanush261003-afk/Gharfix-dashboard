#!/usr/bin/env python3
"""
Gharfix Incremental Scraper - Reads from environment variables
"""

import requests
import psycopg
import time
import os

# ✅ Load from environment variables
CONFIG = {
    'DB_URL': os.getenv('DATABASE_URL'),
    'BELLEVIE_API_URL': 'https://bellevie.life/dapi/marketplace/order-leads/list',
    'COOKIE_FULL': f"_ga_M809EE54F9=GS1.1.1736145199.1.0.1736145211.0.0.0; _ga=GA1.1.1433127903.1736145200; bGH_6fJF77c={os.getenv('BELLEVIE_AUTH_COOKIE')}",
    'PAGE_SIZE': 100,
    'MAX_PAGES': 10,
}

def main():
    print(f"\n{'='*60}")
    print(f"🔄 SCRAPER RUN - {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # Validate environment variables
    if not CONFIG['DB_URL']:
        print("❌ ERROR: DATABASE_URL not set!")
        return
    
    if not os.getenv('BELLEVIE_AUTH_COOKIE'):
        print("❌ ERROR: BELLEVIE_AUTH_COOKIE not set!")
        return
    
    start_time = time.time()
    
    # Get last lead ID
    last_id = get_last_lead_id()
    print(f"📊 Last lead ID in DB: {last_id}\n")
    
    # Fetch new leads
    print("📡 Fetching new leads...")
    new_leads = fetch_new_leads_only(last_id)
    print(f"\n✅ New leads found: {len(new_leads)}\n")
    
    if len(new_leads) == 0:
        print("ℹ️  No new leads since last run\n")
        return
    
    # Insert to database
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
    except Exception as e:
        print(f"⚠️ Warning: Could not get last ID: {e}")
        return 0

def fetch_new_leads_only(last_id):
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
            
            # Filter for new leads
            page_new = [l for l in leads if l.get('customerId', 0) > last_id]
            new_leads.extend(page_new)
            print(f"✓ {len(page_new)} new")
            
            if len(page_new) == 0:
                print("   ℹ️  Reached old leads")
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
    try:
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
                print(f"   Event insert error: {e}")
        
        conn.close()
    except Exception as e:
        print(f"   ⚠️ Warning: lead_events table might not exist: {e}")
    
    return {'inserted': inserted}

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        import traceback
        traceback.print_exc()
