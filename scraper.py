#!/usr/bin/env python3
"""
Gharfix FULL Scraper - Fetches ALL leads and populates both tables
Use this for rescrape button or initial load
"""

import requests
import psycopg
import time
import os

CONFIG = {
    'DB_URL': os.getenv('DATABASE_URL'),
    'BELLEVIE_API_URL': 'https://bellevie.life/dapi/marketplace/order-leads/list',
    'COOKIE_FULL': f"_ga_M809EE54F9=GS1.1.1736145199.1.0.1736145211.0.0.0; _ga=GA1.1.1433127903.1736145200; bGH_6fJF77c={os.getenv('BELLEVIE_AUTH_COOKIE')}",
    'PAGE_SIZE': 100,
    'MAX_PAGES': 100,  # Fetch up to 10,000 leads
}

def main():
    print(f"\n{'='*60}")
    print(f"🔄 FULL SCRAPER - {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # Validate environment variables
    if not CONFIG['DB_URL']:
        print("❌ ERROR: DATABASE_URL not set!")
        return
    
    if not os.getenv('BELLEVIE_AUTH_COOKIE'):
        print("❌ ERROR: BELLEVIE_AUTH_COOKIE not set!")
        return
    
    start_time = time.time()
    
    # Fetch ALL leads
    print("📡 Fetching ALL leads from Bellevie...")
    all_leads = fetch_all_leads()
    print(f"\n✅ Total leads fetched: {len(all_leads)}\n")
    
    if len(all_leads) == 0:
        print("ℹ️  No leads found\n")
        return
    
    # Insert to both tables
    print("💾 Inserting to database...")
    result_leads = insert_to_leads_table(all_leads)
    result_events = insert_to_events_table(all_leads)
    
    print(f"   leads table: {result_leads['inserted']} new, {result_leads['updated']} updated")
    print(f"   lead_events table: {result_events['inserted']} new events\n")
    
    duration = int(time.time() - start_time)
    print(f"⏱️  Completed in {duration}s\n")

def fetch_all_leads():
    """Fetch ALL leads from Bellevie API"""
    all_leads = []
    page = 1
    headers = {'Cookie': CONFIG['COOKIE_FULL']}
    
    while page <= CONFIG['MAX_PAGES']:
        try:
            print(f"   Page {page:3d}...", end=' ', flush=True)
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
            
            if not leads or len(leads) == 0:
                print("✓ END")
                break
            
            print(f"✓ {len(leads):3d} leads")
            all_leads.extend(leads)
            page += 1
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            print(f"❌ {e}")
            break
    
    return all_leads

def insert_to_leads_table(leads):
    """Insert/update leads in 'leads' table (latest status per customer)"""
    inserted = 0
    updated = 0
    
    conn = psycopg.connect(CONFIG['DB_URL'])
    cur = conn.cursor()
    
    for i, lead in enumerate(leads):
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
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    mobile_no = EXCLUDED.mobile_no,
                    email = EXCLUDED.email,
                    comment = EXCLUDED.comment,
                    status = EXCLUDED.status,
                    vendor_name = EXCLUDED.vendor_name,
                    submitted_at = EXCLUDED.submitted_at,
                    updated_at = NOW()
                RETURNING (xmax = 0) AS is_insert
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
            
            # Check if it was an insert or update
            result = cur.fetchone()
            if result and result[0]:
                inserted += 1
            else:
                updated += 1
            
            conn.commit()
            
            # Progress indicator
            if (i + 1) % 100 == 0:
                print(f"     Progress: {i+1}/{len(leads)}")
        
        except Exception as e:
            print(f"     ⚠️ Error inserting lead {lead.get('customerId')}: {e}")
    
    conn.close()
    return {'inserted': inserted, 'updated': updated}

def insert_to_events_table(leads):
    """Insert ALL events into 'lead_events' table"""
    inserted = 0
    duplicates = 0
    
    try:
        conn = psycopg.connect(CONFIG['DB_URL'])
        cur = conn.cursor()
        
        for i, lead in enumerate(leads):
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
                
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    duplicates += 1
                
                conn.commit()
                
                # Progress indicator
                if (i + 1) % 100 == 0:
                    print(f"     Progress: {i+1}/{len(leads)} ({inserted} new events)")
            
            except Exception as e:
                print(f"     ⚠️ Event insert error for {lead.get('customerId')}: {e}")
        
        conn.close()
    
    except Exception as e:
        print(f"   ⚠️ Warning: lead_events table error: {e}")
    
    return {'inserted': inserted, 'duplicates': duplicates}

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user\n")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}\n")
        import traceback
        traceback.print_exc()
