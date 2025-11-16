#!/usr/bin/env python3
"""
Gharfix Full Rescrape - Fetches ALL leads efficiently
"""

import requests
import psycopg
import time
import os
from datetime import datetime

# Config
CONFIG = {
    'DB_URL': os.getenv('DATABASE_URL'),
    'BELLEVIE_API_URL': 'https://bellevie.life/dapi/marketplace/order-leads/list',
    'COOKIE_FULL': f"_ga_M809EE54F9=GS1.1.1736145199.1.0.1736145211.0.0.0; _ga=GA1.1.1433127903.1736145200; bGH_6fJF77c={os.getenv('BELLEVIE_AUTH_COOKIE')}",
    'PAGE_SIZE': 100,
    'MAX_PAGES': 50,  # Fetch up to 5000 leads
}

def main():
    print(f"\n{'='*70}")
    print(f"🔄 GHARFIX RESCRAPER - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    if not CONFIG['DB_URL'] or not os.getenv('BELLEVIE_AUTH_COOKIE'):
        print("❌ ERROR: Environment variables not set!")
        return
    
    start_time = time.time()
    
    # Get database stats
    db_stats = get_db_stats()
    print(f"📊 Database Before:")
    print(f"   - Lead events: {db_stats['events']}")
    print(f"   - Unique customers: {db_stats['customers']}")
    print(f"   - Last customer ID: {db_stats['last_id']}\n")
    
    # Fetch all leads
    print("📡 Fetching leads from Bellevie...\n")
    all_leads = fetch_all_leads()
    
    if not all_leads:
        print("\n⚠️  No leads fetched\n")
        return
    
    print(f"\n✅ Fetched {len(all_leads)} leads from Bellevie\n")
    
    # Insert to database
    print("💾 Updating database...\n")
    result_leads = insert_to_leads(all_leads)
    result_events = insert_to_events(all_leads)
    
    # Final stats
    db_stats_after = get_db_stats()
    
    duration = int(time.time() - start_time)
    
    print(f"\n{'='*70}")
    print(f"✅ SCRAPE COMPLETE!")
    print(f"{'='*70}")
    print(f"📊 Results:")
    print(f"   - Leads table: {result_leads['new']} new, {result_leads['updated']} updated")
    print(f"   - Events table: {result_events['new']} new, {result_events['duplicates']} duplicates")
    print(f"\n📊 Database After:")
    print(f"   - Total events: {db_stats_after['events']} (+{db_stats_after['events'] - db_stats['events']})")
    print(f"   - Unique customers: {db_stats_after['customers']}")
    print(f"\n⏱️  Duration: {duration}s")
    print(f"{'='*70}\n")

def get_db_stats():
    """Get current database statistics"""
    try:
        conn = psycopg.connect(CONFIG['DB_URL'])
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM lead_events")
        events = cur.fetchone()[0] or 0
        
        cur.execute("SELECT COUNT(DISTINCT customer_id) FROM lead_events")
        customers = cur.fetchone()[0] or 0
        
        cur.execute("SELECT MAX(customer_id) FROM leads")
        last_id = cur.fetchone()[0] or 0
        
        conn.close()
        return {'events': events, 'customers': customers, 'last_id': last_id}
    except Exception as e:
        print(f"⚠️  Error getting stats: {e}")
        return {'events': 0, 'customers': 0, 'last_id': 0}

def fetch_all_leads():
    """Fetch ALL leads from Bellevie"""
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
            leads = result.get('data', {}).get('data', [])
            
            if not leads:
                print("✓ END (no more leads)")
                break
            
            print(f"✓ {len(leads):3d} leads")
            all_leads.extend(leads)
            
            page += 1
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            print(f"❌ {str(e)[:50]}")
            break
    
    return all_leads

def insert_to_leads(leads):
    """Insert/update leads table (latest status per customer)"""
    new_count = 0
    updated_count = 0
    
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
                    status = EXCLUDED.status,
                    updated_at = NOW()
                RETURNING (xmax = 0) AS is_insert
            """
            
            values = (
                lead.get('customerId'),
                (lead.get('firstName') or '')[:255],
                (lead.get('lastName') or '')[:255],
                (lead.get('mobileNo') or '')[:20],
                (lead.get('email') or '')[:255],
                (lead.get('comment') or '')[:1000],
                lead.get('vendorId'),
                (lead.get('vendorName') or '')[:255],
                (services.get('rateCardName') or '')[:255],
                services.get('categoryId'),
                services.get('subCategoryId'),
                services.get('serviceId'),
                (services.get('serviceName') or '')[:255],
                (services.get('categoryName') or '')[:255],
                (services.get('subCategoryName') or '')[:255],
                lead.get('leadDoubleAmount', False),
                (lead.get('packageType') or '')[:100],
                (lead.get('status') or '')[:100],
                lead.get('submittedAt'),
            )
            
            cur.execute(query, values)
            result = cur.fetchone()
            
            if result and result[0]:
                new_count += 1
            else:
                updated_count += 1
            
            conn.commit()
            
            if (i + 1) % 500 == 0:
                print(f"      [{i+1}/{len(leads)}] {new_count} new, {updated_count} updated")
        
        except Exception as e:
            print(f"      ⚠️  Error with lead {lead.get('customerId')}: {str(e)[:50]}")
    
    conn.close()
    return {'new': new_count, 'updated': updated_count}

def insert_to_events(leads):
    """Insert all events into lead_events table"""
    new_count = 0
    dup_count = 0
    
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
                    event_id,
                    lead.get('customerId'),
                    (lead.get('firstName') or '')[:255],
                    (lead.get('lastName') or '')[:255],
                    (lead.get('mobileNo') or '')[:20],
                    (lead.get('email') or '')[:255],
                    (lead.get('comment') or '')[:1000],
                    lead.get('vendorId'),
                    (lead.get('vendorName') or '')[:255],
                    (services.get('rateCardName') or '')[:255],
                    services.get('categoryId'),
                    services.get('subCategoryId'),
                    services.get('serviceId'),
                    (services.get('serviceName') or '')[:255],
                    (services.get('categoryName') or '')[:255],
                    (services.get('subCategoryName') or '')[:255],
                    lead.get('leadDoubleAmount', False),
                    (lead.get('packageType') or '')[:100],
                    (lead.get('status') or '')[:100],
                    lead.get('submittedAt'),
                )
                
                cur.execute(query, values)
                
                if cur.rowcount > 0:
                    new_count += 1
                else:
                    dup_count += 1
                
                conn.commit()
                
                if (i + 1) % 500 == 0:
                    print(f"      [{i+1}/{len(leads)}] {new_count} new, {dup_count} duplicates")
            
            except Exception as e:
                print(f"      ⚠️  Event error for {lead.get('customerId')}: {str(e)[:50]}")
        
        conn.close()
    
    except Exception as e:
        print(f"   ⚠️  Events table error: {e}")
    
    return {'new': new_count, 'duplicates': dup_count}

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted\n")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}\n")
        import traceback
        traceback.print_exc()
