"""
Bellevie Scraper Module - SMART INCREMENTAL
============================================

Features:
- Saves ALL leads (no duplicate filtering)
- Auto-detects last lead ID in DB
- Only scrapes NEW leads from last saved position
- Cookie-based authentication
"""

import requests
import time
import csv
import json
from datetime import datetime
from config import BELLEVIE_AUTH_COOKIE

class BellevieScraper:
    def __init__(self):
        """Initialize scraper with authentication"""
        self.base_url = "https://bellevie.life/dapi/marketplace/order-leads/list"
        self.auth_cookie = BELLEVIE_AUTH_COOKIE
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:137.0) Gecko/20100101 Firefox/137.0',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Origin': 'https://brand.bellevie.life',
            'Referer': 'https://brand.bellevie.life/',
        })
        
        # Set authentication cookie
        self.session.cookies.set('bGH_6fJF77c', self.auth_cookie, domain='bellevie.life')

    def fetch_page(self, page_number, page_size=100):
        """Fetch one page of leads from Bellevie API"""
        try:
            url = f"{self.base_url}?page={page_number}&pageSize={page_size}"
            print(f"📄 Fetching page {page_number}...")
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            leads = data.get('leads', [])
            
            print(f"   ✓ Got {len(leads)} leads")
            return leads
            
        except Exception as e:
            print(f"   ⚠️ Error: {e}")
            return []

    def fetch_new_leads_only(self, last_lead_id=None):
        """
        Fetch ONLY new leads starting from last saved ID
        
        Args:
            last_lead_id: The highest customer_id already in database
            
        Returns:
            List of NEW lead dicts (NOT filtered for duplicates)
        """
        new_leads = []
        page = 1
        max_pages = 1000
        found_new = False
        
        print(f"\n{'='*70}")
        print(f"🔄 INCREMENTAL SCRAPE - Starting from ID: {last_lead_id or 'Beginning'}")
        print(f"{'='*70}\n")
        
        while page <= max_pages:
            leads = self.fetch_page(page)
            
            if not leads:
                print(f"✓ Reached end of data at page {page}\n")
                break
            
            for lead in leads:
                lead_id = lead.get('customerId') or lead.get('customer_id')
                
                # If we have a last_lead_id, skip until we find leads AFTER it
                if last_lead_id and lead_id <= last_lead_id:
                    # Still before our marker, skip
                    continue
                
                # Past the marker - this is NEW
                found_new = True
                new_leads.append(lead)
            
            # If we started finding new leads, and now we hit old ones, we're done
            if found_new and leads and leads[-1].get('customerId', 0) <= last_lead_id:
                print(f"✓ Finished at page {page}\n")
                break
            
            page += 1
            time.sleep(0.5)  # Be nice to the API
        
        print(f"{'='*70}")
        print(f"✅ NEW LEADS FOUND: {len(new_leads)}")
        print(f"{'='*70}\n")
        
        return new_leads

    def fetch_all_leads(self, max_pages=1000):
        """Fetch ALL leads from scratch (full rescrape)"""
        all_leads = []
        page = 1
        
        print(f"\n{'='*70}")
        print(f"🔄 FULL RESCRAPE - Starting from beginning")
        print(f"{'='*70}\n")
        
        while page <= max_pages:
            leads = self.fetch_page(page)
            
            if not leads:
                print(f"✓ Reached end at page {page}\n")
                break
            
            all_leads.extend(leads)
            page += 1
            time.sleep(0.5)
        
        print(f"{'='*70}")
        print(f"✅ TOTAL LEADS FETCHED: {len(all_leads)}")
        print(f"{'='*70}\n")
        
        return all_leads

    def export_to_csv(self, leads, filename='all_leads.csv'):
        """Export leads to CSV (save ALL leads as-is)"""
        if not leads:
            print(f"⚠️ No leads to export to {filename}")
            return
        
        try:
            fieldnames = [
                'customerId', 'firstName', 'lastName', 'mobileNo', 'email',
                'comment', 'vendorId', 'vendorName', 'rateCardName',
                'categoryId', 'subCategoryId', 'serviceId', 'serviceName',
                'categoryName', 'subCategoryName', 'leadDoubleAmount',
                'packageType', 'status', 'updatedAt', 'submittedAt'
            ]
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, restval='', extrasaction='ignore')
                writer.writeheader()
                
                for lead in leads:
                    safe_lead = {}
                    for field in fieldnames:
                        value = lead.get(field, '')
                        safe_lead[field] = '' if value is None else str(value)
                    writer.writerow(safe_lead)
            
            print(f"✅ Exported {len(leads)} leads to {filename}\n")
            
        except Exception as e:
            print(f"❌ CSV export error: {e}\n")
            import traceback
            traceback.print_exc()

    def export_to_json(self, leads, filename='all_leads.json'):
        """Export leads to JSON (save ALL leads as-is)"""
        if not leads:
            print(f"⚠️ No leads to export to {filename}")
            return
        
        try:
            fieldnames = [
                'customerId', 'firstName', 'lastName', 'mobileNo', 'email',
                'comment', 'vendorId', 'vendorName', 'rateCardName',
                'categoryId', 'subCategoryId', 'serviceId', 'serviceName',
                'categoryName', 'subCategoryName', 'leadDoubleAmount',
                'packageType', 'status', 'updatedAt', 'submittedAt'
            ]
            
            safe_leads = []
            for lead in leads:
                safe_lead = {field: lead.get(field, '') for field in fieldnames}
                safe_leads.append(safe_lead)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(safe_leads, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"✅ Exported {len(leads)} leads to {filename}\n")
            
        except Exception as e:
            print(f"❌ JSON export error: {e}\n")
            import traceback
            traceback.print_exc()

# Create global instance
scraper = BellevieScraper()
