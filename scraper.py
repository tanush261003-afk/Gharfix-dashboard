"""
Bellevie Scraper Module
=======================
Handles Bellevie API communication and lead fetching
"""

import requests
import time
import csv
import json
from datetime import datetime, timedelta
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
        """Fetch one page of leads from API"""
        try:
            form_data = {
                'pageNumber': str(page_number),
                'pageSize': str(page_size)
            }
            response = self.session.post(self.base_url, data=form_data, timeout=30)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                raise Exception("❌ Authentication failed! Cookie expired or invalid!")
            else:
                raise Exception(f"HTTP {response.status_code}: {response.text}")
        except Exception as e:
            raise Exception(f"Error fetching page {page_number}: {e}")
    
    def flatten_services_data(self, lead):
        """Extract nested services data and flatten it"""
        # Extract services object if it exists
        services = lead.get('services', {})
        if isinstance(services, dict):
            lead['serviceId'] = services.get('serviceId')
            lead['subCategoryId'] = services.get('subCategoryId')
            lead['categoryId'] = services.get('categoryId')
            lead['serviceName'] = services.get('serviceName')
            lead['categoryName'] = services.get('categoryName')
            lead['subCategoryName'] = services.get('subCategoryName')
            lead['rateCardName'] = services.get('rateCardName')
        
        # Remove the nested services object
        if 'services' in lead:
            del lead['services']
        
        return lead
    
    def fetch_all_leads(self):
        """Fetch ALL leads from Bellevie with pagination"""
        print(f"\n{'='*60}")
        print(f"🔄 FETCHING ALL LEADS FROM BELLEVIE")
        print(f"{'='*60}")
        
        all_leads = []
        page = 1
        page_size = 100
        
        while True:
            try:
                print(f"📄 Fetching page {page}...", end='')
                data = self.fetch_page(page, page_size=page_size)
                
                if not data or 'data' not in data:
                    print(" ❌ No data")
                    break
                
                leads = data.get('data', {}).get('data', [])
                if not leads:
                    print(f" ✓ Done! (Empty page)")
                    break
                
                # Flatten services data for each lead
                leads = [self.flatten_services_data(lead) for lead in leads]
                
                all_leads.extend(leads)
                print(f" ✓ {len(leads)} leads (Total: {len(all_leads)})")
                
                page += 1
                time.sleep(0.3)
                
            except Exception as e:
                print(f" ❌ Error: {e}")
                break
        
        print(f"\n✅ TOTAL LEADS FETCHED: {len(all_leads)}")
        return all_leads
    
    def fetch_new_leads(self, hours=0.2):
        """Fetch new leads from last N hours"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Checking for new leads (last {hours}h)...")
        
        # Fetch first few pages (new leads are at top)
        all_leads = []
        for page in range(1, 4):
            try:
                data = self.fetch_page(page, page_size=100)
                if not data or 'data' not in data:
                    break
                leads = data.get('data', {}).get('data', [])
                if not leads:
                    break
                # Flatten services data
                leads = [self.flatten_services_data(lead) for lead in leads]
                all_leads.extend(leads)
                time.sleep(0.5)
            except Exception as e:
                print(f"Error on page {page}: {e}")
                break
        
        # Filter to only leads from last N hours
        cutoff_time = datetime.now() - timedelta(hours=hours)
        cutoff_ms = int(cutoff_time.timestamp() * 1000)
        
        new_leads = [
            lead for lead in all_leads
            if lead.get('submittedAt', 0) >= cutoff_ms
        ]
        
        print(f"Found {len(new_leads)} new leads")
        return new_leads
    
    def export_to_csv(self, leads, filename='all_leads.csv'):
        """Export leads to CSV"""
        if not leads:
            print("No leads to export")
            return
        
        try:
            keys = leads[0].keys()
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(leads)
            print(f"✓ Exported {len(leads)} leads to {filename}")
        except Exception as e:
            print(f"Error exporting CSV: {e}")
    
    def export_to_json(self, leads, filename='all_leads.json'):
        """Export leads to JSON - remove null values"""
        if not leads:
            print("No leads to export")
            return
        
        try:
            # Remove null values from each lead
            clean_leads = []
            for lead in leads:
                clean_lead = {k: v for k, v in lead.items() if v is not None}
                clean_leads.append(clean_lead)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(clean_leads, f, indent=2, ensure_ascii=False, default=str)
            print(f"✓ Exported {len(leads)} leads to {filename} (null values removed)")
        except Exception as e:
            print(f"Error exporting JSON: {e}")

# Create global instance
scraper = BellevieScraper()
