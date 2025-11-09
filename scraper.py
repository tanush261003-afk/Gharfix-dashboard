import requests
import json
import csv
from datetime import datetime

class LeadScraper:
    def __init__(self, base_url='https://app.bellevie.in'):
        self.base_url = base_url
        self.session = requests.Session()

    def fetch_all_leads(self, max_pages=1000):
        """Fetch all leads from Bellevie API"""
        all_leads = []
        page = 1
        
        print("="*60)
        print("🔄 FETCHING ALL LEADS FROM BELLEVIE")
        print("="*60)
        
        while page <= max_pages:
            try:
                url = f"{self.base_url}/api/leads?page={page}&limit=100"
                response = self.session.get(url, timeout=15)
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                leads = data.get('leads', [])
                
                if not leads:
                    print(f"📄 Fetching page {page}... ✓ Done! (Empty page)")
                    break
                
                all_leads.extend(leads)
                print(f"📄 Fetching page {page}... ✓ {len(leads)} leads (Total: {len(all_leads)})")
                page += 1
                
            except Exception as e:
                print(f"⚠️ Error fetching page {page}: {e}")
                break
        
        print(f"\n✅ TOTAL LEADS FETCHED: {len(all_leads)}\n")
        return all_leads

    def fetch_new_leads(self, start_offset=0, max_pages=1000):
        """Fetch ONLY new leads after start_offset"""
        new_leads = []
        page = 1
        leads_found = 0
        
        print("="*60)
        print(f"🔄 FETCHING NEW LEADS (Starting from offset: {start_offset})")
        print("="*60)
        
        while page <= max_pages:
            try:
                url = f"{self.base_url}/api/leads?page={page}&limit=100"
                response = self.session.get(url, timeout=15)
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                leads = data.get('leads', [])
                
                if not leads:
                    print(f"📄 Page {page}... ✓ Done! (Empty page)")
                    break
                
                leads_found += len(leads)
                
                # Only keep leads after the offset
                if leads_found > start_offset:
                    # Calculate how many from this page to keep
                    skip_count = max(0, start_offset - (leads_found - len(leads)))
                    leads_to_add = leads[skip_count:]
                    new_leads.extend(leads_to_add)
                    
                    print(f"📄 Page {page}... ✓ {len(leads)} leads (New: {len(leads_to_add)}, Total new: {len(new_leads)})")
                else:
                    print(f"📄 Page {page}... ⏭️ Skipped (already in DB)")
                
                page += 1
                
            except Exception as e:
                print(f"⚠️ Error fetching page {page}: {e}")
                break
        
        print(f"\n✅ NEW LEADS FOUND: {len(new_leads)}\n")
        return new_leads

    def export_to_csv(self, leads, filename='all_leads.csv'):
        """Export leads to CSV - SAFE VERSION"""
        if not leads:
            print(f"⚠️ No leads to export to {filename}")
            return
        
        try:
            # Define safe fields (exclude problematic ones)
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
                    # Create a filtered copy with only safe fields
                    safe_lead = {}
                    for field in fieldnames:
                        value = lead.get(field, '')
                        # Convert None to empty string
                        safe_lead[field] = '' if value is None else str(value)
                    
                    writer.writerow(safe_lead)
            
            print(f"✅ Exported {len(leads)} leads to {filename}")
            
        except Exception as e:
            print(f"❌ CSV export error: {e}")
            import traceback
            traceback.print_exc()

    def export_to_json(self, leads, filename='all_leads.json'):
        """Export leads to JSON"""
        if not leads:
            print(f"⚠️ No leads to export to {filename}")
            return
        
        try:
            # Safe fields only
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
            
            print(f"✅ Exported {len(leads)} leads to {filename}")
            
        except Exception as e:
            print(f"❌ JSON export error: {e}")
            import traceback
            traceback.print_exc()

# Create global instance
scraper = LeadScraper()
