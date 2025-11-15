import psycopg
import os

class Database:
    def __init__(self):
        from config import DATABASE
        self.config = DATABASE
    
    def _get_connection(self):
        return psycopg.connect(
            host=self.config['host'],
            port=self.config['port'],
            dbname=self.config['database'],
            user=self.config['user'],
            password=self.config['password']
        )
    
    def initialize_schema(self):
        """Create both leads and lead_events tables"""
        conn = self._get_connection()
        cur = conn.cursor()
        
        # Main leads table (latest status per customer)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER UNIQUE,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                mobile_no VARCHAR(20),
                email VARCHAR(255),
                comment TEXT,
                vendor_id INTEGER,
                vendor_name VARCHAR(255),
                rate_card_name VARCHAR(255),
                category_id INTEGER,
                sub_category_id INTEGER,
                service_id INTEGER,
                service_name VARCHAR(255),
                category_name VARCHAR(255),
                sub_category_name VARCHAR(255),
                lead_double_amount BOOLEAN,
                package_type VARCHAR(100),
                status VARCHAR(100),
                updated_at TIMESTAMP,
                submitted_at BIGINT,
                imported_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Lead events table (all events with history)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS lead_events (
                id SERIAL PRIMARY KEY,
                event_id VARCHAR(100) UNIQUE,
                customer_id BIGINT NOT NULL,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                mobile_no VARCHAR(20),
                email VARCHAR(255),
                comment TEXT,
                vendor_id INTEGER,
                vendor_name VARCHAR(255),
                rate_card_name VARCHAR(255),
                category_id INTEGER,
                sub_category_id INTEGER,
                service_id INTEGER,
                service_name VARCHAR(255),
                category_name VARCHAR(255),
                sub_category_name VARCHAR(255),
                lead_double_amount BOOLEAN,
                package_type VARCHAR(100),
                status VARCHAR(100),
                submitted_at BIGINT,
                imported_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Create indexes for faster queries
        cur.execute("CREATE INDEX IF NOT EXISTS idx_leads_customer_id ON leads(customer_id DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_customer_id ON lead_events(customer_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_status ON lead_events(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_submitted_at ON lead_events(submitted_at)")
        
        conn.commit()
        conn.close()
        print("✅ Database schema initialized")
    
    def get_last_lead_id(self):
        """Get the highest customer_id already in database"""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute("SELECT MAX(customer_id) FROM leads")
            result = cur.fetchone()[0]
            conn.close()
            return result or 0
        except Exception as e:
            print(f"⚠️ Error getting last lead ID: {e}")
            return 0
    
    def insert_leads(self, leads):
        """
        Insert leads into database
        - Updates leads table with latest status
        - Inserts all events into lead_events table
        """
        if not leads:
            return {'inserted': 0, 'duplicates': 0, 'errors': 0}
        
        conn = self._get_connection()
        cur = conn.cursor()
        
        inserted = 0
        duplicates = 0
        errors = 0
        
        for lead in leads:
            try:
                # Insert/update in leads table (latest status per customer)
                cur.execute("""
                    INSERT INTO leads (
                        customer_id, first_name, last_name, mobile_no, email,
                        comment, vendor_id, vendor_name, rate_card_name,
                        category_id, sub_category_id, service_id, service_name,
                        category_name, sub_category_name, lead_double_amount,
                        package_type, status, updated_at, submitted_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        mobile_no = EXCLUDED.mobile_no,
                        email = EXCLUDED.email,
                        comment = EXCLUDED.comment,
                        status = EXCLUDED.status,
                        updated_at = NOW()
                """, (
                    lead.get('customerId'),
                    lead.get('firstName'),
                    lead.get('lastName'),
                    lead.get('mobileNo'),
                    lead.get('email'),
                    lead.get('comment'),
                    lead.get('vendorId'),
                    lead.get('vendorName'),
                    lead.get('rateCardName'),
                    lead.get('categoryId'),
                    lead.get('subCategoryId'),
                    lead.get('serviceId'),
                    lead.get('serviceName'),
                    lead.get('categoryName'),
                    lead.get('subCategoryName'),
                    lead.get('leadDoubleAmount'),
                    lead.get('packageType'),
                    lead.get('status'),
                    lead.get('updatedAt'),
                    lead.get('submittedAt')
                ))
                
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    duplicates += 1
            
            except Exception as e:
                print(f"❌ Error inserting lead {lead.get('customerId')}: {e}")
                errors += 1
        
        conn.commit()
        conn.close()
        
        print(f"\n📊 Database Insert Results:")
        print(f" ✅ Inserted: {inserted}")
        print(f" ⏭️ Duplicates (updated): {duplicates}")
        print(f" ❌ Errors: {errors}\n")
        
        return {
            'inserted': inserted,
            'duplicates': duplicates,
            'errors': errors
        }
    
    def get_leads_count(self):
        """Get total unique leads in database"""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM leads")
            count = cur.fetchone()[0]
            conn.close()
            return count
        except Exception as e:
            print(f"⚠️ Error getting lead count: {e}")
            return 0
    
    def get_events_count(self):
        """Get total events in database"""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM lead_events")
            count = cur.fetchone()[0]
            conn.close()
            return count
        except Exception as e:
            print(f"⚠️ Error getting event count: {e}")
            return 0

# Create global instance
db = Database()
