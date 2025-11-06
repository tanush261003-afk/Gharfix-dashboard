import psycopg
import os

class Database:
    def __init__(self):
        # Use DATABASE_URL from environment (Neon in production, localhost in dev)
        db_url = os.getenv("DATABASE_URL")
        
        if not db_url:
            # Fallback for local development
            from config import DATABASE
            self.db_url = None
            self.config = DATABASE
        else:
            self.db_url = db_url
            self.config = None
    
    def _get_connection(self):
        """Create connection using DATABASE_URL or fallback config"""
        try:
            if self.db_url:
                # Use Neon connection string (production)
                return psycopg.connect(self.db_url, autocommit=True)
            else:
                # Use local config (development)
                return psycopg.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    dbname=self.config['database'],
                    user=self.config['user'],
                    password=self.config['password'],
                    autocommit=True
                )
        except Exception as e:
            print(f"❌ Database connection error: {e}")
            raise
    
    def initialize_schema(self):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER,
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
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(customer_id, service_id, submitted_at)
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("✓ Database schema initialized")
    
    def insert_leads(self, leads):
        """INSERT LEADS - BATCH INSERTS FOR SPEED"""
        if not leads:
            return {'inserted': 0, 'duplicates': 0}
        
        conn = self._get_connection()
        cur = conn.cursor()
        inserted = 0
        duplicates = 0
        
        # Insert in batches of 100
        batch_size = 100
        for i in range(0, len(leads), batch_size):
            batch = leads[i:i+batch_size]
            
            try:
                for lead in batch:
                    cur.execute("""
                        INSERT INTO leads (
                            customer_id, first_name, last_name, mobile_no, email,
                            comment, vendor_id, vendor_name, rate_card_name,
                            category_id, sub_category_id, service_id, service_name,
                            category_name, sub_category_name, lead_double_amount,
                            package_type, status, updated_at, submitted_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (customer_id, service_id, submitted_at) DO NOTHING
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
                
                # Commit every 100 records
                conn.commit()
                print(f"✅ Batch {i//batch_size + 1}: Inserted {inserted} leads")
                
            except Exception as e:
                print(f"Error in batch: {e}")
                conn.rollback()
                duplicates += len(batch)
        
        cur.close()
        conn.close()
        return {'inserted': inserted, 'duplicates': duplicates}
    
    def get_analytics(self):
        """Get all-time analytics"""
        conn = self._get_connection()
        cur = conn.cursor()
        
        # Total leads
        cur.execute("SELECT COUNT(*) FROM leads")
        total = cur.fetchone()[0]
        
        # Status distribution
        cur.execute("""
            SELECT status, COUNT(*) as count 
            FROM leads 
            GROUP BY status 
            ORDER BY count DESC
        """)
        status_dist = [{'status': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        # Top services - USE SUB_CATEGORY_NAME
        cur.execute("""
            SELECT 
                COALESCE(sub_category_name, 'Other') as service, 
                COUNT(*) as count 
            FROM leads 
            WHERE sub_category_name IS NOT NULL AND sub_category_name != ''
            GROUP BY sub_category_name 
            ORDER BY count DESC
            LIMIT 15
        """)
        services = [{'service_name': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        conn.close()
        
        return {
            'total_leads': total,
            'status_distribution': status_dist,
            'top_services': services,
            'time_range': 'all-time'
        }

    def get_all_leads(self):
        """Get all leads from database"""
        conn = self._get_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT * FROM leads ORDER BY submitted_at DESC")
        columns = [desc[0] for desc in cur.description]
        leads = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return [dict(zip(columns, lead)) for lead in leads]

db = Database()
