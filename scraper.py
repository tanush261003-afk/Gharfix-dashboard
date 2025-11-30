import psycopg
import os
from dotenv import load_dotenv
from datetime import datetime
import logging

load_dotenv()

logger = logging.getLogger(__name__)

# Progress callback - will be injected by API
progress_callback = None

def set_progress_callback(callback):
    """Set the progress callback function"""
    global progress_callback
    progress_callback = callback

def update_progress(current, total, message=''):
    """Update progress via callback"""
    global progress_callback
    if progress_callback:
        progress_callback(current, total, message)

def rescrape_all(progress_func=None):
    """
    Rescrape all leads from Bellevie and update lead_events table
    Uses ONLY 2 existing tables (lead_events + leads)
    """
    try:
        set_progress_callback(progress_func)
        
        conn = psycopg.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        update_progress(0, 100, '🔄 Initializing rescrape...')
        
        # Import Bellevie scraper (you need to have this)
        try:
            from config import BELLEVIE_LEADS_DATA
            all_leads = BELLEVIE_LEADS_DATA
        except:
            # Fallback: fetch from database if Bellevie data not available
            logger.warning("Bellevie data not available, skipping rescrape")
            update_progress(100, 100, '⚠️ No Bellevie data available')
            return False
        
        total_leads = len(all_leads)
        update_progress(0, total_leads, f'📊 Starting rescrape of {total_leads} leads...')
        
        # Clear old data (optional - set to False to keep history)
        # cur.execute('TRUNCATE TABLE lead_events')
        
        # Batch insert for performance
        batch_size = 100
        processed = 0
        
        for i, lead in enumerate(all_leads):
            try:
                customer_id = lead.get('customer_id')
                status = lead.get('status', 'UNKNOWN')
                first_name = lead.get('first_name', '')
                last_name = lead.get('last_name', '')
                submitted_at = lead.get('submitted_at', int(datetime.now().timestamp() * 1000))
                service_name = lead.get('service_name', '')
                event_id = f"{customer_id}_{submitted_at}_{status}"
                
                # Insert into lead_events (for all events/history)
                cur.execute('''
                    INSERT INTO lead_events 
                    (event_id, customer_id, first_name, last_name, status, submitted_at, service_name, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                ''', [
                    event_id, customer_id, first_name, last_name, 
                    status, submitted_at, service_name, datetime.now()
                ])
                
                # Also update leads table (customer info)
                cur.execute('''
                    INSERT INTO leads (customer_id, first_name, last_name, created_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (customer_id) DO UPDATE
                    SET first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name
                ''', [customer_id, first_name, last_name, datetime.now()])
                
                processed += 1
                
                # Commit in batches
                if processed % batch_size == 0:
                    conn.commit()
                    percentage = int((processed / total_leads) * 100)
                    update_progress(
                        processed, total_leads, 
                        f'📥 Processed {processed}/{total_leads} ({percentage}%)...'
                    )
                    
            except Exception as e:
                logger.error(f"Error processing lead {i}: {str(e)}")
                continue
        
        # Final commit
        conn.commit()
        
        # Get final statistics
        cur.execute('SELECT COUNT(*) FROM lead_events')
        total_events = cur.fetchone()[0]
        
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM lead_events')
        unique_customers = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        update_progress(
            total_leads, total_leads,
            f'✅ Rescrape complete! {total_events} total events, {unique_customers} unique customers'
        )
        
        logger.info(f"Rescrape completed: {total_events} events, {unique_customers} customers")
        return True
        
    except Exception as e:
        logger.error(f"Error in rescrape_all: {str(e)}")
        update_progress(0, 100, f'❌ Error: {str(e)}')
        return False
