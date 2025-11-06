"""
Celery Tasks Module
===================
Defines async tasks for scraping and analytics updates
"""

from celery import shared_task
from database import db
from scraper import scraper
import time

@shared_task
def scrape_all_leads_task():
    """
    Celery task: Scrape all leads from Bellevie and store in database
    """
    try:
        # Fetch all leads
        leads = scraper.fetch_all_leads()
        
        if not leads:
            return {'status': 'error', 'message': 'No leads fetched'}
        
        # Insert into database
        result = db.insert_leads(leads)
        
        # Export to files
        scraper.export_to_csv(leads, 'all_leads.csv')
        scraper.export_to_json(leads, 'all_leads.json')
        
        return {
            'status': 'success',
            'message': f"Scraped {len(leads)} leads",
            'inserted': result['inserted'],
            'duplicates': result['duplicates']
        }
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

@shared_task
def scrape_new_leads_task():
    """
    Celery task: Scrape new leads from last 0.2 hours
    """
    try:
        leads = scraper.fetch_new_leads(hours=0.2)
        
        if not leads:
            return {'status': 'success', 'message': 'No new leads', 'count': 0}
        
        # Insert into database
        result = db.insert_leads(leads)
        
        # Re-export all leads
        all_leads = db.get_all_leads()
        scraper.export_to_csv(all_leads, 'all_leads.csv')
        scraper.export_to_json(all_leads, 'all_leads.json')
        
        return {
            'status': 'success',
            'message': f"Added {len(leads)} new leads",
            'inserted': result['inserted'],
            'duplicates': result['duplicates']
        }
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

@shared_task
def update_analytics_task():
    """
    Celery task: Update analytics cache
    """
    try:
        analytics = db.get_analytics()
        return {'status': 'success', 'data': analytics}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}
