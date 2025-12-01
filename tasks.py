"""
Celery Tasks for Background Jobs
✅ FIXES: Progress tracking, rescrape status
"""
from celery import shared_task, Task
from celery_app import celery_app
import time
from scraper import full_rescrape, fetch_leads_from_bellevie, sync_leads_to_database

class CallbackTask(Task):
    def on_retry(self, exc, task_id, args, kwargs, einfo):
        print(f'Task {task_id} is being retried')

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print(f'Task {task_id} failed: {exc}')

    def on_success(self, result, task_id, args, kwargs):
        print(f'Task {task_id} succeeded')

@shared_task(bind=True, base=CallbackTask, max_retries=3)
def rescrape_task(self):
    """Full rescrape task with progress tracking"""
    try:
        total_steps = 2
        
        # Step 1: Fetch leads
        self.update_state(
            state='PROGRESS',
            meta={
                'current': 1,
                'total': total_steps,
                'status': 'Fetching leads from Bellevie...',
                'percentage': 50
            }
        )
        
        leads = fetch_leads_from_bellevie()
        if not leads:
            return {
                'status': 'error',
                'message': 'No leads fetched from Bellevie',
                'new': 0,
                'updated': 0
            }
        
        # Step 2: Sync to database
        self.update_state(
            state='PROGRESS',
            meta={
                'current': 2,
                'total': total_steps,
                'status': f'Syncing {len(leads)} leads to database...',
                'percentage': 75
            }
        )
        
        new_count, updated_count = sync_leads_to_database(leads)
        
        # Complete
        self.update_state(
            state='SUCCESS',
            meta={
                'current': total_steps,
                'total': total_steps,
                'status': 'Rescrape completed',
                'percentage': 100
            }
        )
        
        return {
            'status': 'success',
            'message': 'Rescrape completed successfully',
            'new': new_count,
            'updated': updated_count,
            'total': len(leads)
        }
    
    except Exception as e:
        print(f"Rescrape task error: {e}")
        self.retry(exc=e, countdown=5)
        return {
            'status': 'error',
            'message': str(e)
        }

@shared_task(bind=True, base=CallbackTask)
def sync_task(self, leads_data):
    """Sync specific leads to database"""
    try:
        new_count, updated_count = sync_leads_to_database(leads_data)
        return {
            'status': 'success',
            'new': new_count,
            'updated': updated_count
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }

@shared_task
def health_check():
    """Health check task"""
    return {'status': 'healthy', 'timestamp': str(time.time())}
