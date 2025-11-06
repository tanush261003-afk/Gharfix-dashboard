"""
Celery Application Configuration
=================================
Configures Celery for background task processing
"""

from celery import Celery
from celery.schedules import crontab
from config import REDIS, TIMEZONE, SCRAPE_INTERVAL_MINUTES, ANALYTICS_UPDATE_INTERVAL

# Create Celery app
app = Celery('bellevie_analytics')

# Configure Redis as message broker
app.conf.broker_url = f"redis://{REDIS['host']}:{REDIS['port']}/{REDIS['db']}"
app.conf.result_backend = f"redis://{REDIS['host']}:{REDIS['port']}/1"

# Task settings
app.conf.task_serializer = 'json'
app.conf.result_serializer = 'json'
app.conf.accept_content = ['json']
app.conf.timezone = TIMEZONE
app.conf.enable_utc = False

# Task time limits (5 minutes max)
app.conf.task_time_limit = 300
app.conf.task_soft_time_limit = 240

# Retry settings
app.conf.task_acks_late = True
app.conf.task_reject_on_worker_lost = True

# Schedule periodic tasks
app.conf.beat_schedule = {
    # Scrape every 5 minutes
    'scrape-leads': {
        'task': 'tasks.scrape_leads_task',
        'schedule': crontab(minute=f'*/{SCRAPE_INTERVAL_MINUTES}'),
    },

    # Update analytics every 1 minute
    'update-analytics': {
        'task': 'tasks.update_analytics_task',
        'schedule': crontab(minute=f'*/{ANALYTICS_UPDATE_INTERVAL}'),
    },
}

print("✓ Celery app configured")
# Import tasks to register them
from tasks import *
