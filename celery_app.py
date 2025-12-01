"""
Celery App Initialization
✅ FIXED: Proper Redis configuration for background tasks
"""
from celery import Celery
from config import CELERY_BROKER_URL, CELERY_RESULT_BACKEND
import os

# Initialize Celery
celery_app = Celery('gharfix')

# Configure from environment
celery_app.conf.update(
    broker_url=CELERY_BROKER_URL,
    result_backend=CELERY_RESULT_BACKEND,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
)

# Auto-discover tasks
celery_app.autodiscover_tasks(['tasks'])
