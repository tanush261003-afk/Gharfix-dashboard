"""
Celery Application Configuration
✅ FIXES: Proper Celery setup for async tasks
"""
import os
from celery import Celery

# Get Redis/Broker URL
BROKER_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
RESULT_BACKEND = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# Create Celery app
celery_app = Celery(
    'gharfix',
    broker=BROKER_URL,
    backend=RESULT_BACKEND
)

# Configure Celery
celery_app.conf.update(
    # Task configuration
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    
    # Task timeout and retries
    task_time_limit=600,  # 10 minutes
    task_soft_time_limit=580,  # 9:40 minutes
    
    # Result backend
    result_expires=3600,  # 1 hour
    
    # Worker configuration
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    
    # Celery Beat schedule (for periodic tasks)
    beat_schedule={
        'health-check': {
            'task': 'tasks.health_check',
            'schedule': 3600.0,  # Every 1 hour
        },
    }
)

if __name__ == '__main__':
    celery_app.start()
