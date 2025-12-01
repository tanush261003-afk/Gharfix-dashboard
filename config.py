"""
Configuration File - Environment Settings
✅ FIXED: Proper PostgreSQL configuration, Redis for Celery
"""
import os
from datetime import timedelta

# Database
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/gharfix')

# Flask
SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')
FLASK_ENV = os.getenv('FLASK_ENV', 'production')
DEBUG = FLASK_ENV == 'development'

# JWT
JWT_SECRET = os.getenv('JWT_SECRET', 'your-jwt-secret-key')
JWT_ALGORITHM = 'HS256'
JWT_EXPIRATION = timedelta(days=30)

# Authentication
ADMIN_USERNAME = os.getenv('ADMIN_USERNAME', 'Gharfix_analyst999')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'Gharfix314159')

# Celery
CELERY_BROKER_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
CELERY_RESULT_BACKEND = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
CELERY_TASK_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'UTC'

# Bellevie API
BELLEVIE_API_KEY = os.getenv('BELLEVIE_API_KEY', 'your-api-key')
BELLEVIE_API_URL = os.getenv('BELLEVIE_API_URL', 'https://api.bellevie.com')

# CORS
CORS_ORIGINS = os.getenv('CORS_ORIGINS', 'http://localhost:*,https://*.onrender.com')

# Port
PORT = int(os.getenv('PORT', 10000))
HOST = '0.0.0.0'
