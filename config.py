"""
Configuration - Works for BOTH local and Render
"""
import os
from urllib.parse import urlparse

# ============================================================================
# DATABASE - Auto-detect from DATABASE_URL (Neon or local)
# ============================================================================

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://localhost:5432/bellevie_leads')
url = urlparse(DATABASE_URL)

DATABASE = {
    'host': url.hostname,
    'port': url.port or 5432,
    'database': (url.path[1:] if url.path and len(url.path) > 1 else None),
    'user': url.username,
    'password': url.password
}

# Fail fast if DATABASE is not configured correctly (prevents Render 500 loops)
if not DATABASE['host'] or not DATABASE['database']:
    raise ValueError("DATABASE_URL is missing or invalid. Set a full Postgres URL in env (e.g., Neon)")

# ============================================================================
# BELLEVIE AUTHENTICATION (only needed locally)
# ============================================================================

BELLEVIE_AUTH_COOKIE = os.getenv(
    "BELLEVIE_AUTH_COOKIE",
    ""  # Leave empty by default to avoid accidental use on Render
)

# ============================================================================
# REDIS (optional - only if using caching)
# ============================================================================

REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
redis_url = urlparse(REDIS_URL)

REDIS = {
    'host': redis_url.hostname or 'localhost',
    'port': redis_url.port or 6379,
    'password': redis_url.password
}

# ============================================================================
# API SETTINGS
# ============================================================================

API_HOST = os.getenv('API_HOST', '0.0.0.0')
API_PORT = int(os.getenv('PORT', 10000))

# ============================================================================
# FEATURE FLAGS
# ============================================================================

# Default to read-only on cloud for safety
READ_ONLY_MODE = os.getenv('READ_ONLY_MODE', 'true').lower() == 'true'
ENABLE_RESCRAPE = os.getenv('ENABLE_RESCRAPE', 'false').lower() == 'true'

print("✅ Config loaded")
print(f"   - Database host: {DATABASE['host']}")
print(f"   - Read-only: {READ_ONLY_MODE}")
print(f"   - Rescrape enabled: {ENABLE_RESCRAPE}")
