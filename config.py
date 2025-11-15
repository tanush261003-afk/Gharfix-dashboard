
"""
Configuration - Works for BOTH local and Render
Production-safe config with fail-fast validation
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

BELLEVIE_AUTH_COOKIE = os.getenv("BELLEVIE_AUTH_COOKIE", "")

# ============================================================================
# REDIS (optional - only if using caching)
# ============================================================================

REDIS_URL = os.getenv('REDIS_URL', '').strip()

if REDIS_URL:
    try:
        ru = urlparse(REDIS_URL)
        # Validate that we can extract the port safely
        # This prevents: ValueError: Port could not be cast to integer value as 'port'
        if ru.port is None:
            raise ValueError(f"REDIS_URL must include a numeric port, got: {REDIS_URL}")
        REDIS = {
            'host': ru.hostname or 'localhost',
            'port': ru.port,
            'password': ru.password
        }
    except ValueError as e:
        print(f"❌ REDIS_URL parse error: {e}")
        print("⚠️  Disabling Redis. Set a valid REDIS_URL or remove it from env.")
        REDIS = None
else:
    REDIS = None

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

# ============================================================================
# STARTUP DIAGNOSTICS
# ============================================================================

print("✅ Config loaded")
print(f"   - Database host: {DATABASE['host']}")
print(f"   - Database: {DATABASE['database']}")
print(f"   - Read-only: {READ_ONLY_MODE}")
print(f"   - Rescrape enabled: {ENABLE_RESCRAPE}")
print(f"   - Redis: {'Enabled' if REDIS else 'Disabled'}")
