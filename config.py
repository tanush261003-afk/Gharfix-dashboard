"""
Configuration File
==================
UPDATE THESE VALUES BEFORE RUNNING!
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# 1. BELLEVIE AUTHENTICATION - GET FROM BROWSER
# ============================================================================

BELLEVIE_AUTH_COOKIE = os.getenv(
    "BELLEVIE_AUTH_COOKIE",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbmNyeXB0ZWREYXRhIjoiNjM3YTFjMWM2YTcyNjNlNTkyNjY3ZDUwMTAyZTBhYmNhMTIwNzE4OGMwYzZiZTZiMTNkNTM2ZWFhMDJlYzBmODFkNjk4YmM0N2U2M2NiYTNkMGYzZmY3ZTc2MzFjNWU5MzZlNzgwMzU4NDA2ODExNjU1ZTM0MjVmNzlhYzY0MDhlZWIwNWFiZWEyZmExY2FlZDM4NzRmYTJhYTY4NjgxNTBhMjhhYWE3ZWEwYWQ2OTk4ZmRmODc3ZDAyZDFhNDQwZDRlNDFiNDA0YjE5OGIzZWNkNDU2YWE2YzQ0ZDdmZWRmYmIxM2MxOTAxNzA2YWNmMDMzYzY0MWMwYzg5ZmUzOWE0NWExNDFjOGVlZGRjNDI3OTYzN2RjNWFjNzcwYTFjMjEyYjBlYjUwYjgyNWJlMTM2ZGI4N2RlN2JiMzRlNGY3NzZkMTk5MDkxNmUzNjM2NjFjODIyYzkxOTRmM2Y1NzljOTdiNzYxOGYyOGMzN2ViMjlmOWM2ZjRiN2IwNWMyOWUwMTM5ZTI3MDZmMTNiMmRkMGRiM2M3ZWEyMGU0ZDIyMzllM2I2NjAzY2E1NGRmMTc2OTUzYzZmMjc5YTAzNGI5YTE2NzNhM2E1MTk3MzBiN2RhZjNkOWM1NTU0NmRlN2Q4YzA5MzUzZTI3ZWQyY2RmODYyMTEzNTljYWMyNWUiLCJpdiI6IllFaGYxUWdhSDFKc1lOTEsiLCJpYXQiOjE3NjIzNjA3NTUsImV4cCI6MTc5Mzg5Njc1NX0.bTkNObBdmMsoV-q9yiD4YKzFjgEipWM7SzGbVPqAhsc"
)

# ============================================================================
# 2. POSTGRESQL DATABASE CONFIGURATION (NOW READS FROM ENV!)
# ============================================================================

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://localhost:5432/bellevie_leads"
)

# For backward compatibility with old code
DATABASE = {
    'host': 'localhost',
    'port': 5432,
    'database': 'bellevie_leads',
    'user': 'tanushkumarnandi',
    'password': '',
}

# ============================================================================
# 3. REDIS CONFIGURATION (for Celery task queue)
# ============================================================================

REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://localhost:6379"
)

REDIS = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
}

# ============================================================================
# 4. SCRAPING SETTINGS
# ============================================================================

SCRAPE_INTERVAL_MINUTES = 5
ANALYTICS_UPDATE_INTERVAL = 1

# ============================================================================
# 5. ANALYTICS SETTINGS
# ============================================================================

ANALYTICS_LOOKBACK_HOURS = 24
TOP_SERVICES_COUNT = 10
TOP_LOCATIONS_COUNT = 5

# ============================================================================
# 6. API SERVER SETTINGS
# ============================================================================

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("PORT", 5000))

# ============================================================================
# 7. TIMEZONE
# ============================================================================

TIMEZONE = "Asia/Kolkata"
