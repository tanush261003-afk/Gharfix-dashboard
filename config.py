"""
Configuration File
==================
UPDATE THESE VALUES BEFORE RUNNING!
"""

# ============================================================================
# 1. BELLEVIE AUTHENTICATION - GET FROM BROWSER
# ============================================================================
# Steps:
# 1. Login to https://brand.bellevie.life/
# 2. Press F12 → Application → Cookies → bellevie.life
# 3. Copy value of "bGH_6fJF77c" cookie
# 4. Paste below:

BELLEVIE_AUTH_COOKIE = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbmNyeXB0ZWREYXRhIjoiNjM3YTFjMWM2YTcyNjNlNTkyNjY3ZDUwMTAyZTBhYmNhMTIwNzE4OGMwYzZiZTZiMTNkNTM2ZWFhMDJlYzBmODFkNjk4YmM0N2U2M2NiYTNkMGYzZmY3ZTc2MzFjNWU5MzZlNzgwMzU4NDA2ODExNjU1ZTM0MjVmNzlhYzY0MDhlZWIwNWFiZWEyZmExY2FlZDM4NzRmYTJhYTY4NjgxNTBhMjhhYWE3ZWEwYWQ2OTk4ZmRmODc3ZDAyZDFhNDQwZDRlNDFiNDA0YjE5OGIzZWNkNDU2YWE2YzQ0ZDdmZWRmYmIxM2MxOTAxNzA2YWNmMDMzYzY0MWMwYzg5ZmUzOWE0NWExNDFjOGVlZGRjNDI3OTYzN2RjNWFjNzcwYTFjMjEyYjBlYjUwYjgyNWJlMTM2ZGI4N2RlN2JiMzRlNGY3NzZkMTk5MDkxNmUzNjM2NjFjODIyYzkxOTRmM2Y1NzljOTdiNzYxOGYyOGMzN2ViMjlmOWM2ZjRiN2IwNWMyOWUwMTM5ZTI3MDZmMTNiMmRkMGRiM2M3ZWEyMGU0ZDIyMzllM2I2NjAzY2E1NGRmMTc2OTUzYzZmMjc5YTAzNGI5YTE2NzNhM2E1MTk3MzBiN2RhZjNkOWM1NTU0NmRlN2Q4YzA5MzUzZTI3ZWQyY2RmODYyMTEzNTljYWMyNWUiLCJpdiI6IllFaGYxUWdhSDFKc1lOTEsiLCJpYXQiOjE3NjIzNjA3NTUsImV4cCI6MTc5Mzg5Njc1NX0.bTkNObBdmMsoV-q9yiD4YKzFjgEipWM7SzGbVPqAhsc"

# ============================================================================
# 2. POSTGRESQL DATABASE CONFIGURATION
# ============================================================================
# These are the credentials you set up during PostgreSQL installation

DATABASE = {
    'host': 'localhost',
    'port': 5432,
    'database': 'bellevie_leads',
    'user': 'tanushkumarnandi',    # ✅ Your Mac username
    'password': '',                 # ✅ No password on Mac
}


# ============================================================================
# 3. REDIS CONFIGURATION (for Celery task queue)
# ============================================================================
# Leave as-is for local installation

REDIS = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,  # Broker database
}

# ============================================================================
# 4. SCRAPING SETTINGS
# ============================================================================

SCRAPE_INTERVAL_MINUTES = 5  # Check for new leads every 5 minutes
ANALYTICS_UPDATE_INTERVAL = 1  # Update analytics every 1 minute

# ============================================================================
# 5. ANALYTICS SETTINGS
# ============================================================================

ANALYTICS_LOOKBACK_HOURS = 24  # Analyze last 24 hours
TOP_SERVICES_COUNT = 10  # Show top 10 services
TOP_LOCATIONS_COUNT = 5  # Show top 5 locations

# ============================================================================
# 6. API SERVER SETTINGS
# ============================================================================

API_HOST = "0.0.0.0"  # Listen on all interfaces
API_PORT = 5001  # Dashboard at http://localhost:5001/dashboard

# ============================================================================
# 7. TIMEZONE
# ============================================================================

TIMEZONE = "Asia/Kolkata"
