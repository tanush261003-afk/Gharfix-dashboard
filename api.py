import os
import jwt
import json
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import psycopg
from dotenv import load_dotenv
import csv
from io import StringIO
import logging
from threading import Thread

load_dotenv()

app = Flask(__name__)
CORS(app)
app.config['JSON_SORT_KEYS'] = False

# JWT Configuration
JWT_SECRET = os.getenv('JWT_SECRET', 'gharfix_secret_key_2025_analytics')
JWT_ALGORITHM = 'HS256'
TOKEN_EXPIRY = 3600  # 1 hour

# Credentials
VALID_USERNAME = 'Gharfix_analyst999'
VALID_PASSWORD = 'Gharfix@314159'

# Global rescrape status
RESCRAPE_STATUS = {
    'is_running': False,
    'progress': 0,
    'total': 0,
    'current': 0,
    'message': 'Ready',
    'start_time': None,
    'estimated_time_remaining': 0
}

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection"""
    return psycopg.connect(os.getenv('DATABASE_URL'))

def auth_required(f):
    """Decorator to check JWT token"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        
        if not token:
            return jsonify({'message': 'Missing authorization token'}), 401
        
        try:
            if token.startswith('Bearer '):
                token = token[7:]
            
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            request.user = payload
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Invalid token'}), 401
        
        return f(*args, **kwargs)
    
    return decorated_function

@app.route('/api/login', methods=['POST'])
def login():
    """Login endpoint - returns JWT token"""
    data = request.get_json()
    username = data.get('username', '')
    password = data.get('password', '')
    
    # Case-sensitive check
    if username != VALID_USERNAME or password != VALID_PASSWORD:
        return jsonify({'message': 'Invalid credentials'}), 401
    
    # Create JWT token
    payload = {
        'username': username,
        'iat': datetime.utcnow(),
        'exp': datetime.utcnow() + timedelta(seconds=TOKEN_EXPIRY)
    }
    
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    
    return jsonify({
        'token': token,
        'expires_in': TOKEN_EXPIRY,
        'message': 'Login successful'
    }), 200

@app.route('/api/logout', methods=['POST'])
@auth_required
def logout():
    """Logout endpoint"""
    return jsonify({'message': 'Logged out successfully'}), 200

@app.route('/login')
def login_page():
    """Serve login page"""
    with open('login.html', 'r') as f:
        return f.read()

@app.route('/dashboard')
def dashboard():
    """Serve dashboard"""
    with open('dashboard_advanced.html', 'r') as f:
        return f.read()

@app.route('/api/all-analytics', methods=['GET'])
@auth_required
def get_all_analytics():
    """
    Get analytics using ONLY lead_events table:
    - total_lead_events = COUNT(*) from lead_events (matches Bellevie)
    - unique_customers = COUNT(DISTINCT customer_id) from lead_events (separate count)
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # COUNT 1: Total lead events (ALL records - matches Bellevie)
        cur.execute('SELECT COUNT(*) FROM lead_events')
        total_result = cur.fetchone()
        total_lead_events = total_result[0] if total_result else 0
        
        # COUNT 2: Unique customers (from lead_events, distinct)
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM lead_events')
        unique_result = cur.fetchone()
        unique_customers = unique_result[0] if unique_result else 0
        
        # Get latest status per customer (GROUP BY customer, ORDER BY submitted_at DESC)
        cur.execute('''
            SELECT DISTINCT ON (customer_id) 
                customer_id, status, submitted_at
            FROM lead_events
            ORDER BY customer_id, submitted_at DESC
        ''')
        
        latest_statuses = cur.fetchall()
        
        # Count by latest status
        status_counts = {}
        for row in latest_statuses:
            status = row[1]
            status_counts[status] = status_counts.get(status, 0) + 1
        
        status_data = [
            {'status': status, 'count': count} 
            for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True)
        ]
        
        # Get services (latest per customer)
        cur.execute('''
            SELECT DISTINCT ON (customer_id) 
                customer_id, service_name
            FROM lead_events
            WHERE service_name IS NOT NULL AND service_name != ''
            ORDER BY customer_id, submitted_at DESC
        ''')
        
        services_data_raw = cur.fetchall()
        services_count = {}
        for row in services_data_raw:
            service = row[1]
            services_count[service] = services_count.get(service, 0) + 1
        
        services_data = [
            {'service': service, 'count': count}
            for service, count in sorted(services_count.items(), key=lambda x: x[1], reverse=True)
        ][:20]
        
        cur.close()
        conn.close()
        
        return jsonify({
            'total_lead_events': total_lead_events,    # ← ALL records (matches Bellevie)
            'unique_customers': unique_customers,       # ← DISTINCT customers
            'status_distribution': status_data,         # ← Latest status per customer
            'top_services': services_data,
            'sync_status': 'Connected to Bellevie',
            'last_updated': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        logger.error(f"Error in get_all_analytics: {str(e)}")
        return jsonify({'message': f'Error: {str(e)}', 'total_lead_events': 0, 'unique_customers': 0}), 200

@app.route('/api/rescrape-status', methods=['GET'])
@auth_required
def rescrape_status():
    """Get rescrape status with progress bar"""
    return jsonify(RESCRAPE_STATUS), 200

@app.route('/api/rescrape', methods=['POST'])
@auth_required
def rescrape():
    """Trigger rescrape from Bellevie"""
    if RESCRAPE_STATUS['is_running']:
        return jsonify({'message': 'Rescrape already in progress'}), 409
    
    # Start rescrape in background thread
    def run_rescrape():
        try:
            from scraper_final import rescrape_all
            rescrape_all(update_progress)
        except Exception as e:
            logger.error(f"Error in rescrape: {str(e)}")
            RESCRAPE_STATUS['is_running'] = False
            RESCRAPE_STATUS['message'] = f'Error: {str(e)}'
    
    thread = Thread(target=run_rescrape, daemon=True)
    thread.start()
    
    return jsonify({
        'message': 'Rescrape started',
        'status': 'success'
    }), 200

def update_progress(current, total, message):
    """Update rescrape progress"""
    RESCRAPE_STATUS['is_running'] = True
    RESCRAPE_STATUS['current'] = current
    RESCRAPE_STATUS['total'] = total
    RESCRAPE_STATUS['message'] = message
    
    if total > 0:
        RESCRAPE_STATUS['progress'] = int((current / total) * 100)
        if current > 0:
            elapsed = (datetime.now() - RESCRAPE_STATUS['start_time']).total_seconds()
            rate = current / elapsed if elapsed > 0 else 0
            remaining = (total - current) / rate if rate > 0 else 0
            RESCRAPE_STATUS['estimated_time_remaining'] = int(remaining)
    
    if current >= total:
        RESCRAPE_STATUS['is_running'] = False
        RESCRAPE_STATUS['message'] = 'Rescrape completed!'

@app.route('/api/system-status', methods=['GET'])
@auth_required
def system_status():
    """Get system status"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute('SELECT COUNT(*) FROM lead_events')
        total_events = cur.fetchone()[0]
        
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM lead_events')
        unique_customers = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        return jsonify({
            'status': 'ok',
            'database': 'connected',
            'total_events': total_events,
            'unique_customers': unique_customers,
            'timestamp': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/', methods=['GET'])
def home():
    """Home endpoint"""
    return jsonify({
        'message': 'Gharfix Analytics Dashboard API',
        'version': '4.0',
        'sync': 'Connected to Bellevie'
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)), debug=False)
