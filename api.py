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
    - status_distribution = Latest status per customer from lead_events
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # COUNT 1: Total lead events (ALL records - matches Bellevie)
        cur.execute('SELECT COUNT(*) FROM lead_events')
        total_lead_events = cur.fetchone()[0]
        
        # COUNT 2: Unique customers (from lead_events, distinct)
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM lead_events')
        unique_customers = cur.fetchone()[0]
        
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
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/api/filtered-analytics', methods=['GET'])
@auth_required
def get_filtered_analytics():
    """Get filtered analytics using only lead_events"""
    try:
        status_filter = request.args.get('status', '').strip()
        service_filter = request.args.get('service', '').strip()
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get latest status per customer first
        cur.execute('''
            SELECT DISTINCT ON (customer_id) 
                customer_id, status
            FROM lead_events
            ORDER BY customer_id, submitted_at DESC
        ''')
        
        latest_by_customer = {}
        for row in cur.fetchall():
            latest_by_customer[row[0]] = row[1]
        
        # Filter by status if specified
        if status_filter and status_filter.upper() != 'ALL STATUS':
            filtered_customers = [
                cid for cid, status in latest_by_customer.items() 
                if status == status_filter
            ]
        else:
            filtered_customers = list(latest_by_customer.keys())
        
        if not filtered_customers:
            return jsonify({
                'filtered_events': 0,
                'filtered_customers': 0,
                'services': [],
                'status_data': []
            }), 200
        
        # Count events for filtered customers
        placeholders = ','.join(['%s'] * len(filtered_customers))
        cur.execute(f'''
            SELECT COUNT(*)
            FROM lead_events
            WHERE customer_id IN ({placeholders})
        ''', filtered_customers)
        filtered_events = cur.fetchone()[0]
        
        # Get services for filtered customers
        cur.execute(f'''
            SELECT DISTINCT ON (customer_id) service_name
            FROM lead_events
            WHERE customer_id IN ({placeholders})
            AND service_name IS NOT NULL AND service_name != ''
            ORDER BY customer_id, submitted_at DESC
        ''', filtered_customers)
        services = [row[0] for row in cur.fetchall()]
        
        # Count by status for filtered customers
        status_counts = {}
        for cid in filtered_customers:
            status = latest_by_customer[cid]
            status_counts[status] = status_counts.get(status, 0) + 1
        
        status_data = [
            {'status': status, 'count': count}
            for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True)
        ]
        
        cur.close()
        conn.close()
        
        return jsonify({
            'filtered_events': filtered_events,
            'filtered_customers': len(filtered_customers),
            'services': services,
            'status_data': status_data
        }), 200
    
    except Exception as e:
        logger.error(f"Error in get_filtered_analytics: {str(e)}")
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/api/update-lead-status', methods=['PUT'])
@auth_required
def update_lead_status():
    """Update lead to new status"""
    try:
        data = request.get_json()
        customer_id = data.get('customer_id')
        new_status = data.get('new_status')
        
        if not customer_id or not new_status:
            return jsonify({'message': 'Missing customer_id or new_status'}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get latest status for this customer
        cur.execute('''
            SELECT DISTINCT ON (customer_id) status
            FROM lead_events
            WHERE customer_id = %s
            ORDER BY customer_id, submitted_at DESC
        ''', [customer_id])
        
        result = cur.fetchone()
        old_status = result[0] if result else None
        
        # Add new status entry to lead_events
        cur.execute('''
            INSERT INTO lead_events 
            (customer_id, status, submitted_at, service_name)
            VALUES (%s, %s, %s, %s)
        ''', [customer_id, new_status, int(datetime.now().timestamp() * 1000), 'Manual Update'])
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'customer_id': customer_id,
            'old_status': old_status,
            'new_status': new_status,
            'message': 'Status updated successfully'
        }), 200
    
    except Exception as e:
        logger.error(f"Error in update_lead_status: {str(e)}")
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/api/export/csv', methods=['GET'])
@auth_required
def export_csv():
    """Export filtered data as CSV"""
    try:
        status_filter = request.args.get('status', '').strip()
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get latest status per customer
        cur.execute('''
            SELECT DISTINCT ON (customer_id) 
                customer_id, status, submitted_at
            FROM lead_events
            ORDER BY customer_id, submitted_at DESC
        ''')
        
        latest_by_customer = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
        
        # Filter if needed
        if status_filter and status_filter.upper() != 'ALL STATUS':
            filtered_data = [
                (cid, status, submitted_at) 
                for cid, (status, submitted_at) in latest_by_customer.items()
                if status == status_filter
            ]
        else:
            filtered_data = [
                (cid, status, submitted_at) 
                for cid, (status, submitted_at) in latest_by_customer.items()
            ]
        
        cur.close()
        conn.close()
        
        # Create CSV
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['Customer ID', 'Latest Status', 'Last Updated'])
        
        for customer_id, status, submitted_at in sorted(filtered_data, key=lambda x: x[2], reverse=True):
            dt = datetime.fromtimestamp(submitted_at / 1000) if submitted_at else 'N/A'
            writer.writerow([customer_id, status, dt])
        
        output.seek(0)
        return send_file(
            StringIO(output.getvalue()),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'gharfix-leads-{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'
        )
    
    except Exception as e:
        logger.error(f"Error in export_csv: {str(e)}")
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/api/export/json', methods=['GET'])
@auth_required
def export_json():
    """Export filtered data as JSON"""
    try:
        status_filter = request.args.get('status', '').strip()
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get latest status per customer
        cur.execute('''
            SELECT DISTINCT ON (customer_id) 
                customer_id, status, submitted_at
            FROM lead_events
            ORDER BY customer_id, submitted_at DESC
        ''')
        
        latest_by_customer = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
        
        # Filter if needed
        if status_filter and status_filter.upper() != 'ALL STATUS':
            filtered_data = [
                {'customer_id': cid, 'status': status, 'last_updated': datetime.fromtimestamp(submitted_at/1000).isoformat() if submitted_at else None}
                for cid, (status, submitted_at) in latest_by_customer.items()
                if status == status_filter
            ]
        else:
            filtered_data = [
                {'customer_id': cid, 'status': status, 'last_updated': datetime.fromtimestamp(submitted_at/1000).isoformat() if submitted_at else None}
                for cid, (status, submitted_at) in latest_by_customer.items()
            ]
        
        cur.close()
        conn.close()
        
        return send_file(
            StringIO(json.dumps(filtered_data, indent=2)),
            mimetype='application/json',
            as_attachment=True,
            download_name=f'gharfix-leads-{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.json'
        )
    
    except Exception as e:
        logger.error(f"Error in export_json: {str(e)}")
        return jsonify({'message': f'Error: {str(e)}'}), 500

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
        'version': '3.0',
        'sync': 'Connected to Bellevie'
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)), debug=False)
