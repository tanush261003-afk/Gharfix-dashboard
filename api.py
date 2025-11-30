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
    """Get all analytics - MATCHES BELLEVIE COUNTS"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # COUNT 1: Total events (matches Bellevie)
        cur.execute('SELECT COUNT(*) FROM lead_events')
        total_events = cur.fetchone()[0]
        
        # COUNT 2: Unique customers
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM lead_latest_status')
        unique_customers = cur.fetchone()[0]
        
        # Get status distribution (LATEST STATUS ONLY)
        cur.execute('''
            SELECT current_status, COUNT(*) as count
            FROM lead_latest_status
            GROUP BY current_status
            ORDER BY count DESC
        ''')
        status_data = [{'status': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        # Get services
        cur.execute('''
            SELECT service_name, COUNT(*) as count
            FROM lead_latest_status
            WHERE service_name IS NOT NULL AND service_name != ''
            GROUP BY service_name
            ORDER BY count DESC
            LIMIT 20
        ''')
        services_data = [{'service': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return jsonify({
            'total_lead_events': total_events,  # ← MATCHES BELLEVIE (e.g., 4857)
            'unique_customers': unique_customers,
            'status_distribution': status_data,
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
    """Get filtered analytics"""
    try:
        status_filter = request.args.get('status', '').strip()
        service_filter = request.args.get('service', '').strip()
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Build WHERE clause
        where_clauses = []
        params = []
        
        if status_filter and status_filter.upper() != 'ALL STATUS':
            where_clauses.append('lls.current_status = %s')
            params.append(status_filter)
        
        where_sql = ' AND '.join(where_clauses) if where_clauses else '1=1'
        
        # Get filtered leads
        cur.execute(f'''
            SELECT DISTINCT lls.lead_id, lls.customer_id, lls.current_status
            FROM lead_latest_status lls
            WHERE {where_sql}
        ''', params)
        
        lead_ids = [str(row[0]) for row in cur.fetchall()]
        
        if not lead_ids:
            return jsonify({
                'filtered_count': 0,
                'services': [],
                'status_data': []
            }), 200
        
        # Filter by service if specified
        if service_filter and service_filter.upper() != 'ALL SERVICES':
            cur.execute(f'''
                SELECT COUNT(DISTINCT le.customer_id)
                FROM lead_events le
                WHERE le.customer_id IN ({', '.join(lead_ids)})
                AND le.service_name = %s
            ''', [service_filter])
            filtered_count = cur.fetchone()[0]
        else:
            filtered_count = len(lead_ids)
        
        # Get services for filtered leads
        cur.execute(f'''
            SELECT DISTINCT service_name
            FROM lead_latest_status
            WHERE customer_id IN ({', '.join(lead_ids)})
            AND service_name IS NOT NULL AND service_name != ''
            ORDER BY service_name
        ''')
        services = [row[0] for row in cur.fetchall()]
        
        # Get status distribution
        cur.execute(f'''
            SELECT current_status, COUNT(*) as count
            FROM lead_latest_status
            WHERE lead_id IN ({', '.join(lead_ids)})
            GROUP BY current_status
            ORDER BY count DESC
        ''')
        status_data = [{'status': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return jsonify({
            'filtered_count': filtered_count,
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
        lead_id = data.get('lead_id')
        new_status = data.get('new_status')
        
        if not lead_id or not new_status:
            return jsonify({'message': 'Missing lead_id or new_status'}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get old status
        cur.execute('''
            SELECT current_status FROM lead_latest_status 
            WHERE lead_id = %s
        ''', [lead_id])
        
        result = cur.fetchone()
        old_status = result[0] if result else None
        
        # Update latest status
        cur.execute('''
            INSERT INTO lead_latest_status 
            (lead_id, customer_id, current_status, previous_status, last_updated)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (lead_id) DO UPDATE
            SET current_status = %s,
                previous_status = %s,
                last_updated = NOW()
        ''', [lead_id, lead_id, new_status, old_status, new_status, old_status])
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'lead_id': lead_id,
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
        
        # Build query
        where_clause = '1=1'
        params = []
        
        if status_filter and status_filter.upper() != 'ALL STATUS':
            where_clause = 'current_status = %s'
            params.append(status_filter)
        
        cur.execute(f'''
            SELECT lead_id, customer_id, current_status, last_updated
            FROM lead_latest_status
            WHERE {where_clause}
            ORDER BY last_updated DESC
        ''', params)
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        # Create CSV
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['Lead ID', 'Customer ID', 'Status', 'Last Updated'])
        
        for row in rows:
            writer.writerow(row)
        
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
        
        where_clause = '1=1'
        params = []
        
        if status_filter and status_filter.upper() != 'ALL STATUS':
            where_clause = 'current_status = %s'
            params.append(status_filter)
        
        cur.execute(f'''
            SELECT lead_id, customer_id, current_status, last_updated
            FROM lead_latest_status
            WHERE {where_clause}
            ORDER BY last_updated DESC
        ''', params)
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        data = [
            {
                'lead_id': row[0],
                'customer_id': row[1],
                'status': row[2],
                'last_updated': row[3].isoformat() if row[3] else None
            }
            for row in rows
        ]
        
        return send_file(
            StringIO(json.dumps(data, indent=2)),
            mimetype='application/json',
            as_attachment=True,
            download_name=f'gharfix-leads-{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.json'
        )
    
    except Exception as e:
        logger.error(f"Error in export_json: {str(e)}")
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/api/rescrape', methods=['POST'])
@auth_required
def rescrape():
    """Trigger rescrape from Bellevie"""
    try:
        from scraper_v2 import rescrape_all
        
        success = rescrape_all()
        
        if success:
            return jsonify({
                'message': 'Rescrape completed successfully',
                'status': 'success'
            }), 200
        else:
            return jsonify({
                'message': 'Rescrape failed',
                'status': 'error'
            }), 500
    
    except Exception as e:
        logger.error(f"Error in rescrape: {str(e)}")
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
        
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM lead_latest_status')
        unique_leads = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        return jsonify({
            'status': 'ok',
            'database': 'connected',
            'total_events': total_events,
            'unique_leads': unique_leads,
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
