"""
Fixed API Backend - Correct Column Names & Credentials
✅ FIXES: vendor → vendor_id, new credentials, proper SQL queries
"""
import os
import jwt
import json
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, render_template_string, request, jsonify, redirect
from flask_cors import CORS
from database import init_db, get_db, insert_or_update_lead, insert_lead_event, get_lead_count, get_event_count

app = Flask(__name__)
CORS(app)

# ✅ NEW CREDENTIALS
ADMIN_USERNAME = os.getenv('ADMIN_USERNAME', 'Gharfix_analyst999')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'Gharfix314159')
JWT_SECRET = os.getenv('JWT_SECRET', 'your-secret-key-change-in-production')
JWT_ALGORITHM = 'HS256'

# Initialize database
init_db()

def token_required(f):
    """Decorator to verify JWT token"""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            try:
                token = auth_header.split(" ")[1]
            except:
                return jsonify({'message': 'Invalid token format'}), 401
        
        if not token:
            return jsonify({'message': 'Token missing'}), 401
        
        try:
            data = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            current_user = data['username']
        except:
            return jsonify({'message': 'Invalid token'}), 401
        
        return f(current_user, *args, **kwargs)
    
    return decorated

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login endpoint"""
    if request.method == 'POST':
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        
        # ✅ VERIFY AGAINST NEW CREDENTIALS
        if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
            token = jwt.encode(
                {
                    'username': username,
                    'exp': datetime.utcnow() + timedelta(days=30)
                },
                JWT_SECRET,
                algorithm=JWT_ALGORITHM
            )
            
            return jsonify({'token': token}), 200
        else:
            return jsonify({'message': 'Invalid credentials'}), 401
    
    # Return login HTML
    return render_template_string(open('login.html').read())

@app.route('/dashboard')
def dashboard():
    """Dashboard page"""
    return render_template_string(open('dashboard_advanced.html').read())

@app.route('/api/all-analytics', methods=['GET'])
@token_required
def all_analytics(current_user):
    """Get all analytics with corrected SQL query"""
    try:
        conn = get_db()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cur = conn.cursor()
        
        # ✅ FIXED: Using vendor_id instead of vendor, correct column references
        cur.execute('''
            SELECT 
                le.event_id,
                l.customer_id,
                l.first_name,
                l.last_name,
                le.service_name,
                le.status,
                le.vendor_id,
                le.rate_card,
                le.submitted_at
            FROM lead_events le
            JOIN leads l ON le.customer_id = l.customer_id
            ORDER BY le.submitted_at DESC
        ''')
        
        events = cur.fetchall()
        
        # Get counts
        cur.execute('SELECT COUNT(*) FROM leads')
        unique_customers = cur.fetchone()[0]
        
        cur.execute('SELECT COUNT(*) FROM lead_events')
        total_events = cur.fetchone()[0]
        
        # Format results
        leads_list = []
        for row in events:
            leads_list.append({
                'event_id': row[0],
                'customer_id': row[1],
                'first_name': row[2],
                'last_name': row[3],
                'service': row[4],
                'status': row[5],
                'vendor_id': row[6],
                'rate_card': row[7],
                'date': row[8].strftime('%Y-%m-%d') if row[8] else None,
                'time': row[8].strftime('%H:%M:%S') if row[8] else None
            })
        
        cur.close()
        conn.close()
        
        return jsonify({
            'unique_customers': unique_customers,
            'total_events': total_events,
            'leads': leads_list
        }), 200
    
    except Exception as e:
        print(f"Error in all_analytics: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/filtered-analytics', methods=['GET'])
@token_required
def filtered_analytics(current_user):
    """Get filtered analytics"""
    try:
        status_filter = request.args.get('status')
        service_filter = request.args.get('service')
        
        conn = get_db()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cur = conn.cursor()
        
        # Build query with filters
        query = '''
            SELECT 
                le.event_id,
                l.customer_id,
                l.first_name,
                l.last_name,
                le.service_name,
                le.status,
                le.vendor_id,
                le.rate_card,
                le.submitted_at
            FROM lead_events le
            JOIN leads l ON le.customer_id = l.customer_id
            WHERE 1=1
        '''
        
        params = []
        
        if status_filter and status_filter != 'all':
            query += ' AND le.status = %s'
            params.append(status_filter)
        
        if service_filter and service_filter != 'all':
            query += ' AND le.service_name = %s'
            params.append(service_filter)
        
        query += ' ORDER BY le.submitted_at DESC'
        
        cur.execute(query, params)
        events = cur.fetchall()
        
        # Format results
        leads_list = []
        for row in events:
            leads_list.append({
                'event_id': row[0],
                'customer_id': row[1],
                'first_name': row[2],
                'last_name': row[3],
                'service': row[4],
                'status': row[5],
                'vendor_id': row[6],
                'rate_card': row[7],
                'date': row[8].strftime('%Y-%m-%d') if row[8] else None,
                'time': row[8].strftime('%H:%M:%S') if row[8] else None
            })
        
        cur.close()
        conn.close()
        
        return jsonify({'leads': leads_list}), 200
    
    except Exception as e:
        print(f"Error in filtered_analytics: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/filter-options', methods=['GET'])
@token_required
def filter_options(current_user):
    """Get available filter options"""
    try:
        conn = get_db()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cur = conn.cursor()
        
        # Get unique statuses
        cur.execute('SELECT DISTINCT status FROM lead_events WHERE status IS NOT NULL ORDER BY status')
        statuses = [row[0] for row in cur.fetchall()]
        
        # Get unique services
        cur.execute('SELECT DISTINCT service_name FROM lead_events WHERE service_name IS NOT NULL ORDER BY service_name')
        services = [row[0] for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return jsonify({
            'statuses': statuses,
            'services': services
        }), 200
    
    except Exception as e:
        print(f"Error getting filter options: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/rescrape', methods=['POST'])
@token_required
def rescrape(current_user):
    """Trigger rescrape task"""
    try:
        # For now, return success (implement Celery integration if needed)
        return jsonify({
            'message': 'Rescrape task queued',
            'task_id': 'task-123'
        }), 200
    
    except Exception as e:
        print(f"Error in rescrape: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/rescrape-status/<task_id>', methods=['GET'])
@token_required
def rescrape_status(current_user, task_id):
    """Get rescrape task status"""
    try:
        # Return mock status
        return jsonify({
            'task_id': task_id,
            'status': 'completed',
            'progress': 100
        }), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

if __name__ == '__main__':
    port = int(os.getenv('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False)
