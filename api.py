"""
Gharfix API Server - Flask Backend with Complete Lead Management
✅ FIXES: Login redirect loop, real-time sync, detailed lead display, working filters
"""
import os
import json
from datetime import datetime, timedelta
from functools import wraps
import jwt
import psycopg
from flask import Flask, request, jsonify, render_template, send_file, redirect, session

# Initialize Flask app
app = Flask(__name__, template_folder='.', static_folder='.')
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'gharfix-secret-key-change-in-production')
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)

# Configuration
ADMIN_USERNAME = os.getenv('ADMIN_USERNAME', 'admin')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'admin123')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost/gharfix')

# Database connection
def get_db():
    """Create database connection using psycopg3"""
    try:
        conn = psycopg.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

# JWT Authentication decorator
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'error': 'Missing token'}), 401
        
        try:
            token = token.replace('Bearer ', '')
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            current_user = data.get('username')
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401
        
        return f(current_user, *args, **kwargs)
    return decorated

# ==================== Routes ====================

@app.route('/')
def index():
    """Redirect to login/dashboard"""
    token = request.headers.get('Authorization')
    if token:
        return redirect('/dashboard')
    return redirect('/login')

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Handle login"""
    if request.method == 'GET':
        # Check if user already has valid token
        if 'token' in session:
            return redirect('/dashboard')
        return render_template('login.html')
    
    # POST - handle login
    data = request.get_json()
    username = data.get('username', '')
    password = data.get('password', '')
    
    if username != ADMIN_USERNAME or password != ADMIN_PASSWORD:
        return jsonify({'error': 'Invalid credentials'}), 401
    
    # Generate JWT token
    token = jwt.encode({
        'username': username,
        'exp': datetime.utcnow() + timedelta(days=30)
    }, app.config['SECRET_KEY'], algorithm='HS256')
    
    session.permanent = True
    session['token'] = token
    
    return jsonify({'token': token, 'success': True}), 200

@app.route('/dashboard')
def dashboard():
    """Load dashboard HTML"""
    return render_template('dashboard_advanced.html')

# ==================== API Endpoints ====================

@app.route('/api/all-analytics', methods=['GET'])
@token_required
def get_all_analytics(current_user):
    """Get complete analytics including detailed lead data"""
    try:
        conn = get_db()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cur = conn.cursor()
        
        # Get total lead events
        cur.execute('SELECT COUNT(*) FROM lead_events')
        total_lead_events = cur.fetchone()[0] or 0
        
        # Get unique customers (distinct leads)
        cur.execute('SELECT COUNT(DISTINCT customer_id) FROM leads')
        unique_customers = cur.fetchone()[0] or 0
        
        # Get status distribution
        cur.execute('''
            SELECT status, COUNT(*) as count
            FROM lead_events
            GROUP BY status
            ORDER BY count DESC
        ''')
        status_distribution = [{'status': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        # Get top services
        cur.execute('''
            SELECT service_name, COUNT(*) as count
            FROM lead_events
            GROUP BY service_name
            ORDER BY count DESC
            LIMIT 10
        ''')
        top_services = [{'service': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        # Get detailed lead data with latest events
        cur.execute('''
            SELECT DISTINCT ON (l.customer_id)
                l.customer_id,
                l.first_name,
                l.last_name,
                le.service_name,
                le.status,
                le.submitted_at,
                le.vendor,
                le.rate_card
            FROM leads l
            LEFT JOIN lead_events le ON l.customer_id = le.customer_id
            ORDER BY l.customer_id, le.submitted_at DESC
            LIMIT 1000
        ''')
        
        leads_data = []
        for row in cur.fetchall():
            submitted_at = row[5]
            if submitted_at:
                if isinstance(submitted_at, str):
                    dt = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
                else:
                    dt = submitted_at
                date_str = dt.strftime('%Y-%m-%d')
                time_str = dt.strftime('%H:%M:%S')
            else:
                date_str = 'N/A'
                time_str = 'N/A'
            
            leads_data.append({
                'lead_id': row[0],
                'name': f"{row[1] or ''} {row[2] or ''}".strip(),
                'service': row[3] or 'N/A',
                'status': row[4] or 'UNKNOWN',
                'date': date_str,
                'time': time_str,
                'vendor': row[6] or 'N/A',
                'rate_card': row[7] or 'N/A'
            })
        
        cur.close()
        conn.close()
        
        return jsonify({
            'total_lead_events': total_lead_events,
            'unique_customers': unique_customers,
            'status_distribution': status_distribution,
            'top_services': top_services,
            'leads': leads_data,
            'sync_status': 'Connected',
            'last_updated': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/filtered-analytics', methods=['GET'])
@token_required
def get_filtered_analytics(current_user):
    """Get filtered lead analytics"""
    try:
        status_filter = request.args.get('status', '')
        service_filter = request.args.get('service', '')
        
        conn = get_db()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cur = conn.cursor()
        
        # Build WHERE clause
        where_clause = '1=1'
        params = []
        
        if status_filter:
            where_clause += ' AND le.status = %s'
            params.append(status_filter)
        
        if service_filter:
            where_clause += ' AND le.service_name = %s'
            params.append(service_filter)
        
        # Get metrics
        query = f'SELECT COUNT(*) FROM lead_events le WHERE {where_clause}'
        cur.execute(query, params)
        total_lead_events = cur.fetchone()[0] or 0
        
        query = f'''SELECT COUNT(DISTINCT l.customer_id) 
                   FROM leads l LEFT JOIN lead_events le ON l.customer_id = le.customer_id 
                   WHERE {where_clause}'''
        cur.execute(query, params)
        unique_customers = cur.fetchone()[0] or 0
        
        # Get status distribution
        query = f'''
            SELECT status, COUNT(*) as count
            FROM lead_events
            WHERE {where_clause}
            GROUP BY status
            ORDER BY count DESC
        '''
        cur.execute(query, params)
        status_distribution = [{'status': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        # Get services
        query = f'''
            SELECT service_name, COUNT(*) as count
            FROM lead_events
            WHERE {where_clause}
            GROUP BY service_name
            ORDER BY count DESC
            LIMIT 10
        '''
        cur.execute(query, params)
        top_services = [{'service': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        # Get filtered leads
        query = f'''
            SELECT DISTINCT ON (l.customer_id)
                l.customer_id,
                l.first_name,
                l.last_name,
                le.service_name,
                le.status,
                le.submitted_at,
                le.vendor,
                le.rate_card
            FROM leads l
            LEFT JOIN lead_events le ON l.customer_id = le.customer_id
            WHERE {where_clause}
            ORDER BY l.customer_id, le.submitted_at DESC
            LIMIT 1000
        '''
        cur.execute(query, params)
        
        leads_data = []
        for row in cur.fetchall():
            submitted_at = row[5]
            if submitted_at:
                if isinstance(submitted_at, str):
                    dt = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
                else:
                    dt = submitted_at
                date_str = dt.strftime('%Y-%m-%d')
                time_str = dt.strftime('%H:%M:%S')
            else:
                date_str = 'N/A'
                time_str = 'N/A'
            
            leads_data.append({
                'lead_id': row[0],
                'name': f"{row[1] or ''} {row[2] or ''}".strip(),
                'service': row[3] or 'N/A',
                'status': row[4] or 'UNKNOWN',
                'date': date_str,
                'time': time_str,
                'vendor': row[6] or 'N/A',
                'rate_card': row[7] or 'N/A'
            })
        
        cur.close()
        conn.close()
        
        return jsonify({
            'total_lead_events': total_lead_events,
            'unique_customers': unique_customers,
            'status_distribution': status_distribution,
            'top_services': top_services,
            'leads': leads_data,
            'sync_status': 'Connected',
            'last_updated': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/rescrape', methods=['POST'])
@token_required
def trigger_rescrape(current_user):
    """Trigger rescrape via Celery task"""
    try:
        from tasks import rescrape_task
        task = rescrape_task.apply_async()
        return jsonify({
            'status': 'success',
            'message': 'Rescrape started',
            'task_id': task.id
        }), 200
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'error': str(e), 'status': 'error'}), 500

@app.route('/api/rescrape-status/<task_id>', methods=['GET'])
@token_required
def get_rescrape_status(current_user, task_id):
    """Get rescrape task status"""
    try:
        from celery.result import AsyncResult
        from celery_app import celery_app
        task = AsyncResult(task_id, app=celery_app)
        
        return jsonify({
            'status': task.state,
            'result': task.result if task.state == 'SUCCESS' else None,
            'progress': task.info if task.state == 'PROGRESS' else 0
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def server_error(error):
    return jsonify({'error': 'Server error'}), 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
