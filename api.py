"""
Gharfix API Server - Flask Backend
Handles authentication, data retrieval, and rescrape operations
"""
import os
import json
from datetime import datetime, timedelta
from functools import wraps
import jwt
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify, render_template, send_file
from werkzeug.security import generate_password_hash, check_password_hash
from celery import Celery
from celery.result import AsyncResult

# Initialize Flask app
app = Flask(__name__, template_folder='.', static_folder='.')

# Configuration
ADMIN_USERNAME = os.getenv('ADMIN_USERNAME', 'admin')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'admin123')
SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost/gharfix')

app.config['SECRET_KEY'] = SECRET_KEY

# Initialize Celery
celery_app = Celery(app.name)
celery_app.conf.broker_url = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')

# Database connection
def get_db():
    """Create database connection"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
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
            # Extract Bearer token
            token = token.replace('Bearer ', '')
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            current_user = data.get('username')
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401
        
        return f(current_user, *args, **kwargs)
    return decorated

# Routes
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        return render_template('dashboard_advanced.html')
    
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    
    # Validate credentials
    if username != ADMIN_USERNAME or password != ADMIN_PASSWORD:
        return jsonify({'error': 'Invalid credentials'}), 401
    
    # Generate JWT token
    token = jwt.encode({
        'username': username,
        'exp': datetime.utcnow() + timedelta(days=30)
    }, app.config['SECRET_KEY'], algorithm='HS256')
    
    return jsonify({'token': token}), 200

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard_advanced.html')

@app.route('/api/all-analytics', methods=['GET'])
@token_required
def get_all_analytics(current_user):
    """Get all analytics data"""
    try:
        conn = get_db()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get total lead events
        cur.execute('SELECT COUNT(*) as count FROM lead_events')
        total_lead_events = cur.fetchone()['count']
        
        # Get unique customers
        cur.execute('SELECT COUNT(DISTINCT customer_id) as count FROM leads')
        unique_customers = cur.fetchone()['count']
        
        # Get status distribution
        cur.execute('''
            SELECT status, COUNT(*) as count
            FROM lead_events
            GROUP BY status
            ORDER BY count DESC
        ''')
        status_distribution = [{'status': row['status'], 'count': row['count']} 
                              for row in cur.fetchall()]
        
        # Get top services
        cur.execute('''
            SELECT service_name as service, COUNT(*) as count
            FROM lead_events
            GROUP BY service_name
            ORDER BY count DESC
            LIMIT 20
        ''')
        top_services = [{'service': row['service'], 'count': row['count']} 
                       for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return jsonify({
            'total_lead_events': total_lead_events,
            'unique_customers': unique_customers,
            'status_distribution': status_distribution,
            'top_services': top_services,
            'sync_status': 'Connected to Bellevie',
            'last_updated': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/filtered-analytics', methods=['GET'])
@token_required
def get_filtered_analytics(current_user):
    """Get filtered analytics data"""
    try:
        status = request.args.get('status', '')
        service = request.args.get('service', '')
        
        conn = get_db()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build query based on filters
        where_clause = 'WHERE 1=1'
        params = []
        
        if status:
            where_clause += ' AND status = %s'
            params.append(status)
        
        if service:
            where_clause += ' AND service_name = %s'
            params.append(service)
        
        # Get total lead events (filtered)
        query = f'SELECT COUNT(*) as count FROM lead_events {where_clause}'
        cur.execute(query, params)
        total_lead_events = cur.fetchone()['count']
        
        # Get unique customers (filtered)
        query = f'''
            SELECT COUNT(DISTINCT customer_id) as count 
            FROM lead_events {where_clause}
        '''
        cur.execute(query, params)
        unique_customers = cur.fetchone()['count']
        
        # Get status distribution (filtered)
        query = f'''
            SELECT status, COUNT(*) as count
            FROM lead_events {where_clause}
            GROUP BY status
            ORDER BY count DESC
        '''
        cur.execute(query, params)
        status_distribution = [{'status': row['status'], 'count': row['count']} 
                              for row in cur.fetchall()]
        
        # Get top services (filtered)
        query = f'''
            SELECT service_name as service, COUNT(*) as count
            FROM lead_events {where_clause}
            GROUP BY service_name
            ORDER BY count DESC
            LIMIT 20
        '''
        cur.execute(query, params)
        top_services = [{'service': row['service'], 'count': row['count']} 
                       for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return jsonify({
            'total_lead_events': total_lead_events,
            'unique_customers': unique_customers,
            'status_distribution': status_distribution,
            'top_services': top_services,
            'sync_status': 'Connected to Bellevie',
            'last_updated': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/rescrape', methods=['POST'])
@token_required
def trigger_rescrape(current_user):
    """Trigger rescrape task"""
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
        task = AsyncResult(task_id, app=celery_app)
        
        return jsonify({
            'status': task.state,
            'result': task.result if task.state == 'SUCCESS' else None,
            'progress': task.info if task.state == 'PROGRESS' else 0
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<format_type>', methods=['GET'])
@token_required
def download_data(current_user, format_type):
    """Download data as CSV or JSON"""
    try:
        conn = get_db()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get all lead events
        cur.execute('''
            SELECT event_id, customer_id, first_name, last_name, 
                   status, submitted_at, service_name
            FROM lead_events
            ORDER BY submitted_at DESC
        ''')
        data = cur.fetchall()
        cur.close()
        conn.close()
        
        if format_type == 'csv':
            # Generate CSV
            import io
            output = io.StringIO()
            output.write('event_id,customer_id,first_name,last_name,status,submitted_at,service_name\n')
            for row in data:
                output.write(f'{row["event_id"]},{row["customer_id"]},{row["first_name"]},{row["last_name"]},{row["status"]},{row["submitted_at"]},{row["service_name"]}\n')
            
            return send_file(
                io.BytesIO(output.getvalue().encode()),
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'gharfix-leads-{datetime.now().strftime("%Y-%m-%d")}.csv'
            )
        
        elif format_type == 'json':
            return jsonify(data), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def server_error(error):
    return jsonify({'error': 'Server error'}), 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
