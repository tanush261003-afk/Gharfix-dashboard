from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from datetime import datetime
import os
import psycopg
import subprocess

# ============================================================================
# GHARFIX ANALYTICS API - LEAD EVENTS TABLE
# Reads analytics from lead_events table (shows lead history, not unique customers)
# ============================================================================

app = Flask(__name__)
CORS(app)

# Database connection
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://neondb_owner:npg_4htwi0nmEdNv@ep-sweet-union-ahmpxyfe-pooler.c-3.us-east-1.aws.neon.tech:5432/Gharfix-leads?sslmode=require')
ENABLE_RESCRAPE = os.getenv('ENABLE_RESCRAPE', 'false').lower() == 'true'

def get_db_connection():
    """Get fresh connection to Neon database"""
    return psycopg.connect(DATABASE_URL)

# ============================================================================
# ANALYTICS ENDPOINTS - READ FROM lead_events TABLE (EVENTS, NOT UNIQUE CUSTOMERS)
# ============================================================================

@app.route('/api/all-analytics')
def get_all_analytics():
    """Get all analytics from lead_events table (shows ALL events, not just unique customers)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Total events (not unique customers!)
        cur.execute("SELECT COUNT(*) FROM lead_events")
        total = cur.fetchone()[0]
        
        # All services
        cur.execute("""
            SELECT COALESCE(sub_category_name, 'Other') as service, COUNT(*) as count
            FROM lead_events
            WHERE sub_category_name IS NOT NULL AND sub_category_name != ''
            GROUP BY sub_category_name
            ORDER BY count DESC
            LIMIT 20
        """)
        all_services = [{'service_name': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        # Status distribution (ALL 7 STATUSES from events)
        cur.execute("""
            SELECT status, COUNT(*) as count
            FROM lead_events
            GROUP BY status
            ORDER BY count DESC
        """)
        status_dist = [{'status': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        conn.close()
        
        print(f"✅ Returned {total} events with {len(all_services)} services and {len(status_dist)} statuses")
        
        return jsonify({
            'total_events': total,
            'total_leads': total,  # For UI compatibility
            'all_services': all_services,
            'status_distribution': status_dist,
            'timestamp': str(datetime.now()),
            'data_source': 'lead_events (all events with history)'
        })
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/filtered-analytics')
def get_filtered_analytics():
    """Get filtered analytics by status and service from lead_events table"""
    try:
        status = request.args.get('status', '').strip()
        service = request.args.get('service', '').strip()
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Build query
        where_conditions = []
        
        if status and status != 'All Status':
            where_conditions.append(f"UPPER(status) = '{status.upper()}'")
        
        if service and service != 'All Services':
            where_conditions.append(f"UPPER(sub_category_name) = '{service.upper()}'")
        
        # Get total count from events
        base_query = "SELECT COUNT(*) FROM lead_events"
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)
        
        cur.execute(base_query)
        total_events = cur.fetchone()[0]
        
        # Get status distribution for filtered results
        status_query = "SELECT status, COUNT(*) as count FROM lead_events"
        if where_conditions:
            status_query += " WHERE " + " AND ".join(where_conditions)
        status_query += " GROUP BY status ORDER BY count DESC"
        
        cur.execute(status_query)
        status_dist = [{'status': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        # Get service distribution for filtered results
        service_query = """
            SELECT sub_category_name, COUNT(*) as count
            FROM lead_events
            WHERE sub_category_name IS NOT NULL AND sub_category_name != ''
        """
        if where_conditions:
            service_query += " AND " + " AND ".join(where_conditions)
        service_query += " GROUP BY sub_category_name ORDER BY count DESC LIMIT 20"
        
        cur.execute(service_query)
        all_services = [{'service_name': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        conn.close()
        
        return jsonify({
            'total_events': total_events,
            'total_leads': total_events,  # For UI compatibility
            'all_services': all_services,
            'status_distribution': status_dist
        })
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/system-status')
def system_status():
    """Get system status"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get stats from lead_events
        cur.execute("SELECT COUNT(*) FROM lead_events")
        total_events = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT customer_id) FROM lead_events")
        unique_customers = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT status) FROM lead_events")
        status_count = cur.fetchone()[0]
        
        # Get latest event time
        cur.execute("SELECT MAX(imported_at) FROM lead_events")
        last_updated = cur.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'status': 'online',
            'database': 'Connected - Neon PostgreSQL',
            'total_events': total_events,
            'unique_customers': unique_customers,
            'unique_statuses': status_count,
            'last_updated': str(last_updated) if last_updated else 'Never',
            'timestamp': str(datetime.now())
        })
    
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

# ============================================================================
# RESCRAPE ENDPOINT
# ============================================================================

@app.route('/api/rescrape', methods=['POST'])
def rescrape():
    """Trigger rescrape by running scraper.py"""
    
    if not ENABLE_RESCRAPE:
        return jsonify({'error': 'Rescrape is disabled'}), 403
    
    try:
        # Run the scraper script
        result = subprocess.run(
            ['python', 'scraper.py'],
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )
        
        if result.returncode == 0:
            return jsonify({
                'status': 'success',
                'message': 'Rescrape completed',
                'output': result.stdout[-500:] if result.stdout else 'No output'  # Last 500 chars
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Rescrape failed',
                'error': result.stderr[-500:] if result.stderr else 'Unknown error'
            }), 500
    
    except subprocess.TimeoutExpired:
        return jsonify({
            'status': 'error',
            'message': 'Rescrape timed out after 10 minutes'
        }), 500
    
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

# ============================================================================
# DASHBOARD ROUTES
# ============================================================================

@app.route('/dashboard')
def dashboard():
    """Serve the dashboard HTML"""
    try:
        return send_file('dashboard_advanced.html')
    except:
        return "dashboard_advanced.html not found", 404

@app.route('/')
def index():
    return jsonify({
        "status": "Gharfix Analytics API - Lead Events",
        "version": "3.0",
        "data_source": "lead_events table (all events with history)",
        "dashboard": "/dashboard"
    })

# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def server_error(error):
    return jsonify({'error': 'Server error'}), 500

# ============================================================================
# STARTUP
# ============================================================================

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 Gharfix Analytics API - Lead Events Table")
    print("  Database: Neon PostgreSQL")
    print("  Data source: lead_events table (all events)")
    print("  Dashboard: http://localhost:10000/dashboard")
    print("  Rescrape: " + ("ENABLED ✅" if ENABLE_RESCRAPE else "DISABLED ❌"))
    print("="*60 + "\n")
    
    # Test database connection
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM lead_events")
        event_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT customer_id) FROM lead_events")
        customer_count = cur.fetchone()[0]
        
        conn.close()
        
        print(f"✅ Database connected!")
        print(f"   - {event_count} total events")
        print(f"   - {customer_count} unique customers\n")
    
    except Exception as e:
        print(f"❌ Database connection failed: {e}\n")
    
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)), debug=False)
