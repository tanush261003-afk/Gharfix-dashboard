from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from datetime import datetime
import os
import psycopg

# ============================================================================
# DIRECT POSTGRESQL DASHBOARD API
# Reads directly from Neon database (no caching)
# ============================================================================

app = Flask(__name__)
CORS(app)

# Database connection
DATABASE_URL = 'postgresql://neondb_owner:npg_4htwi0nmEdNv@ep-sweet-union-ahmpxyfe-pooler.c-3.us-east-1.aws.neon.tech:5432/Gharfix-leads?sslmode=require'

def get_db_connection():
    """Get fresh connection to Neon database"""
    return psycopg.connect(DATABASE_URL)

# ============================================================================
# ANALYTICS ENDPOINTS - READ DIRECTLY FROM DB
# ============================================================================

@app.route('/api/all-analytics')
def get_all_analytics():
    """Get all analytics - reads directly from PostgreSQL"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Total leads
        cur.execute("SELECT COUNT(*) FROM leads")
        total = cur.fetchone()[0]
        
        # All services
        cur.execute("""
            SELECT COALESCE(sub_category_name, 'Other') as service, COUNT(*) as count 
            FROM leads 
            WHERE sub_category_name IS NOT NULL AND sub_category_name != ''
            GROUP BY sub_category_name 
            ORDER BY count DESC 
            LIMIT 20
        """)
        all_services = [{'service_name': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        # Status distribution (ALL 7 STATUSES)
        cur.execute("""
            SELECT status, COUNT(*) as count 
            FROM leads 
            GROUP BY status 
            ORDER BY count DESC
        """)
        status_dist = [{'status': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        conn.close()
        
        print(f"✅ Returned {total} leads with {len(all_services)} services and {len(status_dist)} statuses")
        
        return jsonify({
            'total_leads': total,
            'all_services': all_services,
            'status_distribution': status_dist,
            'timestamp': str(datetime.now()),
            'data_source': 'postgresql'
        })
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/filtered-analytics')
def get_filtered_analytics():
    """Get filtered analytics by status and service"""
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
        
        # Get total count
        base_query = "SELECT COUNT(*) FROM leads"
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)
        
        cur.execute(base_query)
        total_leads = cur.fetchone()[0]
        
        # Get status distribution for filtered results
        status_query = "SELECT status, COUNT(*) as count FROM leads"
        if where_conditions:
            status_query += " WHERE " + " AND ".join(where_conditions)
        status_query += " GROUP BY status ORDER BY count DESC"
        
        cur.execute(status_query)
        status_dist = [{'status': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        # Get service distribution for filtered results
        service_query = """
            SELECT sub_category_name, COUNT(*) as count 
            FROM leads 
            WHERE sub_category_name IS NOT NULL AND sub_category_name != ''
        """
        if where_conditions:
            service_query += " AND " + " AND ".join(where_conditions)
        service_query += " GROUP BY sub_category_name ORDER BY count DESC LIMIT 20"
        
        cur.execute(service_query)
        all_services = [{'service_name': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        conn.close()
        
        return jsonify({
            'total_leads': total_leads,
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
        
        # Get stats
        cur.execute("SELECT COUNT(*) FROM leads")
        total = cur.fetchone()[0]
        
        cur.execute("SELECT MAX(customer_id) FROM leads")
        max_id = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT status) FROM leads")
        status_count = cur.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'status': 'online',
            'database': 'Connected - Neon PostgreSQL',
            'total_leads': total,
            'max_lead_id': max_id,
            'unique_statuses': status_count,
            'timestamp': str(datetime.now())
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
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
        return "<h1>Dashboard</h1><p>dashboard_advanced.html not found</p>", 404

@app.route('/')
def index():
    return jsonify({
        "status": "Gharfix Analytics API - Direct PostgreSQL",
        "version": "2.0",
        "data_source": "Neon PostgreSQL",
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
    print("🚀 Gharfix Analytics API - Direct PostgreSQL")
    print("   Database: Neon (Read-only)")
    print("   Data source: Google Apps Script Scraper")
    print("   Dashboard: http://localhost:10000/dashboard")
    print("="*60 + "\n")
    
    # Test database connection
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM leads")
        count = cur.fetchone()[0]
        conn.close()
        print(f"✅ Database connected! {count} leads in system.\n")
    except Exception as e:
        print(f"❌ Database connection failed: {e}\n")
    
    app.run(host='0.0.0.0', port=10000, debug=False)
