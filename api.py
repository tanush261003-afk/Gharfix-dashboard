from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from datetime import datetime
import os
from database import db

app = Flask(__name__)
CORS(app)

# Initialize database schema on startup
try:
    print("🔄 Initializing database schema...")
    db.initialize_schema()
    print("✅ Database schema initialized successfully!")
except Exception as e:
    print(f"⚠️ Database initialization: {e}")

# ==================== ANALYTICS ROUTES ====================

@app.route('/api/all-analytics')
def get_all_analytics():
    """Get all-time analytics (complete database analysis)"""
    try:
        conn = db._get_connection()
        cur = conn.cursor()
        
        # Total leads
        cur.execute("SELECT COUNT(*) FROM leads")
        total = cur.fetchone()[0]
        print(f"📊 DEBUG: Total leads = {total}")
        
        # All services
        cur.execute("""
            SELECT 
                COALESCE(sub_category_name, 'Other') as service, 
                COUNT(*) as count 
            FROM leads 
            WHERE sub_category_name IS NOT NULL AND sub_category_name != ''
            GROUP BY sub_category_name 
            ORDER BY count DESC
            LIMIT 15
        """)
        all_services = [{'service_name': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        # Status distribution
        cur.execute("SELECT status, COUNT(*) as count FROM leads GROUP BY status ORDER BY count DESC")
        status_dist = [{'status': row[0], 'count': row[1]} for row in cur.fetchall()]
        
        conn.close()
        
        response_data = {
            'total_leads': total,
            'all_services': all_services,
            'status_distribution': status_dist,
            'timestamp': str(datetime.now())
        }
        
        print(f"✅ Returning: {response_data['total_leads']} total leads")
        return jsonify(response_data)
        
    except Exception as e:
        print(f"❌ Error in get_all_analytics: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/filtered-analytics')
def get_filtered_analytics():
    status = request.args.get('status', '')
    service = request.args.get('service', '')

    try:
        conn = db._get_connection()
        cur = conn.cursor()

        # Build WHERE clause
        where_conditions = []
        params = []

        if status and status != 'All Status':
            where_conditions.append("UPPER(status) = UPPER(%s)")
            params.append(status)
        
        if service and service != 'All Services':
            where_conditions.append("UPPER(sub_category_name) = UPPER(%s)")
            params.append(service)

        # Build query
        base_query = "SELECT * FROM leads"
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)

        print(f"📊 Query: {base_query}")
        print(f"📊 Params: {params}")

        cur.execute(base_query, params)
        leads = cur.fetchall()
        total_leads = len(leads)

        print(f"📊 Found {total_leads} leads")

        # Calculate stats from filtered leads
        status_counts = {}
        service_counts = {}

        for lead in leads:
            # status is at index 18
            lead_status = lead[18] if len(lead) > 18 else 'UNKNOWN'
            status_counts[lead_status] = status_counts.get(lead_status, 0) + 1

            # sub_category_name is at index 15
            lead_service = lead[15] if len(lead) > 15 else 'Other'
            if lead_service:
                service_counts[lead_service] = service_counts.get(lead_service, 0) + 1

        status_distribution = [
            {'status': k, 'count': v} 
            for k, v in sorted(status_counts.items(), key=lambda x: x[1], reverse=True)
        ]

        all_services = [
            {'service_name': k, 'count': v} 
            for k, v in sorted(service_counts.items(), key=lambda x: x[1], reverse=True)
        ]

        conn.close()

        print(f"✅ Status dist: {status_distribution}")
        print(f"✅ Services: {all_services}")

        return jsonify({
            'total_leads': total_leads,
            'all_services': all_services,
            'status_distribution': status_distribution
        })

    except Exception as e:
        print(f"❌ Error in get_filtered_analytics: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e), 'status': 'error'}), 500

# ==================== RESCRAPE ROUTE ====================

@app.route('/api/rescrape', methods=['POST'])
def rescrape_leads():
    """Rescrape ONLY new leads from last saved point"""
    try:
        from scraper import scraper
        
        print("🔄 Starting incremental rescrape...")
        
        # Get last lead count
        conn = db._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM leads")
        last_count = cur.fetchone()[0]
        conn.close()
        
        print(f"📊 Current database has: {last_count} leads")
        
        # Fetch ONLY new leads from Bellevie
        new_leads = scraper.fetch_new_leads(last_count)
        
        if not new_leads:
            return jsonify({
                'status': 'success',
                'message': '✅ No new leads found',
                'total_fetched': 0,
                'inserted': 0,
                'duplicates': 0
            }), 200
        
        print(f"✅ NEW LEADS FETCHED: {len(new_leads)}")
        
        # Insert into database
        result = db.insert_leads(new_leads)
        print(f"✅ Rescrape complete: {result['inserted']} new, {result['duplicates']} duplicates")
        
        return jsonify({
            'status': 'success',
            'message': f"✅ Rescrape complete! {len(new_leads)} new leads checked. {result['inserted']} inserted, {result['duplicates']} duplicates",
            'total_fetched': len(new_leads),
            'inserted': result['inserted'],
            'duplicates': result['duplicates']
        })
            
    except Exception as e:
        print(f"❌ Rescrape error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': f"❌ Error: {str(e)}",
            'error': str(e)
        }), 400

# ==================== DOWNLOAD ROUTES ====================

@app.route('/api/download/json')
def download_json():
    """Download all leads as JSON"""
    try:
        if not os.path.exists('all_leads.json'):
            return jsonify({'error': 'JSON file not found. Please rescrape first.'}), 404
        
        return send_file('all_leads.json',
                        mimetype='application/json',
                        as_attachment=True,
                        download_name=f'gharfix_leads_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
    except Exception as e:
        print(f"❌ JSON download error: {e}")
        return jsonify({'error': str(e)}), 500

# ==================== UTILITY ROUTES ====================

@app.route('/dashboard')
def dashboard():
    try:
        return send_file('dashboard_advanced.html')
    except:
        return "<h1>Dashboard</h1><p>dashboard_advanced.html not found</p>", 404

@app.route('/')
def index():
    return jsonify({"status": "Bellevue Analytics API Running"})

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 Advanced Analytics API")
    print("="*60 + "\n")
    app.run(host='0.0.0.0', port=10000, debug=False)
