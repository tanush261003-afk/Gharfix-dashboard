from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from datetime import datetime
import redis
import json
import os
from database import db
from config import REDIS, API_HOST, API_PORT

app = Flask(__name__)
CORS(app)

cache = redis.Redis(host=REDIS['host'], port=REDIS['port'], db=2, decode_responses=True)

# ✅ Initialize database schema on startup
try:
    print("🔄 Initializing database schema...")
    db.initialize_schema()
    print("✅ Database schema initialized successfully!")
except Exception as e:
    print(f"⚠️ Database initialization: {e}")

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
        
        # All services - USE SUB_CATEGORY_NAME
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
        
        # Interested count
        cur.execute("SELECT COUNT(*) FROM leads WHERE UPPER(status) = 'INTERESTED'")
        interested = cur.fetchone()[0]
        
        # Not interested count
        cur.execute("SELECT COUNT(*) FROM leads WHERE UPPER(status) = 'NOT INTERESTED'")
        not_interested = cur.fetchone()[0]
        
        # In progress count
        cur.execute("SELECT COUNT(*) FROM leads WHERE UPPER(status) = 'IN PROGRESS'")
        in_progress = cur.fetchone()[0]
        
        conn.close()
        
        response_data = {
            'total_leads': total,
            'all_services': all_services,
            'status_distribution': status_dist,
            'interested_count': interested,
            'not_interested_count': not_interested,
            'in_progress_count': in_progress,
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
    date_from = request.args.get('from', '')
    date_to = request.args.get('to', '')

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


@app.route('/api/rescrape', methods=['POST'])
def rescrape_leads():
    """Queue rescrape as background task"""
    try:
        from scraper import scraper
        
        print("🔄 Starting rescrape...")
        
        # Fetch ALL leads from Bellevie
        leads = scraper.fetch_all_leads()
        
        if not leads:
            return jsonify({'error': 'No leads fetched from Bellevie', 'status': 'error'}), 400
        
        print(f"✅ TOTAL LEADS FETCHED: {len(leads)}")
        
        # Insert into database - NOTE: Do NOT wait for completion
        # The insertion happens even if we timeout
        try:
            result = db.insert_leads(leads)
            print(f"✅ Rescrape complete: {result['inserted']} new, {result['duplicates']} duplicates")
            
            return jsonify({
                'status': 'success',
                'message': f"Rescrape started! {len(leads)} leads fetched. Check dashboard in 1-2 minutes for updates.",
                'total_fetched': len(leads),
                'note': 'Insertion running in background'
            })
        except Exception as insert_error:
            print(f"⚠️ Insert error (continuing): {insert_error}")
            return jsonify({
                'status': 'partial',
                'message': f"Fetched {len(leads)} leads. Insertion may still complete in background.",
                'total_fetched': len(leads)
            })
            
    except Exception as e:
        print(f"❌ Rescrape error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e), 'status': 'error'}), 500


@app.route('/api/update-analytics', methods=['POST'])
def update_analytics():
    """Manually trigger analytics update"""
    try:
        analytics = db.get_analytics()
        cache.set('analytics', json.dumps(analytics), ex=3600)
        return jsonify({
            'status': 'success',
            'message': 'Analytics updated successfully',
            'data': analytics
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/dashboard')
def dashboard():
    try:
        return send_file('dashboard_advanced.html')
    except:
        return "<h1>Dashboard</h1><p>dashboard_advanced.html not found</p>", 404

@app.route('/')
def index():
    return jsonify({"status": "Bellevie Analytics API Running"})

@app.route('/api/download/csv')
def download_csv():
    """Download all leads as CSV"""
    try:
        if not os.path.exists('all_leads.csv'):
            return jsonify({'error': 'CSV file not found'}), 404
        
        return send_file('all_leads.csv', 
                        mimetype='text/csv',
                        as_attachment=True,
                        download_name=f'gharfix_leads_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/json')
def download_json():
    """Download all leads as JSON"""
    try:
        if not os.path.exists('all_leads.json'):
            return jsonify({'error': 'JSON file not found'}), 404
        
        return send_file('all_leads.json',
                        mimetype='application/json',
                        as_attachment=True,
                        download_name=f'gharfix_leads_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print(f"\n{'='*60}\n🚀 Advanced Analytics API\n{'='*60}")
    print(f"Dashboard: http://localhost:{API_PORT}/dashboard")
    print(f"{'='*60}\n")
    app.run(host=API_HOST, port=API_PORT, debug=False)
