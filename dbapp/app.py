from flask import Flask, request, jsonify, Response
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
import time
from waitress import serve

# Load .env (optional in container)
load_dotenv()

app = Flask(__name__)

# ==============================================================
# MongoDB Configuration
# ==============================================================
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DB')
predictions_table = os.getenv("COLLECTION_1")
corrections_table = os.getenv("COLLECTION_2")

# ==============================================================
# Prometheus Metrics
# ==============================================================
STORE_REQUESTS = Counter(
    'store_requests_total',
    'Total number of /store requests received'
)

RETRIEVE_REQUESTS = Counter(
    'retrieve_requests_total',
    'Total number of /retrieve requests received'
)

STORE_LATENCY = Histogram(
    'store_latency_seconds',
    'Time taken to handle /store requests'
)

RETRIEVE_LATENCY = Histogram(
    'retrieve_latency_seconds',
    'Time taken to handle /retrieve requests'
)

DOCUMENTS_INSERTED = Counter(
    'documents_inserted_total',
    'Total number of documents successfully inserted into MongoDB'
)

ERROR_COUNT = Counter(
    'db_errors_total',
    'Total number of database-related errors'
)

LAST_HEALTH_STATUS = Gauge(
    'mongo_health_status',
    'MongoDB health status: 1=healthy, 0=unhealthy'
)

DB_CONNECTIONS = Gauge(
    'mongo_active_connections',
    'Number of active connections to MongoDB'
)

# ============================================================
# MongoDB Connection
# ==============================================================
try:
    client = MongoClient(MONGO_URI)
    
    db = client[MONGO_DB]
    print("MongoDB connection successful!")
    LAST_HEALTH_STATUS.set(1)
except Exception as e:
    print(f"MongoDB connection error: {e}")
    LAST_HEALTH_STATUS.set(0)

# ==============================================================
# ROUTES
# ==============================================================

@app.route('/store', methods=['POST'])
@STORE_LATENCY.time()
def store_data():
    STORE_REQUESTS.inc()
    try:
        data = request.json
        table_name = data.get('table')
        record_data = data.get('data')
        
        if not table_name or not record_data:
            return jsonify({'error': 'Missing table or data'}), 400
        
        if table_name == "collection_1":
            collection = db[predictions_table]
            result = collection.insert_one(record_data)
            DOCUMENTS_INSERTED.inc()
            return jsonify({
                'success': True,
                'message': 'Data stored in collection_1',
                'id': str(result.inserted_id)
            }), 201
        
        elif table_name == "collection_2":
            collection = db[corrections_table]
            query_data = {k: v for k, v in record_data.items() if k != 'price'}
            existing = collection.find_one(query_data)
            
            if existing:
                return jsonify({
                    'success': False,
                    'message': 'Record already exists in collection_2'
                }), 200
            else:
                result = collection.insert_one(record_data)
                DOCUMENTS_INSERTED.inc()
                return jsonify({
                    'success': True,
                    'message': 'Data stored in collection_2',
                    'id': str(result.inserted_id)
                }), 201
        else:
            return jsonify({'error': 'Invalid table name'}), 400
    
    except Exception as e:
        ERROR_COUNT.inc()
        return jsonify({'error': str(e)}), 500


@app.route('/retrieve/<collection_name>', methods=['GET'])
@RETRIEVE_LATENCY.time()
def retrieve_data(collection_name):
    RETRIEVE_REQUESTS.inc()
    try:
        if collection_name == "collection_1":
            actual_collection = predictions_table
        elif collection_name == "collection_2":
            actual_collection = corrections_table
        else:
            actual_collection = collection_name
        
        collection = db[actual_collection]
        documents = list(collection.find({}, {'_id': 0}))
        
        return jsonify({
            'success': True,
            'collection': collection_name,
            'count': len(documents),
            'data': documents
        }), 200
    
    except Exception as e:
        ERROR_COUNT.inc()
        return jsonify({'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    try:
        client.server_info()
        collections = db.list_collection_names()
        LAST_HEALTH_STATUS.set(1)
        DB_CONNECTIONS.set(client.address is not None)
        return jsonify({
            'status': 'healthy',
            'database': MONGO_DB,
            'predictions_collection': predictions_table,
            'corrections_collection': corrections_table,
            'existing_collections': collections
        }), 200
    except Exception as e:
        LAST_HEALTH_STATUS.set(0)
        ERROR_COUNT.inc()
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


# ==============================================================
# PROMETHEUS METRICS ENDPOINT
# ==============================================================
@app.route('/metrics')
def metrics():
    """
    Exposes metrics for Prometheus scraping.
    """
    # Update current active connections (best-effort)
    try:
        DB_CONNECTIONS.set(len(client.nodes))
    except Exception:
        pass
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

# ==============================================================
# MAIN ENTRY POINT
# ==============================================================
if __name__ == '__main__':
    print("="*60)
    print("Database Service Starting...")
    print(f"Running on: http://localhost:5002")
    print(f"MongoDB URI: {MONGO_URI}")
    print(f"Database: {MONGO_DB}")
    print(f"Predictions Collection: {predictions_table}")
    print(f"Corrections Collection: {corrections_table}")
    print("="*60)
    
    try:
        client.server_info()
        LAST_HEALTH_STATUS.set(1)
        print("✓ MongoDB connection successful!")
        print(f"✓ Available collections: {db.list_collection_names()}")
    except Exception as e:
        LAST_HEALTH_STATUS.set(0)
        print(f"✗ MongoDB connection failed: {e}")

    serve(app, host='0.0.0.0', port=5002)
