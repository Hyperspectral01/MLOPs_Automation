from flask import Flask, render_template, request, jsonify, redirect, url_for, Response
import requests
import json
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
import time
from waitress import serve

app = Flask(__name__)

# Configuration
ML_SERVICE_URL = "http://mlapp.training.svc.cluster.local/predict"
DB_SERVICE_URL = "http://dbapp.training.svc.cluster.local"

# -------------------------------
# Prometheus Metrics Definitions
# -------------------------------

# Count how many predictions have been made
PREDICTION_REQUESTS = Counter(
    'prediction_requests_total',
    'Total number of prediction requests received'
)

# Count how many corrections were submitted
CORRECTION_REQUESTS = Counter(
    'correction_requests_total',
    'Total number of correction submissions'
)

# Track response time of the /predict endpoint
PREDICTION_LATENCY = Histogram(
    'prediction_latency_seconds',
    'Time taken to process prediction requests'
)

# Track last prediction value
LAST_PREDICTED_PRICE = Gauge(
    'last_predicted_price',
    'Most recent predicted house price'
)

# Track total errors
ERROR_COUNT = Counter(
    'error_count_total',
    'Total number of errors encountered'
)

# -------------------------------
# Existing Routes
# -------------------------------

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/predict_page')
def predict_page():
    return render_template('predict.html')

@app.route('/predict', methods=['POST'])
@PREDICTION_LATENCY.time()   # Measure how long this route takes
def predict():
    try:
        PREDICTION_REQUESTS.inc()  # Increment total prediction requests
        start_time = time.time()

        # Get form data
        data = {
            'bedrooms': int(request.form['bedrooms']),
            'bathrooms': float(request.form['bathrooms']),
            'sqft_living': int(request.form['sqft_living']),
            'sqft_lot': int(request.form['sqft_lot']),
            'floors': float(request.form['floors']),
            'waterfront': int(request.form['waterfront']),
            'view': int(request.form['view']),
            'condition': int(request.form['condition']),
            'sqft_above': int(request.form['sqft_above']),
            'sqft_basement': int(request.form['sqft_basement']),
            'yr_built': int(request.form['yr_built']),
            'yr_renovated': int(request.form['yr_renovated']),
            'street': request.form['street'],
            'city': request.form['city'],
            'statezip': request.form['statezip'],
            'country': request.form['country']
        }
        
        # Call ML prediction service
        response = requests.post(ML_SERVICE_URL, json=data)
        prediction_result = response.json()
        predicted_price = prediction_result['result']
        
        LAST_PREDICTED_PRICE.set(predicted_price)  # Store the latest price

        # Store prediction in collection_1
        db_data = {
            'table': 'collection_1',
            'data': {**data, 'price': predicted_price}
        }
        requests.post(f"{DB_SERVICE_URL}/store", json=db_data)
        
        return render_template('result.html', 
                             prediction=predicted_price, 
                             user_data=json.dumps(data))
    
    except Exception as e:
        ERROR_COUNT.inc()
        return render_template('error.html', error=str(e))
    finally:
        end_time = time.time()
        latency = end_time - start_time
        PREDICTION_LATENCY.observe(latency)

@app.route('/correct_prediction', methods=['POST'])
def correct_prediction():
    try:
        CORRECTION_REQUESTS.inc()

        user_data = json.loads(request.form['user_data'])
        corrected_price = float(request.form['corrected_price'])
        
        db_data = {
            'table': 'collection_2',
            'data': {**user_data, 'price': corrected_price}
        }
        requests.post(f"{DB_SERVICE_URL}/store", json=db_data)
        
        return render_template('correction_success.html', corrected_price=corrected_price)
    
    except Exception as e:
        ERROR_COUNT.inc()
        return render_template('error.html', error=str(e))

@app.route('/show_predictions')
def show_predictions():
    try:
        response_1 = requests.get(f"{DB_SERVICE_URL}/retrieve/collection_1")
        response_2 = requests.get(f"{DB_SERVICE_URL}/retrieve/collection_2")
        
        predictions = response_1.json().get('data', [])
        corrections = response_2.json().get('data', [])
        
        return render_template('show_predictions.html', 
                             predictions=predictions, 
                             corrections=corrections)
    
    except Exception as e:
        ERROR_COUNT.inc()
        return render_template('error.html', error=str(e))

# -------------------------------
# New Prometheus Metrics Endpoint
# -------------------------------
@app.route('/metrics')
def metrics():
    """
    Exposes metrics to Prometheus in the correct format.
    """
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


if __name__ == '__main__':
    serve(app, host='0.0.0.0', port=5000)
