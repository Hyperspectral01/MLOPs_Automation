"""
Flask ML Prediction Service with MLflow Integration and Prometheus Metrics

This service loads the best model from MLflow tracking server and serves predictions
for real estate price estimation using stateless feature hashing preprocessing.

PREPROCESSING DETAILS:
- Uses FeatureHasher for categorical columns (stateless, no fitting required)
- Hash dimension: 10 features per categorical column
- Categorical columns: street, city, statezip, country (4 columns × 10 = 40 hashed features)
- Numerical columns: bedrooms, bathrooms, sqft_living, sqft_lot, floors, waterfront, 
                     view, condition, sqft_above, sqft_basement, yr_built, yr_renovated (12 columns)
- FINAL DATASET DIMENSIONS: 52 features (for prediction input)
- Feature order: [12 numerical features] + [40 hashed categorical features]
"""

from flask import Flask, request, jsonify
from waitress import serve
import mlflow
from mlflow.tracking import MlflowClient
import pandas as pd
import numpy as np
from sklearn.feature_extraction import FeatureHasher
import time
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from functools import wraps

# Initialize Flask app
app = Flask(__name__)

# Preprocessing constants
NUMERICAL_COLS = [
    "bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors",
    "waterfront", "view", "condition", "sqft_above", "sqft_basement",
    "yr_built", "yr_renovated"
]
CATEGORICAL_COLS = ["street", "city", "statezip", "country"]
HASH_FEATURES = 10  # Hash dimension per categorical column

# Global variables for model and experiment
loaded_model = None
experiment_name = None
model_version = None
model_run_id = None

# Prometheus metrics
prediction_counter = Counter(
    'prediction_requests_total',
    'Total number of prediction requests',
    ['status', 'experiment']
)
prediction_duration = Histogram(
    'prediction_duration_seconds',
    'Time spent processing prediction requests',
    ['experiment']
)
prediction_errors = Counter(
    'prediction_errors_total',
    'Total number of prediction errors',
    ['error_type', 'experiment']
)
active_requests = Gauge(
    'active_prediction_requests',
    'Number of active prediction requests'
)
model_load_time = Gauge(
    'model_load_time_seconds',
    'Time taken to load the model'
)
last_prediction_time = Gauge(
    'last_prediction_timestamp',
    'Timestamp of the last prediction'
)
predicted_price_histogram = Histogram(
    'predicted_price_dollars',
    'Distribution of predicted prices',
    buckets=[10000, 50000, 100000, 250000, 500000, 1000000, 2500000, 5000000]
)


def preprocess_data_for_prediction(data: dict) -> np.ndarray:
    """
    Preprocess single prediction data using stateless feature hashing.
    
    Args:
        data: Dictionary with keys matching NUMERICAL_COLS and CATEGORICAL_COLS
    
    Returns:
        Numpy array with 52 features ready for prediction
    """
    # Create a dummy dataframe with price column (required by preprocessing)
    # We'll remove it after preprocessing
    df_data = data.copy()
    df_data['price'] = 0  # Dummy price value
    df = pd.DataFrame([df_data])
    
    # Process numerical columns
    numerical_features = []
    for col in NUMERICAL_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            numerical_features.append(df[col].values[0])
        else:
            numerical_features.append(0)
    
    # Process categorical columns with feature hashing
    categorical_features = []
    for col in CATEGORICAL_COLS:
        if col in df.columns:
            # Convert to string and handle missing values
            value = str(df[col].values[0]) if pd.notna(df[col].values[0]) else "unknown"
            
            # Use FeatureHasher (stateless - no fitting required)
            hasher = FeatureHasher(n_features=HASH_FEATURES, input_type="string")
            hashed = hasher.transform([[value]])
            hashed_array = hashed.toarray()[0]
            categorical_features.extend(hashed_array)
        else:
            # If column missing, add zeros
            categorical_features.extend([0] * HASH_FEATURES)
    
    # Combine all features in correct order: numerical (12) + categorical hashed (40) = 52
    all_features = numerical_features + categorical_features
    
    return np.array(all_features).reshape(1, -1)


def load_best_model_from_mlflow():
    """
    Load the best model from MLflow tracking server.
    Searches for the best run based on metrics and loads the model.
    """
    global loaded_model, experiment_name, model_version, model_run_id
    
    start_time = time.time()
    
    try:
        # Set MLflow tracking URI to Kubernetes service
        mlflow.set_tracking_uri("http://mlflow.ml.svc.cluster.local")
        client = MlflowClient()
        
        # Get all experiments (you can filter by specific experiment name if needed)
        experiments = client.search_experiments()
        
        best_run = None
        best_metric = float('-inf')  # Assuming we're maximising (e.g., Accuracy)
        best_experiment = None
        
        # Search through all experiments and runs
        for experiment in experiments:
            if experiment.lifecycle_stage == "deleted":
                continue
            
            # Search runs in this experiment, ordered by metrics
            runs = mlflow.search_runs(
                experiment_ids=[experiment.experiment_id],
                order_by=["metrics.accuracy DESC"],  # Adjust metric name as needed
                max_results=1
            )
            
            if not runs.empty:
                run = runs.iloc[0]
                metric_value = run.get('metrics.accuracy')
                if pd.isna(metric_value):
                    continue
                if metric_value > best_metric:
                    best_metric = metric_value
                    best_run = run
                    best_experiment = experiment
        
        if best_run is None:
            print("WARNING: No runs found in MLflow. Using dummy model.")
            loaded_model = None
            experiment_name = "dummy"
            return
        
        # Extract information about the best run
        experiment_name = best_experiment.name
        model_run_id = best_run['run_id']
        
        print(f"Loading best model from experiment: {experiment_name}")
        print(f"Run ID: {model_run_id}")
        print(f"Best metric (Accuracy): {best_metric}")
        
        # Load the model from the best run
        model_uri = f"runs:/{model_run_id}/model"
        loaded_model = mlflow.pyfunc.load_model(model_uri)
        
        load_time = time.time() - start_time
        model_load_time.set(load_time)
        
        print(f"Model loaded successfully in {load_time:.2f} seconds")
        print(f"Experiment type: {experiment_name}")
        
    except Exception as e:
        print(f"Error loading model from MLflow: {e}")
        print("Falling back to dummy model")
        loaded_model = None
        experiment_name = "dummy"
        model_load_time.set(time.time() - start_time)


@app.route('/predict', methods=['POST'])
def predict():
    """
    Prediction endpoint that processes input data and returns price prediction.
    """
    active_requests.inc()
    start_time = time.time()
    
    try:
        # Get the input data from request (JSON format)
        data = request.json
        
        if not data:
            prediction_errors.labels(error_type='no_data', experiment=experiment_name or 'unknown').inc()
            prediction_counter.labels(status='error', experiment=experiment_name or 'unknown').inc()
            active_requests.dec()
            return jsonify({'error': 'No data provided'}), 400
        
        # Log the received data
        print("Received prediction request:")
        print(f"Bedrooms: {data.get('bedrooms')}")
        print(f"Bathrooms: {data.get('bathrooms')}")
        print(f"Living Area: {data.get('sqft_living')} sqft")
        print(f"City: {data.get('city')}")
        
        # Validate required fields
        required_fields = NUMERICAL_COLS + CATEGORICAL_COLS
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            prediction_errors.labels(error_type='missing_fields', experiment=experiment_name or 'unknown').inc()
            prediction_counter.labels(status='error', experiment=experiment_name or 'unknown').inc()
            active_requests.dec()
            return jsonify({
                'error': f'Missing required fields: {missing_fields}'
            }), 400
        
        # Preprocess the data to get 52 features
        features = preprocess_data_for_prediction(data)
        print(f"Preprocessed features shape: {features.shape}")
        
        # Handle different experiment types
        if experiment_name == "linear_regression":
            print("Using Linear Regression model")
            
            if loaded_model is None:
                print("WARNING: Model not loaded, using fallback prediction")
                predicted_price = 50000.00
            else:
                # Use the loaded linear regression model
                prediction = loaded_model.predict(features)
                predicted_price = float(prediction[0])
        
        elif experiment_name == "random_forest":
            print("Using Random Forest model")
            
            if loaded_model is None:
                print("WARNING: Model not loaded, using fallback prediction")
                predicted_price = 50000.00
            else:
                prediction = loaded_model.predict(features)
                predicted_price = float(prediction[0])
        
        elif experiment_name == "xgboost":
            print("Using XGBoost model")
            
            if loaded_model is None:
                print("WARNING: Model not loaded, using fallback prediction")
                predicted_price = 50000.00
            else:
                prediction = loaded_model.predict(features)
                predicted_price = float(prediction[0])
        
        else:
            # Default case or dummy model
            print(f"Using default prediction for experiment: {experiment_name}")
            
            if loaded_model is None:
                predicted_price = 50000.00
            else:
                prediction = loaded_model.predict(features)
                predicted_price = float(prediction[0])
        
        # Ensure price is positive
        predicted_price = max(0, predicted_price)
        
        # Update metrics
        duration = time.time() - start_time
        prediction_duration.labels(experiment=experiment_name or 'unknown').observe(duration)
        prediction_counter.labels(status='success', experiment=experiment_name or 'unknown').inc()
        predicted_price_histogram.observe(predicted_price)
        last_prediction_time.set(time.time())
        active_requests.dec()
        
        print(f"Predicted price: ${predicted_price:.2f}")
        
        # Return response in the expected format
        return jsonify({
            'result': round(predicted_price, 2)
        }), 200
    
    except Exception as e:
        duration = time.time() - start_time
        prediction_duration.labels(experiment=experiment_name or 'unknown').observe(duration)
        prediction_errors.labels(error_type='processing_error', experiment=experiment_name or 'unknown').inc()
        prediction_counter.labels(status='error', experiment=experiment_name or 'unknown').inc()
        active_requests.dec()
        
        print(f"Error during prediction: {e}")
        import traceback
        traceback.print_exc()
        
        return jsonify({
            'error': str(e)
        }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """
    Health check endpoint for Kubernetes probes.
    """
    return jsonify({
        'status': 'healthy',
        'service': 'ML Prediction Service',
        'model_loaded': loaded_model is not None,
        'experiment': experiment_name,
        'run_id': model_run_id
    }), 200


@app.route('/metrics', methods=['GET'])
def metrics():
    """
    Prometheus metrics endpoint.
    Exposes metrics for monitoring prediction service performance.
    """
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}


@app.route('/model-info', methods=['GET'])
def model_info():
    """
    Endpoint to get information about the currently loaded model.
    """
    return jsonify({
        'experiment_name': experiment_name,
        'run_id': model_run_id,
        'model_loaded': loaded_model is not None,
        'mlflow_tracking_uri': mlflow.get_tracking_uri()
    }), 200


if __name__ == '__main__':
    print("=" * 80)
    print("Starting ML Prediction Service")
    print("=" * 80)
    
    # Load the best model from MLflow on startup
    print("\nLoading best model from MLflow...")
    load_best_model_from_mlflow()
    
    print("\nStarting Flask server on port 5001...")
    print("Endpoints available:")
    print("  - POST /predict : Make predictions")
    print("  - GET /health : Health check")
    print("  - GET /metrics : Prometheus metrics")
    print("  - GET /model-info : Model information")
    print("=" * 80)
    
    serve(app, host='0.0.0.0', port=5001)