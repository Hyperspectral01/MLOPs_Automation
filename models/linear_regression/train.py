"""
Linear Regression Training Script for MLOps Pipeline

This script trains a Linear Regression model on versioned data from GCS,
tracks experiments using MLflow, and supports incremental training.

The script:
1. Loads existing model from MLflow (if available) or creates new one
2. Reads versioned training data from GCS bucket
3. Trains model incrementally on new data versions
4. Logs model, metrics, and parameters to MLflow
5. Tracks which data versions have been used for training

Training Process:
- Reads data from BUCKET_2/versioned/v{N}/train.csv and valid.csv
- Data format: 52 features + 1 target (price) = 53 columns
- Uses "trained_till_version" tag to track incremental training
- Logs accuracy, RMSE, MAE, and R² metrics to MLflow
"""

import os
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
from google.cloud import storage
import io
import re
from pathlib import Path

# Environment variables
BUCKET_2 = os.getenv("BUCKET_2")

# MLflow configuration
MLFLOW_TRACKING_URI = "http://mlflow.ml.svc.cluster.local"

# Get parent directory name (experiment name)
current_file_path = Path(__file__).resolve()
EXPERIMENT_NAME = current_file_path.parent.name
print(f"Experiment name (from parent directory): {EXPERIMENT_NAME}")


def get_gcs_client() -> storage.Client:
    """Initialize and return GCS client."""
    return storage.Client()


def get_latest_version_from_gcs(bucket_name: str) -> int:
    """
    Get the latest version number from versioned folder in GCS.
    
    Args:
        bucket_name: Name of the GCS bucket
    
    Returns:
        Latest version number (e.g., 9 for v9)
    """
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix="versioned/")
        
        versions = []
        for blob in blobs:
            match = re.search(r"versioned/v(\d+)/", blob.name)
            if match:
                versions.append(int(match.group(1)))
        
        latest = max(versions) if versions else 0
        print(f"Latest version in GCS: v{latest}")
        return latest
    except Exception as e:
        print(f"Error getting latest version from GCS: {e}")
        return 0


def read_csv_from_gcs(bucket_name: str, blob_name: str) -> pd.DataFrame:
    """
    Read CSV file from GCS bucket.
    
    Args:
        bucket_name: Name of the GCS bucket
        blob_name: Path to the blob (e.g., "versioned/v1/train.csv")
    
    Returns:
        DataFrame with the CSV data
    """
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            raise FileNotFoundError(f"Blob {blob_name} not found in bucket {bucket_name}")
        
        content = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(content))
        print(f"Read {len(df)} rows from {blob_name}")
        return df
    except Exception as e:
        print(f"Error reading {blob_name} from {bucket_name}: {e}")
        raise


def load_versioned_data(bucket_name: str, start_version: int, end_version: int):
    """
    Load and merge training data from multiple versions.
    
    Args:
        bucket_name: Name of the GCS bucket
        start_version: Starting version (inclusive)
        end_version: Ending version (inclusive)
    
    Returns:
        Tuple of (merged_train_df, merged_valid_df)
    """
    all_train_dfs = []
    all_valid_dfs = []
    
    print(f"\nLoading data from versions v{start_version} to v{end_version}...")
    
    for version in range(start_version, end_version + 1):
        train_path = f"versioned/v{version}/train.csv"
        valid_path = f"versioned/v{version}/valid.csv"
        
        try:
            print(f"\n  Loading v{version}...")
            train_df = read_csv_from_gcs(bucket_name, train_path)
            valid_df = read_csv_from_gcs(bucket_name, valid_path)
            
            all_train_dfs.append(train_df)
            all_valid_dfs.append(valid_df)
        except Exception as e:
            print(f"  Warning: Could not load v{version}: {e}")
            continue
    
    if not all_train_dfs:
        raise ValueError(f"No training data found between v{start_version} and v{end_version}")
    
    # Merge all dataframes
    merged_train = pd.concat(all_train_dfs, ignore_index=True)
    merged_valid = pd.concat(all_valid_dfs, ignore_index=True)
    
    print(f"\nMerged training data: {len(merged_train)} rows")
    print(f"Merged validation data: {len(merged_valid)} rows")
    print(f"Data shape: {merged_train.shape}")
    
    return merged_train, merged_valid


def get_or_create_experiment(client: MlflowClient, experiment_name: str) -> str:
    """
    Get existing experiment or create new one.
    
    Args:
        client: MLflow client
        experiment_name: Name of the experiment
    
    Returns:
        Experiment ID
    """
    experiment = client.get_experiment_by_name(experiment_name)
    
    if experiment is not None:
        print(f"Found existing experiment: {experiment_name} (ID: {experiment.experiment_id})")
        return experiment.experiment_id
    else:
        print(f"Creating new experiment: {experiment_name}")
        experiment_id = client.create_experiment(experiment_name)
        return experiment_id


def get_best_model_and_version(client: MlflowClient, experiment_name: str):
    """
    Get the best model from existing runs and the trained_till_version.
    
    Args:
        client: MLflow client
        experiment_name: Name of the experiment
    
    Returns:
        Tuple of (model, trained_till_version, best_run_id)
        Returns (None, 0, None) if no runs exist
    """
    try:
        # Search for runs with best accuracy
        runs = mlflow.search_runs(
            experiment_names=[experiment_name],
            order_by=["metrics.accuracy DESC"],
            max_results=1
        )
        
        if runs.empty:
            print("No existing runs found. Starting from scratch.")
            return None, 0, None
        
        best_run = runs.iloc[0]
        best_run_id = best_run['run_id']
        accuracy = best_run.get('metrics.accuracy', 0)
        
        print(f"\nFound best existing run:")
        print(f"  Run ID: {best_run_id}")
        print(f"  Accuracy: {accuracy:.4f}")
        
        # Get the trained_till_version tag
        run_data = client.get_run(best_run_id)
        trained_till_version_tag = run_data.data.tags.get('trained_till_version', 'v0')
        
        # Extract numeric version (e.g., "v3" -> 3)
        version_match = re.search(r'v(\d+)', trained_till_version_tag)
        trained_till_version = int(version_match.group(1)) if version_match else 0
        
        print(f"  Trained till version: v{trained_till_version}")
        
        # Load the model
        model_uri = f"runs:/{best_run_id}/model"
        loaded_model = mlflow.sklearn.load_model(model_uri)
        print(f"  Model loaded successfully")
        
        return loaded_model, trained_till_version, best_run_id
        
    except Exception as e:
        print(f"Error loading best model: {e}")
        return None, 0, None


def train_model(model, X_train, y_train):
    """
    Train or continue training a Linear Regression model.
    
    For Linear Regression, we'll use partial_fit if available,
    or retrain on combined data.
    
    Args:
        model: Existing model or None
        X_train: Training features
        y_train: Training targets
    
    Returns:
        Trained model
    """
    if model is None:
        print("Training new Linear Regression model...")
        model = LinearRegression()
    else:
        print("Continuing training of existing model...")
        # Linear Regression doesn't have partial_fit, so we retrain
        # In practice, for linear regression, we can combine with previous training
        print("Note: Retraining Linear Regression on new data")
    
    model.fit(X_train, y_train)
    print("Model training completed")
    
    return model


def evaluate_model(model, X_train, y_train, X_valid, y_valid):
    """
    Evaluate model and compute metrics.
    
    Args:
        model: Trained model
        X_train: Training features
        y_train: Training targets
        X_valid: Validation features
        y_valid: Validation targets
    
    Returns:
        Dictionary of metrics
    """
    # Training predictions
    train_pred = model.predict(X_train)
    train_mse = mean_squared_error(y_train, train_pred)
    train_rmse = np.sqrt(train_mse)
    train_mae = mean_absolute_error(y_train, train_pred)
    train_r2 = r2_score(y_train, train_pred)
    
    # Validation predictions
    valid_pred = model.predict(X_valid)
    valid_mse = mean_squared_error(y_valid, valid_pred)
    valid_rmse = np.sqrt(valid_mse)
    valid_mae = mean_absolute_error(y_valid, valid_pred)
    valid_r2 = r2_score(y_valid, valid_pred)
    
    # Accuracy as 1 - (MAE / mean(y_valid))
    # This gives a percentage-like metric
    accuracy = 1 - (valid_mae / np.mean(y_valid))
    accuracy = max(0, accuracy)  # Ensure non-negative
    
    metrics = {
        'train_rmse': train_rmse,
        'train_mae': train_mae,
        'train_r2': train_r2,
        'rmse': valid_rmse,
        'mae': valid_mae,
        'r2': valid_r2,
        'accuracy': accuracy
    }
    
    print("\nModel Evaluation:")
    print(f"  Training RMSE: {train_rmse:.2f}")
    print(f"  Training MAE: {train_mae:.2f}")
    print(f"  Training R²: {train_r2:.4f}")
    print(f"  Validation RMSE: {valid_rmse:.2f}")
    print(f"  Validation MAE: {valid_mae:.2f}")
    print(f"  Validation R²: {valid_r2:.4f}")
    print(f"  Accuracy: {accuracy:.4f}")
    
    return metrics


def main():
    """Main training pipeline."""
    print("=" * 80)
    print("LINEAR REGRESSION TRAINING PIPELINE")
    print("=" * 80)
    
    # Step 1: Connect to MLflow
    print(f"\n[Step 1] Connecting to MLflow at {MLFLOW_TRACKING_URI}...")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()
    print("Connected successfully")
    
    # Step 2: Get or create experiment
    print(f"\n[Step 2] Setting up experiment '{EXPERIMENT_NAME}'...")
    experiment_id = get_or_create_experiment(client, EXPERIMENT_NAME)
    mlflow.set_experiment(EXPERIMENT_NAME)
    
    # Step 3: Load best existing model (if any) and get trained_till_version
    print(f"\n[Step 3] Loading best existing model...")
    model, trained_till_version, previous_run_id = get_best_model_and_version(
        client, EXPERIMENT_NAME
    )
    
    if model is None:
        print("No existing model found. Will train from scratch.")
    
    # Step 4: Get latest version from GCS
    print(f"\n[Step 4] Checking latest version in GCS bucket '{BUCKET_2}'...")
    latest_version = get_latest_version_from_gcs(BUCKET_2)
    
    if latest_version == 0:
        print("ERROR: No versioned data found in GCS bucket")
        return
    
    # Step 5: Determine which versions to train on
    start_version = trained_till_version + 1
    
    if start_version > latest_version:
        print(f"\n[INFO] Model already trained on latest data (v{trained_till_version})")
        print("No new training needed. Exiting.")
        return
    
    print(f"\n[Step 5] Will train on versions v{start_version} to v{latest_version}")
    
    # Step 6: Load versioned data
    print(f"\n[Step 6] Loading training data...")
    merged_train, merged_valid = load_versioned_data(
        BUCKET_2, start_version, latest_version
    )
    
    # Verify data format (should be 53 columns: 52 features + 1 price)
    print(f"\nData validation:")
    print(f"  Train shape: {merged_train.shape}")
    print(f"  Valid shape: {merged_valid.shape}")
    print(f"  Columns: {list(merged_train.columns)}")
    
    if merged_train.shape[1] != 53:
        print(f"WARNING: Expected 53 columns, got {merged_train.shape[1]}")
    
    # Separate features and target
    if 'price' not in merged_train.columns:
        raise ValueError("Price column not found in training data")
    
    X_train = merged_train.drop('price', axis=1)
    y_train = merged_train['price']
    X_valid = merged_valid.drop('price', axis=1)
    y_valid = merged_valid['price']
    
    print(f"\nFeatures shape: {X_train.shape}")
    print(f"Target shape: {y_train.shape}")
    
    # Step 7: Start MLflow run and train model
    print(f"\n[Step 7] Starting MLflow run for training...")
    
    with mlflow.start_run(run_name=f"train_v{start_version}_to_v{latest_version}"):
        
        # Log parameters
        params = {
            'model_type': 'linear_regression',
            'trained_from_version': start_version,
            'trained_to_version': latest_version,
            'n_features': X_train.shape[1],
            'n_train_samples': len(X_train),
            'n_valid_samples': len(X_valid)
        }
        
        if previous_run_id:
            params['previous_run_id'] = previous_run_id
        
        print("\nLogging parameters:")
        for key, value in params.items():
            print(f"  {key}: {value}")
            mlflow.log_param(key, value)
        
        # Train the model
        print(f"\n[Step 8] Training model...")
        trained_model = train_model(model, X_train.values, y_train.values)
        
        # Evaluate model
        print(f"\n[Step 9] Evaluating model...")
        metrics = evaluate_model(
            trained_model,
            X_train.values, y_train.values,
            X_valid.values, y_valid.values
        )
        
        # Log metrics
        print("\nLogging metrics to MLflow:")
        for key, value in metrics.items():
            print(f"  {key}: {value:.4f}")
            mlflow.log_metric(key, value)
        
        # Log trained_till_version tag
        trained_till_version_tag = f"v{latest_version}"
        mlflow.set_tag("trained_till_version", trained_till_version_tag)
        print(f"\nSet tag: trained_till_version = {trained_till_version_tag}")
        
        # Log the model
        print(f"\n[Step 10] Logging model to MLflow...")
        mlflow.sklearn.log_model(
            trained_model,
            "model",
            registered_model_name=EXPERIMENT_NAME
        )
        print("Model logged successfully")
        
        run_id = mlflow.active_run().info.run_id
        print(f"\nRun ID: {run_id}")
    
    print("\n" + "=" * 80)
    print("TRAINING COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print(f"Experiment: {EXPERIMENT_NAME}")
    print(f"Trained on: v{start_version} to v{latest_version}")
    print(f"Final Accuracy: {metrics['accuracy']:.4f}")
    print(f"Final RMSE: {metrics['rmse']:.2f}")
    print(f"Model logged to MLflow and ready for deployment")
    print("=" * 80)


if __name__ == "__main__":
    main()