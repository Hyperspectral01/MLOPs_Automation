"""
Real Estate Data Processing Daemon

This script processes real estate data from multiple sources (GCP buckets and MongoDB),
preprocesses it using stateless feature hashing, and manages versioned training datasets.

PREPROCESSING DETAILS:
- Uses FeatureHasher for categorical columns (stateless, no fitting required)
- Hash dimension: 10 features per categorical column
- Categorical columns: street, city, statezip, country (4 columns × 10 = 40 hashed features)
- Numerical columns: bedrooms, bathrooms, sqft_living, sqft_lot, floors, waterfront, 
                     view, condition, sqft_above, sqft_basement, yr_built, yr_renovated (12 columns)
- FINAL DATASET DIMENSIONS: 52 features + 1 target (price) = 53 columns total
- Feature order: [12 numerical features] + [40 hashed categorical features] + [price]

The preprocessing is completely stateless - the same categorical values will always 
hash to the same features, allowing incremental training without storing encoders.
"""

import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
from google.cloud import storage
from sklearn.feature_extraction import FeatureHasher
import io
import re
from typing import Optional, Tuple

# Environment variables
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
COLLECTION_2 = os.getenv("COLLECTION_2")
BUCKET_1 = os.getenv("BUCKET_1")
BUCKET_2 = os.getenv("BUCKET_2")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
TRAIN_TEST_SPLIT = float(os.getenv("TRAIN_TEST_SPLIT", "0.8"))

# Column definitions
NUMERICAL_COLS = [
    "bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors",
    "waterfront", "view", "condition", "sqft_above", "sqft_basement",
    "yr_built", "yr_renovated"
]
CATEGORICAL_COLS = ["street", "city", "statezip", "country"]
HASH_FEATURES = 10  # Hash dimension per categorical column


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the real estate data using stateless feature hashing.
    
    This function converts categorical columns to numerical using FeatureHasher
    (stateless) and ensures all numerical columns are properly formatted.
    
    Args:
        df: Raw dataframe with columns: bedrooms, bathrooms, sqft_living, sqft_lot,
            floors, waterfront, view, condition, sqft_above, sqft_basement, yr_built,
            yr_renovated, street, city, statezip, country, price
    
    Returns:
        Preprocessed dataframe with 52 features + 1 price column (53 total)
    """
    df = df.copy()
    
    # Ensure price is at the end
    if "price" in df.columns:
        price = df["price"]
        df = df.drop("price", axis=1)
    else:
        raise ValueError("Price column not found in dataframe")
    
    # Process numerical columns
    for col in NUMERICAL_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    # Process categorical columns with feature hashing
    categorical_features = []
    for col in CATEGORICAL_COLS:
        if col in df.columns:
            # Convert to string and handle missing values
            df[col] = df[col].astype(str).fillna("unknown")
            
            # Use FeatureHasher (stateless - no fitting required)
            hasher = FeatureHasher(n_features=HASH_FEATURES, input_type="string")
            hashed = hasher.transform(df[col].apply(lambda x: [x]))
            hashed_df = pd.DataFrame(
                hashed.toarray(),
                columns=[f"{col}_hash_{i}" for i in range(HASH_FEATURES)]
            )
            categorical_features.append(hashed_df)
    
    # Combine numerical columns
    numerical_df = df[NUMERICAL_COLS]
    
    # Combine all features
    processed_df = pd.concat([numerical_df] + categorical_features, axis=1)
    
    # Add price at the end
    processed_df["price"] = price.values
    
    return processed_df


def get_gcs_client() -> storage.Client:
    """Initialize and return GCS client."""
    return storage.Client()


def read_csv_from_gcs(bucket_name: str, blob_name: str) -> Optional[pd.DataFrame]:
    """Read CSV file from GCS bucket."""
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            return None
        
        content = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(content))
        return df
    except Exception as e:
        print(f"Error reading {blob_name} from {bucket_name}: {e}")
        return None


def delete_blob_from_gcs(bucket_name: str, blob_name: str) -> bool:
    """Delete a blob from GCS bucket."""
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        print(f"Deleted {blob_name} from {bucket_name}")
        return True
    except Exception as e:
        print(f"Error deleting {blob_name} from {bucket_name}: {e}")
        return False


def upload_csv_to_gcs(bucket_name: str, blob_name: str, df: pd.DataFrame) -> bool:
    """Upload CSV dataframe to GCS bucket."""
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
        print(f"Uploaded {blob_name} to {bucket_name}")
        return True
    except Exception as e:
        print(f"Error uploading {blob_name} to {bucket_name}: {e}")
        return False


def get_latest_version(bucket_name: str) -> int:
    """Get the latest version number from versioned folder."""
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix="versioned/")
        
        versions = []
        for blob in blobs:
            match = re.search(r"versioned/v(\d+)/", blob.name)
            if match:
                versions.append(int(match.group(1)))
        
        return max(versions) if versions else 0
    except Exception as e:
        print(f"Error getting latest version: {e}")
        return 0


def read_mongodb_collection() -> pd.DataFrame:
    """Read all documents from MongoDB collection and clear it."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[COLLECTION_2]
        
        # Read all documents
        documents = list(collection.find())
        
        if not documents:
            print("No documents found in MongoDB collection")
            return pd.DataFrame()
        
        # Convert to dataframe
        df = pd.DataFrame(documents)
        
        # Drop _id column
        if "_id" in df.columns:
            df = df.drop("_id", axis=1)
        
        # Clear the collection
        collection.delete_many({})
        print(f"Read and deleted {len(documents)} documents from MongoDB")
        
        client.close()
        return df
    except Exception as e:
        print(f"Error reading from MongoDB: {e}")
        return pd.DataFrame()


def main():
    """Main daemon process."""
    print("=" * 80)
    print("Starting Real Estate Data Processing Daemon")
    print("=" * 80)
    
    # Step 1: Read existing train/valid from instream folder
    print("\n[Step 1] Checking for existing train/valid data in instream...")
    train1_df = read_csv_from_gcs(BUCKET_2, "instream/train.csv")
    valid1_df = read_csv_from_gcs(BUCKET_2, "instream/valid.csv")
    
    if train1_df is not None:
        delete_blob_from_gcs(BUCKET_2, "instream/train.csv")
        print(f"Found and loaded train1.csv with {len(train1_df)} rows")
    else:
        train1_df = pd.DataFrame()
        print("No existing train.csv found")
    
    if valid1_df is not None:
        delete_blob_from_gcs(BUCKET_2, "instream/valid.csv")
        print(f"Found and loaded valid1.csv with {len(valid1_df)} rows")
    else:
        valid1_df = pd.DataFrame()
        print("No existing valid.csv found")
    
    # Step 2: Read data.csv from BUCKET_1
    print("\n[Step 2] Checking for data.csv in BUCKET_1...")
    csv1_df = read_csv_from_gcs(BUCKET_1, "data.csv")
    
    if csv1_df is not None:
        delete_blob_from_gcs(BUCKET_1, "data.csv")
        # Drop date column if exists
        if "date" in csv1_df.columns:
            csv1_df = csv1_df.drop("date", axis=1)
        print(f"Found and loaded csv-1 with {len(csv1_df)} rows")
    else:
        csv1_df = pd.DataFrame()
        print("No data.csv found in BUCKET_1")
    
    # Step 3: Read from MongoDB
    print("\n[Step 3] Reading data from MongoDB...")
    csv2_df = read_mongodb_collection()
    print(f"Loaded csv-2 with {len(csv2_df)} rows from MongoDB")
    
    # Step 4: Merge csv-1 and csv-2
    print("\n[Step 4] Merging data sources...")
    if not csv1_df.empty and not csv2_df.empty:
        merged_df = pd.concat([csv1_df, csv2_df], ignore_index=True)
    elif not csv1_df.empty:
        merged_df = csv1_df
    elif not csv2_df.empty:
        merged_df = csv2_df
    else:
        print("No new data to process")
        return
    
    # Ensure price is at the end
    if "price" in merged_df.columns:
        cols = [col for col in merged_df.columns if col != "price"] + ["price"]
        merged_df = merged_df[cols]
    
    print(f"Merged dataframe has {len(merged_df)} rows")
    
    # Step 5: Preprocess the data
    print("\n[Step 5] Preprocessing data...")
    preprocessed_df = preprocess_data(merged_df)
    print(f"Preprocessed data shape: {preprocessed_df.shape}")
    print(f"Columns: {list(preprocessed_df.columns)}")
    
    # Step 6: Split the data
    print("\n[Step 6] Splitting data into train/test...")
    train_size = int(len(preprocessed_df) * TRAIN_TEST_SPLIT)
    shuffled_df = preprocessed_df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    new_train_df = shuffled_df[:train_size]
    new_valid_df = shuffled_df[train_size:]
    print(f"New train: {len(new_train_df)} rows, New valid: {len(new_valid_df)} rows")
    
    # Step 7: Merge with existing train/valid data
    print("\n[Step 7] Merging with existing train/valid data...")
    if not train1_df.empty:
        final_train_df = pd.concat([train1_df, new_train_df], ignore_index=True)
    else:
        final_train_df = new_train_df
    
    if not valid1_df.empty:
        final_valid_df = pd.concat([valid1_df, new_valid_df], ignore_index=True)
    else:
        final_valid_df = new_valid_df
    
    print(f"Final train: {len(final_train_df)} rows, Final valid: {len(final_valid_df)} rows")
    
    # Step 8 & 9: Check batch size and upload appropriately
    print("\n[Step 8-9] Checking batch size and uploading data...")
    if len(final_train_df) >= BATCH_SIZE:
        print(f"Train size ({len(final_train_df)}) >= batch size ({BATCH_SIZE})")
        print("Creating new version in versioned folder...")
        
        latest_version = get_latest_version(BUCKET_2)
        new_version = latest_version + 1
        print(f"Latest version: v{latest_version}, Creating: v{new_version}")
        
        upload_csv_to_gcs(BUCKET_2, f"versioned/v{new_version}/train.csv", final_train_df)
        upload_csv_to_gcs(BUCKET_2, f"versioned/v{new_version}/valid.csv", final_valid_df)
    else:
        print(f"Train size ({len(final_train_df)}) < batch size ({BATCH_SIZE})")
        print("Uploading to instream folder for accumulation...")
        
        upload_csv_to_gcs(BUCKET_2, "instream/train.csv", final_train_df)
        upload_csv_to_gcs(BUCKET_2, "instream/valid.csv", final_valid_df)
    
    print("\n" + "=" * 80)
    print("Daemon process completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    main()