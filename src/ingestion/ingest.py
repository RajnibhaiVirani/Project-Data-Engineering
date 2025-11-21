import os
import sys
import time
import socket
from minio import Minio
from minio.error import S3Error

# Configuration
HOSTNAME = "minio_storage"
PORT = "9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin_user")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "secure_password")
BUCKET_NAME = "rawdata" # Changed to simple name to avoid issues
LOCAL_FILE = "/app/data/bankdataset.csv"
OBJECT_NAME = "bankdataset.csv"

def get_ip_address(hostname):
    # Resolves 'minio_storage' to an IP like '172.18.0.3'
    # This forces the library to use Path-Style addressing
    try:
        return socket.gethostbyname(hostname)
    except Exception as e:
        print(f"FATAL: Could not resolve hostname {hostname}. {e}")
        sys.exit(1)

def run_ingestion():
    print("--- INGESTION SERVICE STARTED ---")
    
    # 1. Resolve IP
    print(f"DEBUG: Resolving IP for {HOSTNAME}...")
    minio_ip = get_ip_address(HOSTNAME)
    endpoint = f"{minio_ip}:{PORT}"
    print(f"DEBUG: Connecting to {endpoint} (IP-based)...")
    
    # 2. Initialize Client
    client = Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # 3. Test Connection (List Buckets)
    try:
        client.list_buckets()
        print("DEBUG: Authentication Successful.")
    except Exception as e:
        print(f"FATAL: Connection Failed. Details: {e}")
        sys.exit(1)

    # 4. Create Bucket
    try:
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            print(f"DEBUG: Created bucket '{BUCKET_NAME}'")
        else:
            print(f"DEBUG: Bucket '{BUCKET_NAME}' already exists.")
    except S3Error as err:
        if err.code in ["BucketAlreadyOwnedByYou", "BucketAlreadyExists"]:
            print(f"DEBUG: Bucket '{BUCKET_NAME}' exists.")
        else:
            print(f"FATAL: Bucket error: {err}")
            sys.exit(1)

    # 5. Upload File
    try:
        print(f"DEBUG: Uploading {OBJECT_NAME}...")
        client.fput_object(BUCKET_NAME, OBJECT_NAME, LOCAL_FILE)
        print("SUCCESS: File uploaded to Data Lake.")
    except Exception as e:
        print(f"FATAL: Upload failed. Details: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Wait loop to ensure MinIO is ready
    print("Waiting 5s for MinIO...")
    time.sleep(5)
    run_ingestion()