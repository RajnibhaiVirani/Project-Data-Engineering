import os
import sys
from minio import Minio
from minio.error import S3Error
from tenacity import retry, stop_after_delay, wait_fixed, retry_if_exception_type

# My Configuration Variables
MINIO_HOST = os.getenv("MINIO_ENDPOINT", "minio_storage:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin_user")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "secure_password")
BUCKET = "raw-data-zone"
LOCAL_FILE = "/app/data/bankdataset.csv"
OBJECT_NAME = "bankdataset.csv"

def get_client():
    # DEBUG: Printing credentials to verify they match docker-compose
    print(f"DEBUG: Connecting to {MINIO_HOST}")
    print(f"DEBUG: User={MINIO_ACCESS_KEY}")
    # Helper function to connect to the storage layer
    return Minio(MINIO_HOST, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

@retry(retry=retry_if_exception_type(Exception), wait=wait_fixed(5), stop=stop_after_delay(60))
def upload_file():
    client = get_client()

    # I added a check here: If the bucket doesn't exist, create it.
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        print(f"--> Created new bucket: {BUCKET}")
    else:
        print(f"--> Bucket already exists: {BUCKET}")

    print(f"--> Uploading {OBJECT_NAME} to the Data Lake...")
    client.fput_object(BUCKET, OBJECT_NAME, LOCAL_FILE)
    print("--> Success! File is safely stored in MinIO.")

if __name__ == "__main__":
    print("--- Starting Ingestion Service ---")
    try:
        upload_file()
    except Exception as e:
        print(f"!!! Ingestion Failed: {e}")
        sys.exit(1)