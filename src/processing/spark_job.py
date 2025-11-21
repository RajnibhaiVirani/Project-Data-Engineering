import os
import sys
import time
import socket
from minio import Minio
from pyspark.sql import SparkSession, functions as F
from sqlalchemy import create_engine

# Config
HOSTNAME = "minio_storage"
PORT = "9000"
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "admin_user")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "secure_password")
DB_URL = os.getenv("DATABASE_URL", "postgresql://admin_user:secure_password@postgres_warehouse:5432/fraud_detection_db")
BUCKET = "rawdata" # Must match ingestion bucket
FILE = "bankdataset.csv"
LOCAL_TEMP = "/tmp/bankdataset.csv"

def get_minio_endpoint():
    # Resolve hostname to IP to force Path-Style access
    ip = socket.gethostbyname(HOSTNAME)
    return f"{ip}:{PORT}"

def download_csv():
    endpoint = get_minio_endpoint()
    print(f"[PROC] Connecting to Storage at {endpoint}...")
    client = Minio(endpoint, access_key=MINIO_KEY, secret_key=MINIO_SECRET, secure=False)
    
    # Retry loop
    retries = 10
    for i in range(retries):
        try:
            client.fget_object(BUCKET, FILE, LOCAL_TEMP)
            print(f"[PROC] Success: Downloaded to {LOCAL_TEMP}")
            return
        except Exception as e:
            print(f"[PROC] Waiting for file... ({e})")
            time.sleep(5)
    
    print("[PROC] FATAL: Could not download file.")
    sys.exit(1)

def run_spark_job():
    print("[PROC] Starting Spark Session...")
    spark = SparkSession.builder \
        .appName("FraudBatchProcessor") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    # Read
    try:
        df = spark.read.csv(LOCAL_TEMP, header=True, inferSchema=True)
        print("[PROC] CSV loaded into Spark.")
    except Exception as e:
        print(f"[PROC] Spark Read Failed: {e}")
        sys.exit(1)
    
    # Transform
    print("[PROC] Transforming data...")
    for col in df.columns:
        df = df.withColumnRenamed(col, col.strip())
        
    df = df.withColumn("Avg_Txn_Size", F.col("Value") / F.col("Transaction_count"))
    
    # Aggregate
    r1 = df.groupBy("Date", "Domain").agg(F.mean("Avg_Txn_Size").alias("Daily_Avg_Value"))
    r2 = df.groupBy("Location").agg(F.mean("Avg_Txn_Size").alias("Avg_Txn_Value"), F.mean("Transaction_count").alias("Avg_Txn_Count"))
    r3 = df.groupBy("Domain").agg(F.sum("Value").alias("Total_Value")).orderBy(F.desc("Total_Value"))
           
    return r1.toPandas(), r2.toPandas(), r3.toPandas(), spark

def save_to_postgres(p1, p2, p3):
    print("[PROC] Saving to Postgres...")
    try:
        engine = create_engine(DB_URL)
        with engine.begin() as conn:
            p1.to_sql('rpt_daily_domain_trends', conn, if_exists='replace', index=False)
            p2.to_sql('rpt_location_performance', conn, if_exists='replace', index=False)
            p3.to_sql('rpt_domain_leaderboard', conn, if_exists='replace', index=False)
        print("[PROC] SUCCESS: Reports are live in the Database.")
    except Exception as e:
        print(f"[PROC] Database Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Wait for Ingestion
    time.sleep(10)
    download_csv()
    df1, df2, df3, spark = run_spark_job()
    save_to_postgres(df1, df2, df3)
    spark.stop()