import os
import sys
from minio import Minio
from tenacity import retry, stop_after_delay, wait_fixed
from pyspark.sql import SparkSession, functions as F
from sqlalchemy import create_engine

# Configs
MINIO_HOST = os.getenv("MINIO_ENDPOINT", "minio_storage:9000")
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY", "admin_user")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "secure_password")
DB_URL = os.getenv("DATABASE_URL", "postgresql://admin_user:secure_password@postgres_warehouse:5432/fraud_detection_db")
BUCKET = "raw-data-zone"
FILE = "bankdataset.csv"
LOCAL_TEMP = "/tmp/bankdataset.csv"

# 1. Helper to download file (Robustness)
@retry(stop=stop_after_delay(60), wait=wait_fixed(5))
def fetch_raw_data():
    print("--> Step 1: Fetching raw CSV from Data Lake...")
    client = Minio(MINIO_HOST, access_key=MINIO_KEY, secret_key=MINIO_SECRET, secure=False)
    client.fget_object(BUCKET, FILE, LOCAL_TEMP)
    print(f"--> Downloaded to {LOCAL_TEMP}")

# 2. The Core Spark Logic
def run_spark_transformation():
    print("--> Step 2: Initializing Spark Session...")
    # I'm running Spark in 'local' mode since this is a Docker container
    spark = SparkSession.builder \
        .appName("MyFraudPipeline") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    # Reading the CSV
    df = spark.read.csv(LOCAL_TEMP, header=True, inferSchema=True)

    print("--> Cleaning and Transforming data...")
    # I noticed some columns might have spaces, so I'm stripping them
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.strip())

    # Calculating Average Transaction Size (Value / Count)
    df = df.withColumn("Avg_Txn_Size", F.col("Value") / F.col("Transaction_count"))

    # Report 1: Daily Trends
    r1 = df.groupBy("Date", "Domain").agg(F.mean("Avg_Txn_Size").alias("Daily_Avg_Value"))

    # Report 2: City Performance
    r2 = df.groupBy("Location").agg(
        F.mean("Avg_Txn_Size").alias("Avg_Txn_Value"),
        F.mean("Transaction_count").alias("Avg_Txn_Count")
    )

    # Report 3: Leaderboard
    r3 = df.groupBy("Domain").agg(F.sum("Value").alias("Total_Value")) \
           .orderBy(F.desc("Total_Value"))

    print("--> Transformation complete. Converting to Pandas for DB load...")
    # Converting to Pandas only at the very end for easier SQL insertion
    return r1.toPandas(), r2.toPandas(), r3.toPandas(), spark

# 3. Saving to Warehouse
@retry(stop=stop_after_delay(60), wait=wait_fixed(5))
def load_to_postgres(p1, p2, p3):
    print("--> Step 3: Loading results into PostgreSQL Warehouse...")
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        # Using 'replace' for this assignment run to ensure fresh data
        p1.to_sql('rpt_daily_domain_trends', conn, if_exists='replace', index=False)
        p2.to_sql('rpt_location_performance', conn, if_exists='replace', index=False)
        p3.to_sql('rpt_domain_leaderboard', conn, if_exists='replace', index=False)
    print("--> Success! Reports are live in the Database.")

if __name__ == "__main__":
    try:
        fetch_raw_data()
        df1, df2, df3, spark_session = run_spark_transformation()
        load_to_postgres(df1, df2, df3)
        spark_session.stop()
    except Exception as e:
        print(f"!!! Processing Failed: {e}")
        sys.exit(1)