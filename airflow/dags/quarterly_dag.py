from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# I set the schedule to run quarterly (every 3 months)
QUARTERLY_SCHEDULE = "0 0 1 */3 *"

default_args = {
    'owner': 'student',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    'quarterly_fraud_pipeline',
    default_args=default_args,
    schedule_interval=QUARTERLY_SCHEDULE,
    catchup=False
) as dag:

    # Just checking if the pipeline is ready to start
    start_task = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "Starting Quarterly Batch Pipeline..."'
    )
    
    # Triggers the ingestion
    ingest_task = BashOperator(
        task_id='trigger_ingestion',
        bash_command='echo "Triggering Ingestion Service..."'
    )
    
    # Triggers the spark processing
    process_task = BashOperator(
        task_id='trigger_spark_processing',
        bash_command='echo "Triggering Spark Job..."'
    )

    start_task >> ingest_task >> process_task