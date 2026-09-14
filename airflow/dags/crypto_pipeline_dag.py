from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pytz

# Timezone
JAKARTA_TZ = pytz.timezone('Asia/Jakarta')

# Default DAG arguments
default_args = {
    'owner': 'crypto-pipeline',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 9, 14, tzinfo=JAKARTA_TZ),
    'email_on_failure': False,
    'email_on_retry': False,
}

# DAG definition
dag = DAG(
    'crypto_streaming_pipeline',
    default_args=default_args,
    description='Crypto prices pipeline: produce → consume → query',
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    catchup=False,
    tags=['crypto', 'streaming', 'pipeline'],
)

# Tasks using BashOperator
task_producer = BashOperator(
    task_id='run_producer',
    bash_command='cd /opt/airflow/src && timeout 30 python producer.py',
    dag=dag,
)

task_consumer = BashOperator(
    task_id='run_consumer',
    bash_command='cd /opt/airflow/src && timeout 10 python consumer.py',
    dag=dag,
)

task_query = BashOperator(
    task_id='run_query',
    bash_command='cd /opt/airflow/src && python query.py',
    dag=dag,
)

# Task dependencies
task_producer >> task_consumer >> task_query