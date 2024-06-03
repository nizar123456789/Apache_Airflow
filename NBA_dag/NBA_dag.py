from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.python import PythonOperator
from NBA_etl import run_nba_etl

default_args={
    
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2024,11,8),
    'email':['karkar.nizar@ensi-uma.tn'],
    'email_in_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}
dag=DAG(
    'NBA_dag',
    default_args=default_args,
    description="etl_code"
)

run_etl=PythonOperator(
    task_id='complete_NBA_etl',
    python_callable=run_nba_etl,
    dag=dag
)


run_nba_etl()