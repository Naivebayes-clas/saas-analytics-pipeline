from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "barnap",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "saas_analytics_pipeline",
    default_args=default_args,
    description="End-to-end SaaS analytics: dbt build + data quality",
    schedule_interval="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
) as dag:

    refresh_pipe = BashOperator(
        task_id="refresh_snowpipe",
        bash_command="cd /opt/airflow/dbt && python refresh_pipe.py",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/dbt && dbt run --no-partial-parse --profiles-dir /opt/airflow/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --no-partial-parse --profiles-dir /opt/airflow/dbt",
    )   

    row_count_check = BashOperator(
        task_id="row_count_check",
        bash_command="cd /opt/airflow/dbt && python row_count_check.py",
    )

    refresh_pipe >> dbt_build >> dbt_test >> row_count_check   
