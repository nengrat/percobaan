from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "ratih malini",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="finprode8_elt_dag",
    default_args=default_args,
    schedule_interval=None,
    description="Test for spark submit",
    start_date=days_ago(1),
)

etl_task = SparkSubmitOperator(
    application="/spark-scripts/finprode8_etl.py",
    conn_id="spark_main",
    task_id="etl_task",
    packages="org.postgresql:postgresql:42.2.18",
    dag=spark_dag,
)


etl_task
