from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "ratih malini",
}

spark_dag = DAG(
    dag_id="finprode8_v2_etl_dag",
    default_args=default_args,
    schedule_interval=None,
    description="Test v2 for spark submit",
    start_date=days_ago(1),
)

start_task = EmptyOperator( 
    task_id = "start_task",
    dag     = spark_dag, 
)

extract_task = SparkSubmitOperator(
    application="/spark-scripts/finprode8_v2_extract.py",
    conn_id="spark_main",
    task_id="extract_task",
    #packages="org.postgresql:postgresql:42.2.18",
    dag=spark_dag,
)

transform_task = SparkSubmitOperator(
    application="/spark-scripts/finprode8_v2_transform.py",
    conn_id="spark_main",
    task_id="transform_task",
    #packages="org.postgresql:postgresql:42.2.18",
    dag=spark_dag,
)

load_task = SparkSubmitOperator(
    application="/spark-scripts/finprode8_v2_load.py",
    conn_id="spark_main",
    task_id="load_task",
    packages="org.postgresql:postgresql:42.2.18",
    dag=spark_dag,
)

end_task = EmptyOperator( 
    task_id = "end_task",
    dag     = spark_dag, 
)


start_task >> extract_task >> transform_task >> load_task >> end_task
