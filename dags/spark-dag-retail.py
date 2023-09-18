from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="spark_airflow_dag_postgres_retail",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="spark for data retail",
    start_date=days_ago(1),
)

Extract = SparkSubmitOperator(
    application="/spark-scripts/app_spark.py",
    driver_class_path='/spark-driver/postgresql-42.2.18.jar', 
    conn_id="spark_tgs",
    task_id="spark_submit_task_postgres",
    dag=spark_dag,
)

Extract