from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

import pyspark
from pyspark.sql import DataFrame

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    'app',
    default_args=default_args,
    schedule_interval='0 2 * * *', # This means the DAG is not scheduled, it needs to be triggered manually.
    catchup=False,  # Don't backfill for past dates.
):
    def get_data():

        spark_host = "spark://dibimbing-dataeng-spark-master:7077"

        sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
                pyspark
                .SparkConf()
                .setAppName('Dibimbing')
                .setMaster(spark_host)
                .set("spark.jars", "/spark-driver/postgresql-42.2.18.jar")
            ))
        sparkcontext.setLogLevel("WARN")

        spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

        print("Fighting")
    
    task1 = SparkSubmitOperator(
        application=get_data,
        driver_class_path='/spark-driver/postgresql-42.2.18.jar', 
        conn_id="spark_tgs",
        task_id="spark_submit_task2_postgres",
    )