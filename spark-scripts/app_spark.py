import pyspark
from pyspark.sql import DataFrame
import os
import pyspark.sql.functions as F


spark_host = "spark://dibimbing-dataeng-spark-master:7077"

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Dibimbing01')
        .setMaster(spark_host)
        .set("spark.jars", "/spark-driver/postgresql-42.2.18.jar")
    ))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = "jdbc:postgresql://dataeng-postgres:5432/postgres_db"
properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"  # Specify the PostgreSQL driver class
}
table_name_read = "retail"

print(f"table_name: {table_name_read}")

print("JDBC URL:", jdbc_url)
print("Connection Properties:", properties)

file_path = "/spark-driver/postgresql-42.2.18.jar"  # Replace with the actual path to your file

if os.path.exists(file_path):
    print(f"The file {file_path} exists.")
else:
    print(f"The file {file_path} does not exist.")

# List all files in the directory
files = os.listdir("/spark-scripts")

# Print the list of files
for file in files:
    print(f"file: {file}")


df = spark.read.jdbc(url=jdbc_url, table=table_name_read, properties=properties)

df.show()


# DATA VISUALIZATION

# 1. Total transaction by Country
df_pie = df.groupBy('country').agg(F.sum('quantity').alias('total_quantity'))
df_pie = df_pie.orderBy(F.col('total_quantity').desc())
df_pie.show()

# 2. Adding column sub_total to dataframe to calculate total payment 
df = df.withColumn('sub_total', F.round(F.col('unitprice') * F.col('quantity'),2))
df.show(5)

# Print the schema of the DataFrame
df.printSchema()

# 3. Get to know total amount/sub_total by date
df_line = df.groupBy(F.date_format('invoicedate', 'yyyy-MM-dd').alias('invoicedate'))\
.agg(F.round(F.sum('sub_total'),2).alias('total_per_day'))\
.orderBy('invoicedate')
df_line.show()


# Stop the SparkSession
spark.stop()

