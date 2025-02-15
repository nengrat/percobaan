from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os
import pyspark
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('finprode8')
        .setMaster('local')
        .set("spark.jars", "/spark-scripts/jars/postgresql-42.2.18.jar")
    ))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

#Membuat spark session
#spark_session = SparkSession.builder \
#                .appName('finprode8') \
#                .master('local') \
#                .config("spark.jars", "/opt/postgresql-42.2.18.jar") \
#                .getOrCreate()
#spark_context_session = spark_session.sparkContext
#spark_context_session



#-----EXTRACT-----#

#Membaca file dataset
df = spark.read.csv("/spark-scripts/Uncleaned_employees_final_dataset.csv", header = True)
print("---------------- MENAMPILKAN DATASET ----------------")
df.show(10)

#Menampilkan tipe data schema
print("---------------- MENAMPILKAN TIPE DATA AWAL ----------------")
df.printSchema()

#-----EXTRACT-----#



#-----TRANSFORM-----#

#Mengubah tipe data beberapa kolom menjadi integer
df = df.withColumn("no_of_trainings", F.col("no_of_trainings").cast("integer")) \
       .withColumn("age", F.col("age").cast("integer")) \
       .withColumn("previous_year_rating", F.col("previous_year_rating").cast("integer")) \
       .withColumn("length_of_service", F.col("length_of_service").cast("integer")) \
       .withColumn("KPIs_met_more_than_80", F.col("KPIs_met_more_than_80").cast("integer")) \
       .withColumn("awards_won", F.col("awards_won").cast("integer")) \
       .withColumn("avg_training_score", F.col("avg_training_score").cast("integer"))
#Menampilkan tipe data schema yang telah diubah
print("---------------- MENAMPILKAN TIPE DATA HASIL TRANSFORMASI ----------------")
df.printSchema()


#Mengecek data NULL
print("---------------- MENAMPILKAN BANYAK DATA NULL DI SETIAP KOLOM ----------------")
df.select((F.count(F.when(F.col("employee_id").isNull(), 1)).alias("employee_id_null")), \
          (F.count(F.when(F.col("department").isNull(), 1)).alias("department_null")), \
          (F.count(F.when(F.col("region").isNull(), 1)).alias("region_null")), \
          (F.count(F.when(F.col("education").isNull(), 1)).alias("education_null")), \
          (F.count(F.when(F.col("gender").isNull(), 1)).alias("gender_null")),\
          (F.count(F.when(F.col("recruitment_channel").isNull(), 1)).alias("recruitment_channel_null")),\
          (F.count(F.when(F.col("no_of_trainings").isNull(), 1)).alias("no_of_trainings_null")),\
          (F.count(F.when(F.col("age").isNull(), 1)).alias("age_null")),\
          (F.count(F.when(F.col("previous_year_rating").isNull(), 1)).alias("previous_year_rating_null")),\
          (F.count(F.when(F.col("length_of_service").isNull(), 1)).alias("length_of_service_null")),\
          (F.count(F.when(F.col("KPIs_met_more_than_80").isNull(), 1)).alias("KPIs_met_more_than_80_null")),\
          (F.count(F.when(F.col("awards_won").isNull(), 1)).alias("awards_won_null")),\
          (F.count(F.when(F.col("avg_training_score").isNull(), 1)).alias("avg_training_score_nuli"))).show()

#NULL handling pada kolom education dan previous_year_rating
print("---------------- NULL HANDLING PADA KOLOM EDUCATION & PREVIOUS_YEAR_RATING ----------------")
df = df.withColumn("education", F.coalesce(df["education"], F.lit('unknown'))) #mengubah data null jadi unknown
df = df.withColumn("previous_year_rating", F.coalesce(df["previous_year_rating"], F.lit(0))) #mengubah data null jadi 0

#Mengecek apakah masih ada data NULL
df.select((F.count(F.when(F.col("education").isNull(), 1)).alias("education_null")), \
          (F.count(F.when(F.col("previous_year_rating").isNull(), 1)).alias("previous_year_rating_null"))).show()



#df_filtered_ed = df.select("education").distinct().show()

#-----TRANSFORM-----#



#-----LOAD TO POSTGRES-----#

# Konfigurasi koneksi PostgreSQL
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

postgres_url = f'jdbc:postgresql://{postgres_host}/{postgres_dw_db}'
postgres_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver",
    'stringtype': 'unspecified'
}

# Menyimpan DataFrame ke PostgreSQL
df.write \
    .jdbc(url=postgres_url, \
         table="employees", \
         mode="overwrite", \
         properties=postgres_properties)

#query = "(SELECT * FROM employees WHERE education = 'unknown') as temp"
df_postgres = spark.read.jdbc(url=postgres_url, \
                              table="employees", \
                              properties=postgres_properties)
print("---------------- MENAMPILKAN DATA HASIL LOAD KE POSTGRES ----------------")
df_postgres.show()

#-----LOAD TO POSTGRES-----#
