from pyspark.sql import SparkSession
import kagglehub
import os
import pyspark
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import count, asc, col, sum


dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)


def transform():

    """
    sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
            pyspark
            .SparkConf()
            .setAppName('finprode8_transform')
            .setMaster('local')
            .set("spark.jars", "/spark-scripts/jars/postgresql-42.2.18.jar")
        ))
    sparkcontext.setLogLevel("WARN")

    spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())
    """
    spark = SparkSession.builder \
        .appName("finprode8_transform") \
        .master("local") \
        .config("spark.jars", "/spark-scripts/jars/postgresql-42.2.18.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet("data/extracted.parquet")
    """
    print("---------------- MENAMPILKAN TIPE DATA SEBELUM TRANSFORMASI TRANSFORMASI ----------------")
    df.printSchema()
    
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
    """
    
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

    #Mengecek data duplikat
    print("---------------- MENAMPILKAN BANYAK DATA DUPLIKAT BERDASARKAN EMPLOYEE_ID ----------------")
    df.groupBy("employee_id").agg(count("*").alias("jumlah")).filter("jumlah > 1").show()

    print("---------------- MENAMPILKAN SEMUA BARIS DAN KOLOM DARI EMPLOYEE ID YANG DUPLIKAT ----------------")
    df.where(df.employee_id.isin(['64573', '49584'])).orderBy(asc('employee_id')).show()

    #Karena kolom unik dari data duplikasi ditampilkan adalah berasal dari kolom 
    # employee_id dan departemen, maka penghapusan data duplikasi harus berdasarkan kedua kolom tersebut
    df_cleaned = df.dropDuplicates(['employee_id', 'department'])

    print("---------------- MENAMPILKAN HASIL DARI DATA DUPLIKASI YANG TELAH DIHANDLE BERDASARKAN KOLOM EMPLOYEE_ID DAN DEPARTMENT ----------------")
    df_cleaned.where(df_cleaned.employee_id.isin(['64573', '49584'])).orderBy(asc('employee_id')).show()
        
        
    # Simpan sebagai Parquet untuk langkah berikutnya
    df_cleaned.write.mode("overwrite").parquet("data/transformed.parquet")
    
    print("---------------- MENAMPILKAN DATA HASIL TRASNFORMASI ----------------")
    df_cleaned.show(5)
    print("Jumlah baris dalam DataFrame :", df_cleaned.count())
    print("Isi folder data/transformed.parquet :")
    print(os.listdir("data/transformed.parquet"))
    
    print("Transform data berhasil.")
    spark.stop()

if __name__ == "__main__":
    transform()