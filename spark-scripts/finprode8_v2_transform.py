from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import count, asc, col, sum



dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)


def transform():

    spark = SparkSession.builder \
        .appName("finprode8_transform") \
        .master("local").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet("data/extracted.parquet")
    
    #Mengecek data NULL
    print("---------------- MENAMPILKAN BANYAK DATA NULL DI SETIAP KOLOM ----------------")
    df.select([sum(col(c).isNull().cast("int")).alias((c) + "_null") for c in df.columns]).show()
    
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

    """
    Karena kolom unik dari data duplikasi ditampilkan adalah berasal dari kolom 
    employee_id dan departement, maka penghapusan data duplikasi harus berdasarkan kedua kolom tersebut
    """
    df_cleaned = df.dropDuplicates(['employee_id', 'department'])

    print("---------------- MENAMPILKAN HASIL DARI DATA DUPLIKASI YANG TELAH DIHANDLE BERDASARKAN KOLOM EMPLOYEE_ID DAN DEPARTMENT ----------------")
    df_cleaned.where(df_cleaned.employee_id.isin(['64573', '49584'])).orderBy(asc('employee_id')).show()
        
        
    # Simpan sebagai Parquet
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