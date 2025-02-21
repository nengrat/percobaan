from pyspark.sql import SparkSession
import kagglehub
import os
import pyspark
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)


def extract():

    spark = SparkSession.builder \
        .appName("finprode8_extract") \
        .master("local").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
   
    # Download latest version
    path = kagglehub.dataset_download("sanjanchaudhari/employees-performance-for-hr-analytics")

    csv_files = [os.path.join(path, file) for file in os.listdir(path) if file.endswith(".csv")]
    
    if not csv_files:
        print("File CSV tidak ditemukan.")
        return
    
    # Baca file CSV pertama yang ditemukan dengan Spark
    df = spark.read.csv(csv_files[0], header=True, inferSchema=True)
    
    # Simpan sebagai Parquet
    df.write.mode("overwrite").parquet("data/extracted.parquet")
    print("---------------- MENAMPILKAN DATA HASIL EXTRACT ----------------")
    df.show(5)
    df.printSchema()

    print("Jumlah baris dalam DataFrame :", df.count())
    print("Isi folder data/extracted.parquet :")
    print(os.listdir("data/extracted.parquet"))
    
    print("Extract data berhasil.")
    spark.stop()

if __name__ == "__main__":
    extract()