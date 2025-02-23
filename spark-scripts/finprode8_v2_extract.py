from pyspark.sql import SparkSession
import kagglehub
import os
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)


def extract():

    # Inisialisasi SparkSession
    spark = SparkSession.builder \
        .appName("finprode8_extract") \
        .master("local").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
   
    # Download latest version dataset dari kaggle
    path = kagglehub.dataset_download("sanjanchaudhari/employees-performance-for-hr-analytics")

    # Mengambil file dan folder dari direktori path yang berformat csv
    csv_files = [os.path.join(path, file) for file in os.listdir(path) if file.endswith(".csv")]
    
    if not csv_files:
        print("File CSV tidak ditemukan.")
        return
    
    # Membaca file CSV pertama yang ditemukan dengan Spark
    df = spark.read.csv(csv_files[0], header=True, inferSchema=True)

    # Simpan sebagai Parquet
    df.write.mode("overwrite").parquet("data/extracted.parquet")

    print("Jumlah baris dalam DataFrame extract :", df.count())
    print("Isi folder data/extracted.parquet :")
    print(os.listdir("data/extracted.parquet"))
    print("Extract data berhasil.")
    spark.stop()

if __name__ == "__main__":
    extract()