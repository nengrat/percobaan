from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.types import *
import pyspark.sql.functions as F


dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)


def load():
    
    # Inisialisasi SparkSession
    spark = SparkSession.builder \
        .appName("finprode8_load") \
        .master("local").getOrCreate()
   
    spark.sparkContext.setLogLevel("WARN")

    # Membaca data hasil transform
    df = spark.read.parquet("data/transformed.parquet")
   
    # Konfigurasi koneksi PostgreSQL
    postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
    postgres_dw_db = os.getenv('POSTGRES_DW_DB')
    postgres_user = os.getenv('POSTGRES_USER')
    postgres_password = os.getenv('POSTGRES_PASSWORD')

    # Menghubungkan Apache Spark ke PostgreSQL dengan JDBC
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

    df_postgres = spark.read.jdbc(url=postgres_url, \
                                table="employees", \
                                properties=postgres_properties)     
    
    print("Jumlah baris dalam DataFrame:", df_postgres.count())
    print("Load data ke PostgreSQL berhasil.")
    spark.stop()

if __name__ == "__main__":
    load()