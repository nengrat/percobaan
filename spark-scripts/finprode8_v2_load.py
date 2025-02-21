from pyspark.sql import SparkSession
import os
import pyspark
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.types import *
import pyspark.sql.functions as F


dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)


def load():
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
        .appName("finprode8_load") \
        .master("local").getOrCreate()
        #.config("spark.jars", "/spark-scripts/jars/postgresql-42.2.18.jar") \
        #.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet("data/transformed.parquet")


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
                table="employees_v2", \
                mode="overwrite", \
                properties=postgres_properties)

    #query = "(SELECT * FROM employees WHERE education = 'unknown') as temp"
    df_postgres = spark.read.jdbc(url=postgres_url, \
                                table="employees_v2", \
                                properties=postgres_properties)

    print("---------------- MENAMPILKAN DATA HASIL LOAD KE POSTGRES ----------------")
    df_postgres.show()
        
    
    print("Jumlah baris dalam DataFrame:", df_postgres.count())
       
    print("Load data berhasil.")
    spark.stop()

if __name__ == "__main__":
    load()