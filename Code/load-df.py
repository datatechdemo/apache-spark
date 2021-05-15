"""
A simple application shows how to load(or read) data into spark dataframe
Run with spark2-submit simple_with_session.py
Author- Andy
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def readdata:
  # Create a dataframe manually, from a list
  df = spark.createDataFrame([(1,'Andy'),(2,'mandy'),(3,'sandy')])
  print(type(df))
  df.show()
  df.printSchema()
  
  # Loading DataFrames: CSV method 1 using spark.read.csv('path')
  player_df =spark.read.option('header', True).csv('gs://dataproc-staging-us-east1-548550014762-rie5an4z/notebooks/jupyter/player.csv')
  player_df.show(5, False)
  display(player_df)
  player_df.printSchema()
  
  # Loading DataFrames: CSV method 2 using spark.read.format('csv').load('path')
  player_df = spark.read.format("csv").load('gs://dataproc-staging-us-east1-548550014762-rie5an4z/notebooks/jupyter/player.csv')
  player_df.show(5, False)
  display(player_df)
  player_df.printSchema()

  # Loading JSON
  people_jsondf = spark.read.json('gs://dataproc-staging-us-east1-548550014762-rie5an4z/notebooks/jupyter/people.json')
  people_jsondf.printSchema()
  people_jsondf.show(5)
  
  # In the same way we can upload other formats such as parquet etc 
  
  # Infer Schema
  player_headersdF = spark.read.option("inferSchema", "true").option('header', True).csv('/user/cloudera/stackexchange/posts_all_csv_with_header')
  player_headersdF.printSchema()
  player_headersdF.show(5)


  # Best practice it is explicitly define the schema
  playerSchema = \
    StructType([
                StructField("id", IntegerType()),
                StructField("player_api_id", IntegerType()),
                StructField("player_name", StringType()),
                StructField("player_fifa_api_id", IntegerType()),
                StructField("birthday", TimestampType()),              
                StructField("height", FloatType()),
                StructField("weight", FloatType())
              ])

  player_schemadf = spark.read.schema(postsSchema).csv('/user/cloudera/stackexchange/posts_all_csv')
  player_schemadf.printSchema()
  player_schemadf.schema
  player_schemadf.dtypes
  player_schemadf.columns



if __name__ == "__main__":
  spark = SparkSession.builder.appName("Simple with Session").getOrCreate()
  def readdata(spark)
  
  spark.stop()
