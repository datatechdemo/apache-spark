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
  
  # Loading DataFrames: CSV method 1
  csv_df =spark.read.csv('/user/cloudera/stackexchange/posts_all_csv')
  csv_df.show(5, False)
  display(csv_df)
  csv_df.printSchema()
  
  # Loading DataFrames: CSV method 2
  csv_df2 = spark.read.format("csv").load("path")
  csv_df2.show(5, False)
  display(csv_df2)
  csv_df2.printSchema()
  
  # Infer Schema
  posts_headersDF = spark.read.option("inferSchema", "true").option('header', True).csv('/user/cloudera/stackexchange/posts_all_csv_with_header')
  posts_headersDF.printSchema()
  posts_headersDF.show(5)

 

  # Correct
  postsSchema = \
    StructType([
                StructField("Id", IntegerType()),
                StructField("PostTypeId", IntegerType()),
                StructField("AcceptedAnswerId", IntegerType()),
                StructField("CreationDate", TimestampType()),
                StructField("Score", IntegerType()),
                StructField("ViewCount", IntegerType()),
                StructField("OwnerUserId", IntegerType()),
                StructField("LastEditorUserId", IntegerType()),
                StructField("LastEditDate", TimestampType()),
                StructField("Title", StringType()),
                StructField("LastActivityDate", TimestampType()),              
                StructField("Tags", StringType()),
                StructField("AnswerCount", IntegerType()),
                StructField("CommentCount", IntegerType()),
                StructField("FavoriteCount", IntegerType())])

  postsDF = spark.read.schema(postsSchema).csv('/user/cloudera/stackexchange/posts_all_csv')
  postsDF.printSchema()
  postsDF.schema
  postsDF.dtypes
  postsDF.columns

    posts_no_schemaTxtDF.show(5, truncate=False)
    posts_no_schemaTxtDF.show(100, truncate=False)
  

if __name__ == "__main__":
  spark = SparkSession.builder.appName("Simple with Session").getOrCreate()
  def readdata(spark)
  
  spark.stop()
