
"""
A simple application shows how to work with spark dataframe
Run with spark2-submit simple_with_session.py
Author- Andy
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def readdata():
  # Loading DataFrames: CSV method 1 using spark.read.csv('path')
  player_df =spark.read.option('header', True).csv('gs://dataproc-staging-us-east1-548550014762-rie5an4z/notebooks/jupyter/player.csv')
  player_df.show(5, False)
  display(player_df)
  player_df.printSchema()
  
  # Selecting multiple columns
  df = player_df.select('player_api_id')
  df.show(5,False)
  df = 
  
  
  # Add and update columns
  
  # Rename columns
  
  # Drop columns 
  
  # When and otherwise 
  
  # Where and Filter
  
  # Distinct
  
  # Sort
  
  # Aggregation
  
  # join
  
  # Union
  
  # datefucntion
  
  # string function 
  
  # Windows 
  
  # explode 
  
  # partitioning 
  

 
if __name__ == "__main__":
  spark = SparkSession \
    .builder \
    .appName("Analyzing soccer players") \
    .getOrCreate()
  
  def readdata(spark)
  
  spark.stop()
