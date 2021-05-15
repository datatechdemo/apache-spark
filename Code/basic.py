
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
  player_df.select('id','player_api_id').show(5,False)
  
  
  # Add and update columns # notes Spark withColumn() is a DataFrame function that is used to add a new column to DataFrame, change the value of an existing column, convert the datatype of a column, derive a new column from an existing column
  
  player_df.withColumn("country", lit("USA"))
  player_df.withColumn("height", col('height')/2.54)
  
  # Rename columns #notes Spark has a withColumnRenamed() function on DataFrame to change a column name. This is the most straight forward approach; this function takes two parameters; the first is your existing column name and the second is the new column name you wish for.
  player_df.withColumnRenamed('height', 'height_in_cms')
  
  # Drop columns 
  
  player_df = player_df.drop('id', 'player_fifa_api_id')
  player_df.columns
  
  player_attributes = player_attributes.drop(
    'id', 
    'player_fifa_api_id', 
    'preferred_foot',
    'attacking_work_rate',
    'defensive_work_rate',
    'crossing',
    'jumping',
    'sprint_speed',
    'balance',
    'aggression',
    'short_passing',
    'potential'
)
player_attributes.columns
  
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
