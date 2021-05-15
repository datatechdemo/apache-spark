
"""
A simple application shows how to load(or read) data into spark dataframe
Run with spark2-submit simple_with_session.py
Author- Andy
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def readdata():
  # Create a dataframe manually, from a list
  df = spark.createDataFrame([(1,'Andy'),(2,'mandy'),(3,'sandy')])
  print(type(df))
  df.show()
  df.printSchema()
  
  # Working with data frame 
  # Select columns And we can return the values in those columns
  postsDF.select(postsDF.Title).show(1)
  postsDF.select(postsDF['Title']).show(1)
  postsDF.select('Title').show()
  
  # Selecting multiple columns
  
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
