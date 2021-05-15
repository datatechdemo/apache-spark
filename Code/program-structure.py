"""
Description about the program
Author - Andy 
"""

# A good programmer alway put comments 

#Import libraries
from pyspark.sql import SparkSession
#Other libraries

def function_example(spark):
  # +--------+---+---+---+
    # |    time| id| v1| v2|
    # +--------+---+---+---+
    # |20000101|  1|1.0|  x|
    # |20000102|  1|3.0|  x|
    # |20000101|  2|2.0|  y|
    # |20000102|  2|4.0|  y|
    # +--------+---+---+---+


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Application name") \
        .getOrCreate()

    function_example(spark):

    spark.stop()
