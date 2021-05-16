"""
Description about the program
Author - Andy 
"""

# A good programmer alway put comments 

#Import libraries
from pyspark.sql import SparkSession
# Other libraries

def function_example(spark):
# code


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Application name") \
        .getOrCreate()

    function_example(spark):

    spark.stop()
