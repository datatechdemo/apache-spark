"""
Shows a simple application being executed in spark without a session
Run with spark2-submit simple_with_session.py
Auther- Andy
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Simple with Session").getOrCreate()

print "*** Testing simple_with_session.py ***"
print "*** Application name: " + spark.sparkContext.appName + " / Version: " + spark.version + " ***"

spark.stop()
