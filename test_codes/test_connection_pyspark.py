import os
import sys

# CRITICAL: Tell PySpark to use the correct Python
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Set these BEFORE importing pyspark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[*] pyspark-shell'
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Remove problematic Java options
if 'JAVA_TOOL_OPTIONS' in os.environ:
    del os.environ['JAVA_TOOL_OPTIONS']
if '_JAVA_OPTIONS' in os.environ:
    del os.environ['_JAVA_OPTIONS']

import pyspark
from pyspark.sql import SparkSession

print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")
print(f"PySpark version: {pyspark.__version__}")

spark = SparkSession.builder \
    .appName("MongoDBConnection") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()
        
spark.sparkContext.setLogLevel("ERROR")

# Create test data - all floats
data = [("Java", 17.0), ("Spark", 3.5), ("Python", 3.11)]
columns = ["Technology", "Version"]
df = spark.createDataFrame(data, columns)
df.show()

spark.stop()
print("Test completed successfully!")