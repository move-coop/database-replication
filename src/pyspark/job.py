### The following hello world job was copied & pasted from Google Search AI ###

from pyspark.sql import SparkSession

# Create a SparkSession
# The .appName() method sets a name for your application, visible in the Spark UI.
# .master("local[*]") configures Spark to run locally using all available cores.
# .getOrCreate() retrieves an existing SparkSession or creates a new one if none exists.
spark = SparkSession.builder \
    .appName("HelloWorldPySpark") \
    .master("local[*]") \
    .getOrCreate()

# Perform a simple operation: print "Hello, Spark!"
print("Hello, Spark!")

# Create a simple RDD (Resilient Distributed Dataset) and perform an action
data = ["Hello", "World", "PySpark", "Job"]
rdd = spark.sparkContext.parallelize(data)

# Count the number of elements in the RDD and print it
print(f"Number of elements in RDD: {rdd.count()}")

# Stop the SparkSession
spark.stop()