from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("ParallelizeData").getOrCreate()

# Create a list of data
data = [1, 2, 3, 4, 5]

# Create an RDD from the list
distData = spark.sparkContext.parallelize(data)

# Print the RDD
print(distData.collect())

# Stop SparkSession
spark.stop()
