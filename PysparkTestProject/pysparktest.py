from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("project-1").getOrCreate()

# Read CSV file into a DataFrame
csv_file_path = "/Users/hmakhlouf/Desktop/TechCnsltng_WorkSpace/data/project-1-data/car_insurance_claim.csv"
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Perform analytics or transformations on the DataFrame
# For example, let's create a new DataFrame with some transformation
# transformed_df = df.select("column1", "column2", ...)

# Specify the HDFS path to save the result
# HDFS output path on your Hadoop cluster (replace with the actual hostname or IP)
hdfs_output_path = "hdfs://ec2-18-133-73-36.eu-west-2.compute.amazonaws.com:9000//tmp/USUK30/hocine/project-1"

# Save the DataFrame to HDFS
# transformed_df.write.parquet(hdfs_output_path, mode="overwrite")
df.write.parquet(hdfs_output_path, mode="overwrite")
# Stop the Spark session
spark.stop()