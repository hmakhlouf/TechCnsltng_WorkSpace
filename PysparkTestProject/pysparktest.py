from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CarInsuranceClaimsApp") \
    #.config("spark.jars", "/Users/hmakhlouf/Desktop/TechCnsltng_WorkSpace/config/postgresql-42.7.2.jar") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://ec2-18-133-73-36.eu-west-2.compute.amazonaws.com:9000") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
    .getOrCreate()


# PostgreSQL's connection details
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {"user": "consultants", "password": "WelcomeItc@2022", "driver": "org.postgresql.Driver"}

# HDFS output path for full load and incremental load
full_load_output_path = "hdfs://ec2-18-133-73-36.eu-west-2.compute.amazonaws.com:9000/tmp/USUK30/hocine/project-1/full_load_data"


#incremental_load_output_path = "hdfs://ec2-18-133-73-36.eu-west-2.compute.amazonaws.com:9000/tmp/USUK30/hocine/project-1/incremental_load_data"

# Table name in PostgreSQL
table_name = "car_insurance_claims"

# Read data from PostgreSQL
df_postgres = spark.read.jdbc(url=postgres_url, table=table_name, properties=postgres_properties)

# Perform transformations (add your transformations here)
#df_transformed = df_postgres.withColumn("new_column", col("existing_column") * 2)

# Write transformed data to HDFS

# df_transformed.write.mode("overwrite").parquet(full_load_output_path)

## Full Load data to hdfs
df_postgres.write.mode("overwrite")  #.parquet(full_load_output_path)

# Incremental Load: Append transformed data to existing data in HDFS
# df_transformed.write.mode("append").parquet(incremental_load_output_path)
