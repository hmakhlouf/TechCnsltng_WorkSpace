from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("PostgresToHive").getOrCreate()

# PostgreSQL connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver",
}

# PostgreSQL table name
postgres_table_name = "car_insurance_claims"


# Read data from PostgreSQL
df_postgres = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)

# Hive database and table names
hive_database_name = "project1db"
hive_table_name = "carInsuranceClaims"

# Save data to Hive
#df_postgres.write.mode("overwrite").saveAsTable(f"{hive_database_name}.{hive_table_name}")
df_postgres.write.mode("overwrite").saveAsTable("{}.{}".format(hive_database_name, hive_table_name))


# Stop Spark session
spark.stop()
