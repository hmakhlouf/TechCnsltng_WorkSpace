
from os.path import abspath
from pyspark.sql import SparkSession


# Create spark session with hive enabled
spark = SparkSession.builder \
    .appName("carInsuranceClaimsApp") \
    .enableHiveSupport() \
    .getOrCreate()
#.config("spark.jars", "/Users/hmakhlouf/Desktop/TechCnsltng_WorkSpace/config/postgresql-42.7.2.jar") \



# PostgreSQL connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver",
}
postgres_table_name = "car_insurance_claims"


# Read data from PostgreSQL
df_postgres = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
df_postgres.show()



# full load data to hive

# Create database
#spark.sql("CREATE DATABASE IF NOT EXISTS project1db")

# Hive database and table names
hive_database_name = project1db
hive_table_name = carInsuranceClaims

# Create Hive Internal table
df_postgres.write.mode('overwrite').saveAsTable("{}.{}".format(hive_database_name, hive_table_name))

#.saveAsTable("project1db.carInsuranceClaims")

# Read Hive table
df = spark.read.table("project1db.carInsuranceClaims")
df.show()

# Stop Spark session
spark.stop()


# Save data to Hive
#df_postgres.write.mode("overwrite").saveAsTable("{}.{}".format(hive_database_name, hive_table_name))