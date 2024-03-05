from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create spark session with hive enabled
spark = SparkSession.builder.appName("carInsuranceClaimsApp").enableHiveSupport().getOrCreate()
# .config("spark.jars", "/Users/hmakhlouf/Desktop/TechCnsltng_WorkSpace/config/postgresql-42.7.2.jar") \

# 1- Establish the connection to PostgresSQL and Redshift:

# PostgresSQL connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver",
}
postgres_table_name = "car_insurance_claims"

# hive database and table names
hive_database_name = "project1db"
hive_table_name = "carinsuranceclaims"


# 2. Load new data dataset from PostgresSQL:
new_data = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
new_data.show(3)

# 3. Load the existing_data in hive table
existing_hive_data = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))

# existing_hive_data = spark.read.table("project1db.carinsuranceclaims")

existing_hive_data.show(3)


# 4. Determine the incremental data

incremental_data = new_data.join(existing_hive_data.select("id"), new_data["id"] == existing_hive_data["id"], "left_anti")

incremental_data.show()
#incremental_data = new_data.filter(~col("id").isin(existing_hive_data.select("id")))



# 5.  Adding the new records to the existing hive table
new_hive_table = existing_hive_data.union(incremental_data)
new_hive_table.show(3)

# Show the new  records in hive table 
newDataHive_df = spark.sql("SELECT * FROM project1db.carinsuranceclaims cic WHERE cic.ID = 1 OR cic.ID = 2")
newDataHive_df.show()

# 6 . Writing the updated DataFrame back to the Hive table
new_hive_table.write.mode("overwrite").saveAsTable("your_existing_table")


# Stop Spark session
spark.stop()



