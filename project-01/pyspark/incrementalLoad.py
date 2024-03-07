from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create spark session with hive enabled
spark = SparkSession.builder.appName("carInsuranceClaimsApp").enableHiveSupport().getOrCreate()
# .config("spark.jars", "/Users/hmakhlouf/Desktop/TechCnsltng_WorkSpace/config/postgresql-42.7.2.jar") \

# 1- Establish the connection to PostgresSQL and hive:

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


# 2. read & show new dataset from PostgresSQL:
postgres_df = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
postgres_df.show(3)


#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
#-+-+--+-+--+-+--+-+--+-+-Transformations-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--
#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-

# Rename column from "ID" to "policy_number"
postgres_df = postgres_df.withColumnRenamed("ID", "policy_number")
postgres_df.show(3)

# Use Spark SQL to rename the column in Hive
spark.sql("USE hive_database_name")  # Replace with your Hive database name
spark.sql("ALTER TABLE your_hive_table_name CHANGE COLUMN ID policy_number INT")

#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-



# 3. read and show the existing_data in hive table
existing_hive_data = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))    #table("project1db.carinsuranceclaims")
existing_hive_data.show(3)

# 4. Determine the incremental data
print('---------------------------------------------------------')
print('------------------Incremental data-----------------------')
print('---------------------------------------------------------')
incremental_data_df = postgres_df.join(existing_hive_data.select("id"), postgres_df["id"] == existing_hive_data["id"], "left_anti")
incremental_data_df.show()


# counting the number of the new records added to postgres tables
print('---------------------------------------------------------')
print('------------------COUNTING INCREMENT RECORDS ------------')
print('---------------------------------------------------------')
new_records = incremental_data_df.count()
print('new records added count', new_records)

# 5.  Adding the incremental_data DataFrame to the existing hive table
# Check if there are extra rows in PostgresSQL. if exist => # write & append to the Hive table
if incremental_data_df.count() > 0:
    # Append new rows to Hive table
    incremental_data_df.write.mode("append").insertInto("{}.{}".format(hive_database_name, hive_table_name))
    print("Appended {} new records to Hive table.".format(incremental_data_df.count()))
else:
    print("No new records been inserted in PostgreSQL table.")

# 6. Show the new  records in hive table
newDataHive_df = spark.sql("SELECT * FROM project1db.carinsuranceclaims cic WHERE cic.ID = 1 OR cic.ID = 2")
newDataHive_df.show()


# 7. Stop Spark session
spark.stop()



