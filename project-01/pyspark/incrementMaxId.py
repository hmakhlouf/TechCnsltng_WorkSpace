from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()
max_id = spark.sql("SELECT max(id) FROM product.emp_info")
m_id = max_id.collect()[0][0]

query = 'SELECT * FROM emp_info WHERE ID > ' + str(m_id)

more_data = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .option("query", query) \
    .load()

more_data.write.mode("append").saveAsTable("product.emp_info")



## 2. load df_postgres to hive table
# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS hocinedb")

# Hive database and table names
hive_database_name = "hocinedbdb"
hive_table_name = "carInsuranceClaims"
