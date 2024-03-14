from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()
max_id = spark.sql("SELECT max(policy_number) FROM hocinedb.carinsuranceclaims")
m_id = max_id.collect()[0][0]

query = 'SELECT * FROM car_insurance_claims WHERE ID > ' + str(m_id)

more_data = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .option("query", query) \
    .load()

more_data.write.mode("append").saveAsTable("hocinedb.carinsuranceclaims")


# Stop Spark session
spark.stop()
