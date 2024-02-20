### Sqoop 
⇒ qoop is a tool used for transferring data between Hadoop hdfs file system and structured data sources such as relational databases.
------------

#### PostgreSQL Table Creation and Data Insertion
1. Create a Table in PostgreSQL using DBeaver:

```
CREATE TABLE hocine_us_states (
  StateID SERIAL PRIMARY KEY,
   State VARCHAR(255),
   City VARCHAR(255),
   ZipCode VARCHAR(10)
);
```

2. Insert Data into the PostgreSQL Table:
```
INSERT INTO hocine_us_states (State, City, ZipCode)
VALUES
('Alabama', 'Birmingham', '35203'),
('Alabama', 'Huntsville', '35801'),
('Alabama', 'Montgomery', '36104'),
('Wyoming', 'Cheyenne', '82001'),
('Wyoming', 'Casper', '82601');
```
3. Verify Data in PostgreSQL Table:

```
select * from hocine_us_states hus ;
```
![Alt Text](/sqoop/png/db.png)

------------

### Sqoop Commands for Data Transfer
4. Connect to EC2 and Run Sqoop Commands:

- CD to path/to/test_key.pem

```
cd Desktop/TechCnsltng_WorkSpace/
```

- connect to ec2 using ssh

```
ssh -i "test_key.pem" ec2-user@ec2-18-133-73-36.eu-west-2.compute.amazonaws.com
```

5. Sqoop Commands:

- 1.  List existing Databases:
```
sqoop list-databases \
--connect jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb \
--username consultants -P

```
- 2. List existing Tables:
``` 
sqoop list-tables \
--connect jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb \
--username consultants -P

```
- 3.  Import Data from PostgreSQL to HDFS:
```
sqoop import \
--connect jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb \
--username consultants \
--password WelcomeItc@2022 \
--table hocine_us_states \
--m 1 \
--target-dir /tmp/USUK30/hocine/sqoopdata

```
-  Check HDFS Directory:
```
hdfs dfs -ls /tmp/USUK30/hocine/sqoopdata
```
-  View Imported Data:
```
hdfs dfs -cat /tmp/USUK30/hocine/sqoopdata/part-m-00000
```
![Alt Text](/sqoop/png/sqoop_data.png)

------------

### Hive Table Creation and Data Import

6. Connect to Hive:

```
gohive
```
7. Create External Table in Hive:

```
CREATE EXTERNAL TABLE us_states_cities_zipcodes_external (
    StateID INT,
    State STRING,
    City STRING,
    ZipCode STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tmp/USUK30/hocine/datasq';
```
8. Verify Hive External Table:

![Alt Text](/sqoop/png/us_states_cities_zipcodes_external.png)

------------
9. Import Data from PostgreSQL to Hive:


```
sqoop import 
sqoop import \
--connect jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb \
--username consultants \
--password WelcomeItc@2022 \
--table hocine_us_states \
--target-dir /warehouse/tablespace/managed/hive/us_states_cities_zipcodes_external \
--delete-target-dir \
--fields-terminated-by "," \
--hive-import \
--create-hive-table \
--hive-table us_states_cities_zipcodes_external \
-m 1 \
--hs2-url "jdbc:hive2://ip-172-31-3-80.eu-west-2.compute.internal:10000/default"
```
