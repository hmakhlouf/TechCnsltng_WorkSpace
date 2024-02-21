----------------------------
----------------------------

## Task Summary
1. Create PostgreSQL Table:

. Create a table named your_table_name in PostgreSQL.

2. Insert Records with Primary Key:

. Add 10 records to the table with a primary key constraint.

3. Sqoop Import to HDFS:

. Use Sqoop to import data from PostgreSQL to HDFS.
Create an external Hive table on top of the imported data.

4. Hive Shell Script to Find Max ID:

. Write a Hive shell script to find the maximum ID from the external table.

5. Pass Max ID to Sqoop Condition:

. Utilize the max ID obtained from the Hive script in Sqoop conditions.

6. Reload New Data to HDFS:

. Load the newly conditioned data back to HDFS using Sqoop.

7. Add Additional Records to PostgreSQL:

. Insert 10 more records to the PostgreSQL table.

8. Select Records Based on Condition:

. Retrieve records from the PostgreSQL table where ID is greater than the previously identified max ID.

-----------------------------
-----------------------------





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

## Sqoop Commands for Data Transfer
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

- Import Data from PostgreSQL to HDFS:
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

## Hive Table Creation and Data Import

6. Connect to Hive:

```
gohive
```
7. Create External Table in Hive:

```
CREATE EXTERNAL TABLE us_states_exformaxid (
    StateID INT,
    State STRING,
    City STRING,
    ZipCode STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tmp/USUK30/hocine/sqoopdata/';
```


8. Verify Hive External Table:

```
select * from us_states_exformaxid;
```

![Alt Text](/sqoop/png/external_table_for_maxid.png)


- Keep Data Synchronized:

If the PostgreSQL data is expected to change over time, you may want to periodically update the HDFS directory by re-running Sqoop or using a scheduled process. This ensures that your external table in Hive stays synchronized with the latest data from PostgreSQL.

### in the case of re-running sqoop follow these steps by creating scripts 

1. Create the Sqoop Import Script:

Open your preferred text editor and create a new file, for example, sqoop_import.sh. Add the following content:

```
# remove existing directry then replace it

hdfs dfs -rm -r /tmp/USUK30/hocine/sqoopdata

#!/bin/bash

# Sqoop Import
sqoop import \
  --connect jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb \
  --username consultants \
  --password WelcomeItc@2022 \
  --table hocine_us_states \
  --m 1 \
  --target-dir /tmp/USUK30/hocine/sqoopdata

```

2. Create the Hive External Table Script:

Open another file, for example, hive_external_table.sql. Add the following content:

```
-- hive_external_table.sql

-- Assuming StateID is an INTEGER in PostgreSQL
CREATE EXTERNAL TABLE IF NOT EXISTS us_states_exformaxid (
  StateID INT,
  State STRING,
  City STRING,
  ZipCode STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tmp/USUK30/hocine/sqoopdata';

```
3. Grant Execute Permission (if needed):

```
chmod +x sqoop_import.sh

```
4. Run the Scripts:

Execute the Sqoop script to import data:
```
./sqoop_import.sh
```

Execute the Hive script to create or update the external table:
```
hive -f hive_external_table.sql
```

5. Verify Hive External Table:

```
select * from us_states_exformaxid;
```

![Alt Text](/sqoop/png/new_external_table_for_maxid.png)

