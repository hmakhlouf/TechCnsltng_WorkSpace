### Sqoop 
⇒ qoop is a tool used for transferring data between Hadoop hdfs file system and structured data sources such as relational databases.

#### From Dbeaver side : 
- create a table 

```
CREATE TABLE hocine_us_states (
  StateID SERIAL PRIMARY KEY,
   State VARCHAR(255),
   City VARCHAR(255),
   ZipCode VARCHAR(10)
);
```

- insert data to the table 
```
INSERT INTO hocine_us_states (State, City, ZipCode)
VALUES
('Alabama', 'Birmingham', '35203'),
('Alabama', 'Huntsville', '35801'),
('Alabama', 'Montgomery', '36104'),
('Wyoming', 'Cheyenne', '82001'),
('Wyoming', 'Casper', '82601');
```

```
select * from hocine_us_states hus ;
```

- From terminal 

1.  connect to ec2 using ssh, hdfs by runing these two commands :

- CD to path/to/test_key.pem

```
cd Desktop/TechCnsltng_WorkSpace/
```

- connect to ec2 using ssh

```
ssh -i "test_key.pem" ec2-user@ec2-18-133-73-36.eu-west-2.compute.amazonaws.com
```



- Then follow these  ⇒ steps 

- 1.  
```
sqoop list-databases --connect jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb --username consultants -P
```
- 2.  
sqoop list-tables --connect jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb --username consultants -P

- 3.  
```
sqoop import --connect jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb --username consultants --password WelcomeItc@2022 --table hocine_us_states --m 1 --target-dir /tmp/USUK30/hocine/sqoopdata
```
- 4. 
```
hdfs dfs -ls /tmp/USUK30/hocine/sqoopdata
```
- 5. 
```
hdfs dfs -cat /tmp/USUK30/hocine/sqoopdata/part-m-00000
```
![Alt Text](/sqoop/sqoop_data.png)




- 6. commands to create a table inside hive 

- 1. 

```
gohive
```
- 2. From hive commande line : 

- greate external table with the same schema

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
- 7. show table 

![Alt Text](/sqoop/us_states_cities_zipcodes_external.png)

- 8.   

```
sqoop import --connect jdbc:postgresql://ec2-13-40-49-105.eu-west-2.compute.amazonaws.com:5432/testdb --username consultants --password WelcomeItc@2022  --table hocine_us_states  --target-dir /warehouse/tablespace/managed/hive/ofc5 --delete-target-dir --fields-terminated-by ","  --hive-import --create-hive-table --hive-table ofc5 -m 1 --hs2-url "jdbc:hive2://ip-172-31-3-80.eu-west-2.compute.internal:10000/default;"
```


sqoop import --connect jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb --username consultants --password WelcomeItc@2022  --table keith_sq  --target-dir /warehouse/tablespace/managed/hive/keith_test --delete-target-dir --fields-terminated-by ","  --hive-import --create-hive-table --hive-table keith_test -m 1 --hs2-url "jdbc:hive2://ip-172-31-3-80.eu-west-2.compute.internal:10000/default;"

