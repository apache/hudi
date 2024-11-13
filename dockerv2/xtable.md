## Using Apache xtable with Hudi Docker Demo

### Run Apache xtable

```
docker exec -it openjdk11 /bin/bash

# Run Apache xtable
export AWS_SECRET_ACCESS_KEY=password
export AWS_ACCESS_KEY_ID=admin
export ENDPOINT=http://minio:9000
export AWS_REGION=us-east-1
cd /opt/xtable/jars/; java -jar /opt/incubator-xtable/xtable-utilities/target/xtable-utilities-0.2.0-SNAPSHOT-bundled.jar --datasetConfig xtable_hudi.yaml -p core-site.xml
```
Check out both xml.  One xml is related to xtable config.  The other xml is used to configure access to S3A.

### Run Hudi Sync to HMS

This was already done in the demo.

### Run spark-sql with Delta Lake libraries to register Delta Lake files into Apache Hive MetaStore (HMS)

```
docker exec -it spark /bin/bash

spark-sql --packages com.amazonaws:aws-java-sdk-s3:1.11.271,org.apache.hadoop:hadoop-aws:2.10.2,io.delta:delta-core_2.12:2.4.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--conf "spark.sql.catalogImplementation=hive" \
--conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
```
Run this when you get the spark-sql prompt
```
CREATE SCHEMA delta_db LOCATION 's3a://warehouse/';

CREATE TABLE delta_db.stock_ticks_cow USING DELTA LOCATION 's3a://warehouse/stock_ticks_cow';
```

###  Run spark-sql with Apache Iceberg libraries to register Apache Iceberg files into Apache Hive MetaStore (HMS)

```
docker exec -it spark /bin/bash

spark-sql --packages com.amazonaws:aws-java-sdk-s3:1.11.271,org.apache.hadoop:hadoop-aws:2.10.2,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.2 \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
--conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog" \
--conf "spark.sql.catalog.spark_catalog.type=hive" \
--conf "spark.sql.catalog.hive_prod=org.apache.iceberg.spark.SparkCatalog" \
--conf "spark.sql.catalog.hive_prod.type=hive" \
--conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
```
Run this when you get the spark-sql prompt
```
CREATE SCHEMA iceberg_db;

CALL hive_prod.system.register_table(
   table => 'hive_prod.iceberg_db.stock_ticks_cow',
   metadata_file => 's3a://warehouse/stock_ticks_cow/metadata/v2.metadata.json'
);
```

### Access the Trino container and configure Trino to understand S3

Configure Trino to understand S3

```
docker exec -it trino /usr/bin/bash

cat /etc/trino/catalog/hudi.properties
cat /etc/trino/catalog/iceberg.properties
cat /etc/trino/catalog/delta.properties
```

The most important item is adding the "hive.s3" configurations.
```
hive.s3.aws-access-key=admin
hive.s3.aws-secret-key=password
hive.s3.endpoint=http://minio:9000
```

### Accessing Services with Trino

You can access the local Trino container by running 

```
docker exec -it trino trino
```

Run the following SQL to view the data.

```
select * from hudi.hudi_db.people;
select * from delta.delta_db.stock_ticks_cow;
select * from iceberg.iceberg_db.stock_ticks_cow;
```

### Debug

```
trino> show schemas in hudi;
       Schema       
--------------------
 default            
 information_schema 
 sales_default      
(3 rows)

Query 20240815_003243_00020_93hak, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0.58 [3 rows, 53B] [5 rows/s, 91B/s]

trino> show tables in hudi.sales_default;
        Table        
---------------------
 public_books_ro     
 public_books_rt     
 public_customers_ro 
 public_customers_rt 
 public_toys_ro      
 public_toys_rt      
 public_users_ro     
 public_users_rt     
(8 rows)

Query 20240815_003315_00021_93hak, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0.50 [8 rows, 310B] [16 rows/s, 624B/s]
```
