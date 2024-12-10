# Running a Local Demo
This is a modified demo of Docker Demo.

Use `./start_demo.sh` to spin up a local notebook with a scala interpreter, Hive Metastore, Presto and Trino in docker containers. The script will first build the XTable jars required for the demo and then start the containers.  

## Setup Instructions
1. Build the spark image
`docker build -t atwong/openjdk-11-spark-3.4-hive-2.3.10-hadoop-2.10.2:0.1.1 docker-image/openjdk-11-spark-3.4-hive-2.3.10-hadoop-2.10.2/.`
The versions were picked because these are the versions that are used to compile Apache Hudi 0.14.1. See https://github.com/apache/hudi/blob/release-0.14.1/pom.xml for more details.

2. Start the demo
`./start_demo.sh`
You can see that in the docker-compose file, it will have Min.IO as your object store, scripts that will create the inital s3 bucket (s3://warehouse), Apache Hive Metastore, and the just built Spark Container.

3. Access the spark container and create the open table format data in S3
`docker exec -it spark /usr/bin/bash`

3.1 Create the initial Apache Hudi data
```
pyspark --packages com.amazonaws:aws-java-sdk-s3:1.11.271,org.apache.hadoop:hadoop-client:2.10.2,org.apache.hadoop:hadoop-aws:2.10.2,org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1   --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer"   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog"   --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
```
What is important to note is that the hadoop client has to match the hadoop version in your spark container (eg. org.apache.hadoop:hadoop-client:2.10.2 means hadoop 2.10.2), Hudi spark bundle has to match (eg. org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1 means spark 3.4 with Hudi 0.14.1) and the AWS java SDK has to match the one that is compiled against hadoop-client (you can see the compile dependancy at https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.10.2).

And this is the pyspark script
```
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
import os

# initialize the bucket
database_name = "hudi_db"
table_name = "people"
base_path = "s3a://warehouse/"

records = [
   (1, 'John', 25, 'NYC', '2023-09-28 00:00:00'),
   (2, 'Emily', 30, 'SFO', '2023-09-28 00:00:00'),
   (3, 'Michael', 35, 'ORD', '2023-09-28 00:00:00'),
   (4, 'Andrew', 40, 'NYC', '2023-10-28 00:00:00'),
   (5, 'Bob', 28, 'SEA', '2023-09-23 00:00:00'),
   (6, 'Charlie', 31, 'DFW', '2023-08-29 00:00:00')
]

schema = StructType([
   StructField("id", IntegerType(), True),
   StructField("name", StringType(), True),
   StructField("age", IntegerType(), True),
   StructField("city", StringType(), True),
   StructField("create_ts", StringType(), True)
])

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(records, schema)

hudi_options = {
#   'hoodie.datasource.hive_sync.enable': 'true',
#   'hoodie.datasource.hive_sync.mode': 'hms',
#   'hoodie.datasource.hive_sync.database': database_name,
#   'hoodie.datasource.hive_sync.table': table_name,
#   'hoodie.datasource.hive_sync.metastore.uris': 'thrift://hive-metastore:9083',
   'hoodie.table.name': table_name,
   'hoodie.datasource.write.partitionpath.field': 'city',
   'hoodie.datasource.write.hive_style_partitioning': 'true'

}

(
   df.write
   .format("hudi")
   .options(**hudi_options)
   .mode('overwrite')
   .save(f"{base_path}/{table_name}")
)
```
Note that I commented out the extra statements need to sync Hudi to HMS.   If you uncomment these lines you don't need to run the `hudi-hive-sync/run_sync_tool.sh` tool.  For this demo, we will assume that we'll write the hudi files into S3 and will need to use `hudi-hive-sync/run_sync_tool.sh` later.

You can browse the data you've written by going to http://localhost:9000 using admin/password.

3.2 Use xtable to scan the Apache Hudi data and create Iceberg and Delta data
```
export AWS_SECRET_ACCESS_KEY=password
export AWS_ACCESS_KEY_ID=admin
export ENDPOINT=http://minio:9000
export AWS_REGION=us-east-1
cd /opt/xtable/jars/; java -jar xtable-utilities-0.1.0-SNAPSHOT-bundled.jar --datasetConfig xtable_hudi.yaml -p core-site.xml
```
Check out both xml.  One xml is related to xtable config.  The other xml is used to configure access to S3A.

4. Access the spark container and sync the data in S3 to HMS
`docker exec -it spark /usr/bin/bash`

4.1 Download compiled hudi-hive-sync-bundle
```
pyspark --packages org.apache.hudi:hudi-hive-sync-bundle:0.14.1,com.amazonaws:aws-java-sdk-s3:1.11.271,org.apache.hadoop:hadoop-client:2.10.2,org.apache.hadoop:hadoop-aws:2.10.2
```
I'm using pyspark to download hudi hive sync libraries to ~/.ivy2/jars

4.2 Download missing packages to get hudi-hive-sync/run_sync_tool.sh to work
```
pyspark --packages org.apache.parquet:parquet-avro:1.10.1,com.esotericsoftware:kryo-shaded:4.0.2
```
I'm using pyspark to download missing hudi hive sync libraries to ~/.ivy2/jars

4.3 Modify run_sync_tool.sh to add Apache Ivy libraries (via pyspark) to classpath
```
vi /opt/hudi/hudi-sync/hudi-hive-sync/run_sync_tool.sh
```
Change the line from
```
java -cp $HUDI_HIVE_UBER_JAR:${HADOOP_HIVE_JARS}:${HADOOP_CONF_DIR} org.apache.hudi.hive.HiveSyncTool "$@"
```
to 
```
java -cp ~/.ivy2/jars/*:$HUDI_HIVE_UBER_JAR:${HADOOP_HIVE_JARS}:${HADOOP_CONF_DIR} org.apache.hudi.hive.HiveSyncTool "$@"
```

4.4 Add spark settings to access S3
```
vi /hadoop/etc/hadoop/core-site.xml
```
It should look like this
```
root@spark:/opt/xtable/jars# cat /hadoop/etc/hadoop/core-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>s3a://minio:9000</value>
    </property>


    <!-- Minio properties -->
    <property>
        <name>fs.s3a.connection.ssl.enabled</name>
        <value>false</value>
    </property>

    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://minio:9000</value>
    </property>

    <property>
        <name>fs.s3a.access.key</name>
        <value>admin</value>
    </property>

    <property>
        <name>fs.s3a.secret.key</name>
        <value>password</value>
    </property>

    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>

    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
</configuration>
```

4.5 Run hudi-hive-sync/run_sync_tool.sh to register Apache Hudi files into Apache Hive MetaStore
```
cd /opt/hudi/hudi-sync/hudi-hive-sync
./run_sync_tool.sh  \
--metastore-uris 'thrift://hive-metastore:9083' \
--partitioned-by city \
--base-path 's3a://warehouse/people' \
--database hudi_db \
--table people \
--sync-mode hms
```

4.6 Run spark-sql with Delta Lake libraries to register Delta Lake files into Apache Hive MetaStore (HMS)
```
spark-sql --packages com.amazonaws:aws-java-sdk-s3:1.11.271,org.apache.hadoop:hadoop-aws:2.10.2,io.delta:delta-core_2.12:2.4.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--conf "spark.sql.catalogImplementation=hive" \
--conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
```
Run this when you get the spark-sql prompt
```
CREATE SCHEMA delta_db LOCATION 's3a://warehouse/';

CREATE TABLE delta_db.people USING DELTA LOCATION 's3a://warehouse/people';
```

4.7 Run spark-sql with Apache Iceberg libraries to register Apache Iceberg files into Apache Hive MetaStore (HMS)
```
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
   table => 'hive_prod.iceberg_db.people',
   metadata_file => 's3a://warehouse/people/metadata/v2.metadata.json'
);
```

5. Access the Trino container and configure Trino to understand S3
`docker exec -it trino /usr/bin/bash`

5.1 Configure Trino to understand S3
```
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

## Accessing Services
### Trino
You can access the local Trino container by running `docker exec -it trino trino`

Run the following SQL to view the data.
```
select * from hudi.hudi_db.people;
select * from delta.delta_db.people;
select * from iceberg.iceberg_db.people;
```
### Presto
You can access the local Presto container by running `docker exec -it presto presto-cli --server localhost:8082`

## Debug

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
