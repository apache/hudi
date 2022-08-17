---
title: "Ingest multiple tables using Hudi"
excerpt: "Ingesting multiple tables using Hudi at a single go is now possible. This blog gives a detailed explanation of how to achieve the same using `HoodieMultiTableDeltaStreamer.java`"
author: pratyakshsharma
category: blog
---

When building a change data capture pipeline for already existing or newly created relational databases, one of the most common problems that one faces is simplifying the onboarding process for multiple tables. Ingesting multiple tables to Hudi dataset at a single go is now possible using `HoodieMultiTableDeltaStreamer` class which is a wrapper on top of the more popular `HoodieDeltaStreamer` class. Currently `HoodieMultiTableDeltaStreamer` supports **COPY_ON_WRITE** storage type only and the ingestion is done in a **sequential** way.
<!--truncate-->
This blog will guide you through configuring and running `HoodieMultiTableDeltaStreamer`.

### Configuration

 - `HoodieMultiTableDeltaStreamer` expects users to maintain table wise overridden properties in separate files in a dedicated config folder. Common properties can be configured via common properties file also.
 - By default, hudi datasets are created under the path `<base-path-prefix>/<database_name>/<name_of_table_to_be_ingested>`. You need to provide the names of tables to be ingested via the property `hoodie.deltastreamer.ingestion.tablesToBeIngested` in the format `<database>.<table>`, for example 
 
```java
hoodie.deltastreamer.ingestion.tablesToBeIngested=db1.table1,db2.table2
``` 
 
 - If you do not provide database name, then it is assumed the table belongs to default database and the hudi dataset for the concerned table is created under the path `<base-path-prefix>/default/<name_of_table_to_be_ingested>`. Also there is a provision to override the default path for hudi datasets. You can create hudi dataset for a particular table by setting the property `hoodie.deltastreamer.ingestion.targetBasePath` in table level config file
 - There are a lot of properties that one might like to override per table, for example
 
```java
hoodie.datasource.write.recordkey.field=_row_key
hoodie.datasource.write.partitionpath.field=created_at
hoodie.deltastreamer.source.kafka.topic=topic2
hoodie.deltastreamer.keygen.timebased.timestamp.type=UNIX_TIMESTAMP
hoodie.deltastreamer.keygen.timebased.input.dateformat=yyyy-MM-dd HH:mm:ss.S
hoodie.datasource.hive_sync.table=short_trip_uber_hive_dummy_table
hoodie.deltastreamer.ingestion.targetBasePath=s3:///temp/hudi/table1
```  
 
 - Properties like above need to be set for every table to be ingested. As already suggested at the beginning, users are expected to maintain separate config files for every table by setting the below property
 
```java
hoodie.deltastreamer.ingestion.<db>.<table>.configFile=s3:///tmp/config/config1.properties
``` 

If you do not want to set the above property for every table, you can simply create config files for every table to be ingested under the config folder with the name - `<database>_<table>_config.properties`. For example if you want to ingest table1 and table2 from dummy database, where config folder is set to `s3:///tmp/config`, then you need to create 2 config files on the given paths - `s3:///tmp/config/dummy_table1_config.properties` and `s3:///tmp/config/dummy_table2_config.properties`.

 - Finally you can specify all the common properties in a common properties file. Common properties file does not necessarily have to lie under config folder but it is advised to keep it along with other config files. This file will contain the below properties
 
```java
hoodie.deltastreamer.ingestion.tablesToBeIngested=db1.table1,db2.table2
hoodie.deltastreamer.ingestion.db1.table1.configFile=s3:///tmp/config_table1.properties
hoodie.deltastreamer.ingestion.db2.table2.configFile=s3:///tmp/config_table2.properties
``` 

### Configuring schema providers

It is possible to configure different schema providers for different tables or same schema provider class for all tables. All you need to do is configure the property `hoodie.deltastreamer.schemaprovider.class` accordingly as per your use case as below - 

```java
hoodie.deltastreamer.schemaprovider.class=org.apache.hudi.utilities.schema.FilebasedSchemaProvider
```

Further it is also possible to configure different source and target schema registry urls with `SchemaRegistryProvider` as the schemaprovider class. Originally HoodieMultiTableDeltaStreamer was designed to cater to use cases where subject naming strategy is set to [TopicNameStrategy](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#subject-name-strategy) which is the default provided by Confluent. 
With this default strategy in place, the subject name is same as the topic name being used in kafka. Source and target schema registry urls can be configured as below with TopicNameStrategy - 

```java
hoodie.deltastreamer.schemaprovider.registry.baseUrl=http://localhost:8081/subjects/
hoodie.deltastreamer.schemaprovider.registry.urlSuffix=-value/versions/latest
```

If you want to consume different versions of your source and target subjects, you can configure as below - 

```java
hoodie.deltastreamer.schemaprovider.registry.baseUrl=http://localhost:8081/subjects/
hoodie.deltastreamer.schemaprovider.registry.sourceUrlSuffix=-value/versions/latest
hoodie.deltastreamer.schemaprovider.registry.targetUrlSuffix=-value/versions/1
```

If you are looking to configure the schema registry urls in the most straight forward way, you can do that as below

```java
hoodie.deltastreamer.schemaprovider.registry.url=http://localhost:8081/subjects/random-value/versions/latest
hoodie.deltastreamer.schemaprovider.registry.targetUrl=http://localhost:8081/subjects/random-value/versions/latest
```

### Run Command

`HoodieMultiTableDeltaStreamer` can be run similar to how one runs `HoodieDeltaStreamer`. Please refer to the example given below for the command. 


### Example

Suppose you want to ingest table1 and table2 from db1 and want to ingest the 2 tables under the path `s3:///temp/hudi`. You can ingest them using the below command

```java
[hoodie]$ spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieMultiTableDeltaStreamer `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` \
  --props s3:///temp/hudi-ingestion-config/kafka-source.properties \
  --config-folder s3:///temp/hudi-ingestion-config \
  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
  --source-ordering-field impresssiontime \
  --base-path-prefix s3:///temp/hudi \ 
  --target-table dummy_table \
  --op UPSERT
```

s3:///temp/config/kafka-source.properties

```java
hoodie.deltastreamer.ingestion.tablesToBeIngested=db1.table1,db1.table2
hoodie.deltastreamer.ingestion.db1.table1.configFile=s3:///temp/hudi-ingestion-config/config_table1.properties
hoodie.deltastreamer.ingestion.db21.table2.configFile=s3:///temp/hudi-ingestion-config/config_table2.properties

#Kafka props
bootstrap.servers=localhost:9092
auto.offset.reset=earliest
schema.registry.url=http://localhost:8081

hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.CustomKeyGenerator
```

s3:///temp/hudi-ingestion-config/config_table1.properties

```java
hoodie.datasource.write.recordkey.field=_row_key1
hoodie.datasource.write.partitionpath.field=created_at
hoodie.deltastreamer.source.kafka.topic=topic1
```

s3:///temp/hudi-ingestion-config/config_table2.properties

```java
hoodie.datasource.write.recordkey.field=_row_key2
hoodie.datasource.write.partitionpath.field=created_at
hoodie.deltastreamer.source.kafka.topic=topic2
```

Contributions are welcome for extending multiple tables ingestion support to **MERGE_ON_READ** storage type and enabling `HoodieMultiTableDeltaStreamer` ingest multiple tables parallely. 

Happy ingesting! 