---
title: Writing Data
keywords: [hudi, incremental, batch, stream, processing, Hive, ETL, Spark SQL]
summary: In this page, we will discuss some available tools for incrementally ingesting & storing data.
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

In this section, we will cover ways to ingest new changes from external sources or even other Hudi tables.
Currently Hudi supports following ways to write the data.
- [Hudi Streamer](hoodie_streaming_ingestion.md#hudi-streamer)
- [Spark Hudi Datasource](#spark-datasource-writer)
- [Spark Structured Streaming](hoodie_streaming_ingestion.md#structured-streaming)
- [Spark SQL](sql_ddl.md#spark-sql)
- [Flink Writer](hoodie_streaming_ingestion.md#flink-ingestion)
- [Flink SQL](sql_ddl.md#flink)
- [Java Writer](#java-writer)
- [Kafka Connect](hoodie_streaming_ingestion.md#kafka-connect-sink)

## Spark Datasource Writer

The `hudi-spark` module offers the DataSource API to write (and read) a Spark DataFrame into a Hudi table. There are a number of options available:

**`HoodieWriteConfig`**:

**TABLE_NAME** (Required)<br/>


**`DataSourceWriteOptions`**:

**RECORDKEY_FIELD_OPT_KEY** (Required): Primary key field(s). Record keys uniquely identify a record/row within each partition. If one wants to have a global uniqueness, there are two options. You could either make the dataset non-partitioned, or, you can leverage Global indexes to ensure record keys are unique irrespective of the partition path. Record keys can either be a single column or refer to multiple columns. `KEYGENERATOR_CLASS_OPT_KEY` property should be set accordingly based on whether it is a simple or complex key. For eg: `"col1"` for simple field, `"col1,col2,col3,etc"` for complex field. Nested fields can be specified using the dot notation eg: `a.b.c`. <br/>
Default value: `"uuid"`<br/>

**PARTITIONPATH_FIELD_OPT_KEY** (Required): Columns to be used for partitioning the table. To prevent partitioning, provide empty string as value eg: `""`. Specify partitioning/no partitioning using `KEYGENERATOR_CLASS_OPT_KEY`. If partition path needs to be url encoded, you can set `URL_ENCODE_PARTITIONING_OPT_KEY`. If synchronizing to hive, also specify using `HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY.`<br/>
Default value: `"partitionpath"`<br/>

**PRECOMBINE_FIELD_OPT_KEY** (Required): When two records within the same batch have the same key value, the record with the largest value from the field specified will be choosen. If you are using default payload of OverwriteWithLatestAvroPayload for HoodieRecordPayload (`WRITE_PAYLOAD_CLASS`), an incoming record will always takes precendence compared to the one in storage ignoring this `PRECOMBINE_FIELD_OPT_KEY`. <br/>
Default value: `"ts"`<br/>

**OPERATION_OPT_KEY**: The [write operations](write_operations.md) to use.<br/>
Available values:<br/>
`UPSERT_OPERATION_OPT_VAL` (default), `BULK_INSERT_OPERATION_OPT_VAL`, `INSERT_OPERATION_OPT_VAL`, `DELETE_OPERATION_OPT_VAL`

**TABLE_TYPE_OPT_KEY**: The [type of table](concepts.md#table-types) to write to. Note: After the initial creation of a table, this value must stay consistent when writing to (updating) the table using the Spark `SaveMode.Append` mode.<br/>
Available values:<br/>
[`COW_TABLE_TYPE_OPT_VAL`](concepts.md#copy-on-write-table) (default), [`MOR_TABLE_TYPE_OPT_VAL`](concepts.md#merge-on-read-table)

**KEYGENERATOR_CLASS_OPT_KEY**: Refer to [Key Generation](key_generation.md) section below.

**HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY**: If using hive, specify if the table should or should not be partitioned.<br/>
Available values:<br/>
`classOf[MultiPartKeysValueExtractor].getCanonicalName` (default), `classOf[SlashEncodedDayPartitionValueExtractor].getCanonicalName`, `classOf[TimestampBasedKeyGenerator].getCanonicalName`, `classOf[NonPartitionedExtractor].getCanonicalName`, `classOf[GlobalDeleteKeyGenerator].getCanonicalName` (to be used when `OPERATION_OPT_KEY` is set to `DELETE_OPERATION_OPT_VAL`)


Example:
Upsert a DataFrame, specifying the necessary field names for `recordKey => _row_key`, `partitionPath => partition`, and `precombineKey => timestamp`

```java
inputDF.write()
       .format("org.apache.hudi")
       .options(clientOpts) //Where clientOpts is of type Map[String, String]. clientOpts can include any other options necessary.
       .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
       .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
       .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
       .option(HoodieWriteConfig.TABLE_NAME, tableName)
       .mode(SaveMode.Append)
       .save(basePath);
```

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Python', value: 'python', },
{ label: 'SparkSQL', value: 'sparksql', },
]}>
<TabItem value="scala">

Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi table as below.

```scala
// spark-shell
val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Overwrite).
  save(basePath)
```
:::info
`mode(Overwrite)` overwrites and recreates the table if it already exists.
You can check the data generated under `/tmp/hudi_trips_cow/<region>/<country>/<city>/`. We provided a record key
(`uuid` in [schema](https://github.com/apache/hudi/blob/6f9b02decb5bb2b83709b1b6ec04a97e4d102c11/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L60)), partition field (`region/country/city`) and combine logic (`ts` in
[schema](https://github.com/apache/hudi/blob/6f9b02decb5bb2b83709b1b6ec04a97e4d102c11/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L60)) to ensure trip records are unique within each partition. For more info, refer to
[Modeling data stored in Hudi](faq_general/#how-do-i-model-the-data-stored-in-hudi)
and for info on ways to ingest data into Hudi, refer to [Writing Hudi Tables](writing_data.md).
Here we are using the default write operation : `upsert`. If you have a workload without updates, you can also issue
`insert` or `bulk_insert` operations which could be faster. To know more, refer to [Write operations](write_operations.md)
:::
</TabItem>

<TabItem value="python">
Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi table as below.

```python
# pyspark
inserts = sc._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(dataGen.generateInserts(10))
df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

hudi_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.table.name': tableName,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

df.write.format("hudi").
    options(**hudi_options).
    mode("overwrite").
    save(basePath)
```
:::info
`mode(Overwrite)` overwrites and recreates the table if it already exists.
You can check the data generated under `/tmp/hudi_trips_cow/<region>/<country>/<city>/`. We provided a record key
(`uuid` in [schema](https://github.com/apache/hudi/blob/2e6e302efec2fa848ded4f88a95540ad2adb7798/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L60)), partition field (`region/country/city`) and combine logic (`ts` in
[schema](https://github.com/apache/hudi/blob/2e6e302efec2fa848ded4f88a95540ad2adb7798/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L60)) to ensure trip records are unique within each partition. For more info, refer to
[Modeling data stored in Hudi](faq_general/#how-do-i-model-the-data-stored-in-hudi)
and for info on ways to ingest data into Hudi, refer to [Writing Hudi Tables](writing_data.md).
Here we are using the default write operation : `upsert`. If you have a workload without updates, you can also issue
`insert` or `bulk_insert` operations which could be faster. To know more, refer to [Write operations](write_operations.md)
:::
</TabItem>

<TabItem value="sparksql">

```sql
insert into h0 select 1, 'a1', 20;

-- insert static partition
insert into h_p0 partition(dt = '2021-01-02') select 1, 'a1';

-- insert dynamic partition
insert into h_p0 select 1, 'a1', dt;

-- insert dynamic partition
insert into h_p1 select 1 as id, 'a1', '2021-01-03' as dt, '19' as hh;

-- insert overwrite table
insert overwrite table h0 select 1, 'a1', 20;

-- insert overwrite table with static partition
insert overwrite h_p0 partition(dt = '2021-01-02') select 1, 'a1';

-- insert overwrite table with dynamic partition
  insert overwrite table h_p1 select 2 as id, 'a2', '2021-01-03' as dt, '19' as hh;
```

**NOTICE**

- Insert mode : Hudi supports two insert modes when inserting data to a table with primary key(we call it pk-table as followed):<br/>
  Using `strict` mode, insert statement will keep the primary key uniqueness constraint for COW table which do not allow
  duplicate records. If a record already exists during insert, a HoodieDuplicateKeyException will be thrown
  for COW table. For MOR table, updates are allowed to existing record.<br/>
  Using `non-strict` mode, hudi uses the same code path used by `insert` operation in spark data source for the pk-table. <br/>
  One can set the insert mode by using the config: **hoodie.sql.insert.mode**

- Bulk Insert : By default, hudi uses the normal insert operation for insert statements. Users can set **hoodie.sql.bulk.insert.enable**
  to true to enable the bulk insert for insert statement.

</TabItem>
</Tabs>


Checkout https://hudi.apache.org/blog/2021/02/13/hudi-key-generators for various key generator options, like Timestamp based,
complex, custom, NonPartitioned Key gen, etc.


### Insert Overwrite Table

Generate some new trips, overwrite the table logically at the Hudi metadata level. The Hudi cleaner will eventually
clean up the previous table snapshot's file groups. This can be faster than deleting the older table and recreating
in `Overwrite` mode.

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'SparkSQL', value: 'sparksql', },
]}>
<TabItem value="scala">

```scala
// spark-shell
spark.
  read.format("hudi").
  load(basePath).
  select("uuid","partitionpath").
  show(10, false)

val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(OPERATION_OPT_KEY,"insert_overwrite_table").
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)

// Should have different keys now, from query before.
spark.
  read.format("hudi").
  load(basePath).
  select("uuid","partitionpath").
  show(10, false)

``` 
</TabItem>

<TabItem value="sparksql">

The insert overwrite non-partitioned table sql statement will convert to the ***insert_overwrite_table*** operation.
e.g.
```sql
insert overwrite table h0 select 1, 'a1', 20;
```
</TabItem>
</Tabs>

### Insert Overwrite

Generate some new trips, overwrite the all the partitions that are present in the input. This operation can be faster
than `upsert` for batch ETL jobs, that are recomputing entire target partitions at once (as opposed to incrementally
updating the target tables). This is because, we are able to bypass indexing, precombining and other repartitioning
steps in the upsert write path completely.

<Tabs
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'SparkSQL', value: 'sparksql', },
]}>
<TabItem value="scala">

```scala
// spark-shell
spark.
  read.format("hudi").
  load(basePath).
  select("uuid","partitionpath").
  sort("partitionpath","uuid").
  show(100, false)

val inserts = convertToStringList(dataGen.generateInserts(10))
val df = spark.
  read.json(spark.sparkContext.parallelize(inserts, 2)).
  filter("partitionpath = 'americas/united_states/san_francisco'")
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(OPERATION_OPT_KEY,"insert_overwrite").
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)

// Should have different keys now for San Francisco alone, from query before.
spark.
  read.format("hudi").
  load(basePath).
  select("uuid","partitionpath").
  sort("partitionpath","uuid").
  show(100, false)
```
</TabItem>

<TabItem value="sparksql">

The insert overwrite partitioned table sql statement will convert to the ***insert_overwrite*** operation.
e.g.
```sql
insert overwrite table h_p1 select 2 as id, 'a2', '2021-01-03' as dt, '19' as hh;
```
</TabItem>
</Tabs>

### Deletes

Hudi supports implementing two types of deletes on data stored in Hudi tables, by enabling the user to specify a different record payload implementation.
For more info refer to [Delete support in Hudi](https://cwiki.apache.org/confluence/x/6IqvC).

- **Soft Deletes** : Retain the record key and just null out the values for all the other fields.
  This can be achieved by ensuring the appropriate fields are nullable in the table schema and simply upserting the table after setting these fields to null.
  Note that soft deletes are always persisted in storage and never removed, but all values are set to nulls. 
  So for GDPR or other compliance reasons, users should consider doing hard deletes if record key and partition path
  contain PII.

For example:
```scala
// fetch two records for soft deletes
val softDeleteDs = spark.sql("select * from hudi_trips_snapshot").limit(2)

// prepare the soft deletes by ensuring the appropriate fields are nullified
val nullifyColumns = softDeleteDs.schema.fields.
  map(field => (field.name, field.dataType.typeName)).
  filter(pair => (!HoodieRecord.HOODIE_META_COLUMNS.contains(pair._1)
    && !Array("ts", "uuid", "partitionpath").contains(pair._1)))

val softDeleteDf = nullifyColumns.
  foldLeft(softDeleteDs.drop(HoodieRecord.HOODIE_META_COLUMNS: _*))(
    (ds, col) => ds.withColumn(col._1, lit(null).cast(col._2)))

// simply upsert the table after setting these fields to null
softDeleteDf.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(OPERATION_OPT_KEY, "upsert").
  option(PRECOMBINE_FIELD_OPT_KEY, "ts").
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
  option(TABLE_NAME, tableName).
  mode(Append).
  save(basePath)
```

- **Hard Deletes** : A stronger form of deletion is to physically remove any trace of the record from the table. This can be achieved in 3 different ways. 

1. Using Datasource, set `OPERATION_OPT_KEY` to `DELETE_OPERATION_OPT_VAL`. This will remove all the records in the DataSet being submitted.

Example, first read in a dataset:
```scala
val roViewDF = spark.
        read.
        format("org.apache.hudi").
        load(basePath + "/*/*/*/*")
roViewDF.createOrReplaceTempView("hudi_ro_table")
spark.sql("select count(*) from hudi_ro_table").show() // should return 10 (number of records inserted above)
val riderValue = spark.sql("select distinct rider from hudi_ro_table").show()
// copy the value displayed to be used in next step
```
Now write a query of which records you would like to delete:
```scala
val df = spark.sql("select uuid, partitionPath from hudi_ro_table where rider = 'rider-213'")
```
Lastly, execute the deletion of these records:
```scala
val deletes = dataGen.generateDeletes(df.collectAsList())
val df = spark.read.json(spark.sparkContext.parallelize(deletes, 2));
df.write.format("org.apache.hudi").
options(getQuickstartWriteConfigs).
option(OPERATION_OPT_KEY,"delete").
option(PRECOMBINE_FIELD_OPT_KEY, "ts").
option(RECORDKEY_FIELD_OPT_KEY, "uuid").
option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
option(TABLE_NAME, tableName).
mode(Append).
save(basePath);
```

2. Using DataSource, set `PAYLOAD_CLASS_OPT_KEY` to `"org.apache.hudi.EmptyHoodieRecordPayload"`. This will remove all the records in the DataSet being submitted. 

This example will remove all the records from the table that exist in the DataSet `deleteDF`:
```scala
 deleteDF // dataframe containing just records to be deleted
   .write().format("org.apache.hudi")
   .option(...) // Add HUDI options like record-key, partition-path and others as needed for your setup
   // specify record_key, partition_key, precombine_fieldkey & usual params
   .option(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY, "org.apache.hudi.EmptyHoodieRecordPayload")
```

3. Using DataSource or DeltaStreamer, add a column named `_hoodie_is_deleted` to DataSet. The value of this column must be set to `true` for all the records to be deleted and either `false` or left null for any records which are to be upserted.

Let's say the original schema is:
```json
{
  "type":"record",
  "name":"example_tbl",
  "fields":[{
     "name": "uuid",
     "type": "String"
  }, {
     "name": "ts",
     "type": "string"
  },  {
     "name": "partitionPath",
     "type": "string"
  }, {
     "name": "rank",
     "type": "long"
  }
]}
```
Make sure you add `_hoodie_is_deleted` column:
```json
{
  "type":"record",
  "name":"example_tbl",
  "fields":[{
     "name": "uuid",
     "type": "String"
  }, {
     "name": "ts",
     "type": "string"
  },  {
     "name": "partitionPath",
     "type": "string"
  }, {
     "name": "rank",
     "type": "long"
  }, {
    "name" : "_hoodie_is_deleted",
    "type" : "boolean",
    "default" : false
  }
]}
```

Then any record you want to delete you can mark `_hoodie_is_deleted` as true:
```json
{"ts": 0.0, "uuid": "19tdb048-c93e-4532-adf9-f61ce6afe10", "rank": 1045, "partitionpath": "americas/brazil/sao_paulo", "_hoodie_is_deleted" : true}
```

### Concurrency Control

The `hudi-spark` module offers the DataSource API to write (and read) a Spark DataFrame into a Hudi table.

Following is an example of how to use optimistic_concurrency_control via spark datasource. Read more in depth about concurrency control in the [concurrency control concepts](https://hudi.apache.org/docs/concurrency_control) section  

```java
inputDF.write.format("hudi")
       .options(getQuickstartWriteConfigs)
       .option(PRECOMBINE_FIELD_OPT_KEY, "ts")
       .option("hoodie.cleaner.policy.failed.writes", "LAZY")
       .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
       .option("hoodie.write.lock.zookeeper.url", "zookeeper")
       .option("hoodie.write.lock.zookeeper.port", "2181")
       .option("hoodie.write.lock.zookeeper.lock_key", "test_table")
       .option("hoodie.write.lock.zookeeper.base_path", "/test")
       .option(RECORDKEY_FIELD_OPT_KEY, "uuid")
       .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
       .option(TABLE_NAME, tableName)
       .mode(Overwrite)
       .save(basePath)
```

### Commit Notifications
Apache Hudi provides the ability to post a callback notification about a write commit. This may be valuable if you need
an event notification stream to take actions with other services after a Hudi write commit.
You can push a write commit callback notification into HTTP endpoints or to a Kafka server.

#### HTTP Endpoints
You can push a commit notification to an HTTP URL and can specify custom values by extending a callback class defined below.

|  Config  | Description | Required | Default |
|  -----------  | -------  | ------- | ------ |
| TURN_CALLBACK_ON | Turn commit callback on/off | optional | false (*callbacks off*) |
| CALLBACK_HTTP_URL | Callback host to be sent along with callback messages | required | N/A |
| CALLBACK_HTTP_TIMEOUT_IN_SECONDS | Callback timeout in seconds | optional | 3 |
| CALLBACK_CLASS_NAME | Full path of callback class and must be a subclass of HoodieWriteCommitCallback class, org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback by default | optional | org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback |
| CALLBACK_HTTP_API_KEY_VALUE | Http callback API key | optional | hudi_write_commit_http_callback |
| | | | |

#### Kafka Endpoints
You can push a commit notification to a Kafka topic so it can be used by other real time systems.

|  Config  | Description | Required | Default |
|  -----------  | -------  | ------- | ------ |
| TOPIC | Kafka topic name to publish timeline activity into. | required | N/A |
| PARTITION | It may be desirable to serialize all changes into a single Kafka partition for providing strict ordering. By default, Kafka messages are keyed by table name, which guarantees ordering at the table level, but not globally (or when new partitions are added) | required | N/A |
| RETRIES | Times to retry the produce | optional | 3 |
| ACKS | kafka acks level, all by default to ensure strong durability | optional | all |
| BOOTSTRAP_SERVERS | Bootstrap servers of kafka cluster, to be used for publishing commit metadata | required | N/A |
| | | | |

#### Pulsar Endpoints
You can push a commit notification to a Pulsar topic so it can be used by other real time systems. 

|  Config  | Description                                                                 | Required | Default |
|  -----------  |-----------------------------------------------------------------------------| ------- |--------|
| hoodie.write.commit.callback.pulsar.broker.service.url | Server's Url of pulsar cluster to use to publish commit metadata.   | required | N/A    |
| hoodie.write.commit.callback.pulsar.topic | Pulsar topic name to publish timeline activity into | required | N/A    |
| hoodie.write.commit.callback.pulsar.producer.route-mode | Message routing logic for producers on partitioned topics.  | optional |   RoundRobinPartition     |
| hoodie.write.commit.callback.pulsar.producer.pending-queue-size | The maximum size of a queue holding pending messages.               | optional | 1000   |
| hoodie.write.commit.callback.pulsar.producer.pending-total-size | The maximum number of pending messages across partitions. | required | 50000  |
| hoodie.write.commit.callback.pulsar.producer.block-if-queue-full | When the queue is full, the method is blocked instead of an exception is thrown. | optional | true   |
| hoodie.write.commit.callback.pulsar.producer.send-timeout | The timeout in each sending to pulsar.     | optional | 30s    |
| hoodie.write.commit.callback.pulsar.operation-timeout | Duration of waiting for completing an operation.     | optional | 30s    |
| hoodie.write.commit.callback.pulsar.connection-timeout | Duration of waiting for a connection to a broker to be established.     | optional | 10s    |
| hoodie.write.commit.callback.pulsar.request-timeout | Duration of waiting for completing a request.  | optional | 60s    |
| hoodie.write.commit.callback.pulsar.keepalive-interval | Duration of keeping alive interval for each client broker connection. | optional | 30s    |
| |                                                                             | |        |

#### Bring your own implementation
You can extend the HoodieWriteCommitCallback class to implement your own way to asynchronously handle the callback
of a successful write. Use this public API:

https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/callback/HoodieWriteCommitCallback.java

## Flink SQL Writer
The hudi-flink module defines the Flink SQL connector for both hudi source and sink.
There are a number of options available for the sink table:

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| path | Y | N/A | Base path for the target hoodie table. The path would be created if it does not exist, otherwise a hudi table expects to be initialized successfully |
| table.type  | N | COPY_ON_WRITE | Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ |
| write.operation | N | upsert | The write operation, that this write should do (insert or upsert is supported) |
| write.precombine.field | N | ts | Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..) |
| write.payload.class | N | OverwriteWithLatestAvroPayload.class | Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for the option in-effective |
| write.insert.drop.duplicates | N | false | Flag to indicate whether to drop duplicates upon insert. By default insert will accept duplicates, to gain extra performance |
| write.ignore.failed | N | true | Flag to indicate whether to ignore any non exception error (e.g. writestatus error). within a checkpoint batch. By default true (in favor of streaming progressing over data integrity) |
| hoodie.datasource.write.recordkey.field | N | uuid | Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using the dot notation eg: `a.b.c` |
| hoodie.datasource.write.keygenerator.class | N | SimpleAvroKeyGenerator.class | Key generator class, that implements will extract the key out of incoming record |
| write.tasks | N | 4 | Parallelism of tasks that do actual write, default is 4 |
| write.batch.size.MB | N | 128 | Batch buffer size in MB to flush data into the underneath filesystem |

If the table type is MERGE_ON_READ, you can also specify the asynchronous compaction strategy through options:

|  Option Name  | Required | Default | Remarks |
|  -----------  | -------  | ------- | ------- |
| compaction.async.enabled | N | true | Async Compaction, enabled by default for MOR |
| compaction.trigger.strategy | N | num_commits | Strategy to trigger compaction, options are 'num_commits': trigger compaction when reach N delta commits; 'time_elapsed': trigger compaction when time elapsed > N seconds since last compaction; 'num_and_time': trigger compaction when both NUM_COMMITS and TIME_ELAPSED are satisfied; 'num_or_time': trigger compaction when NUM_COMMITS or TIME_ELAPSED is satisfied. Default is 'num_commits' |
| compaction.delta_commits | N | 5 | Max delta commits needed to trigger compaction, default 5 commits |
| compaction.delta_seconds | N | 3600 | Max delta seconds time needed to trigger compaction, default 1 hour |

You can write the data using the SQL `INSERT INTO` statements:
```sql
INSERT INTO hudi_table select ... from ...; 
```

**Note**: INSERT OVERWRITE is not supported yet but already on the roadmap.


### Non-Blocking Concurrency Control (Experimental)

Hudi Flink supports a new non-blocking concurrency control mode, where multiple writer tasks can be executed
concurrently without blocking each other. One can read more about this mode in
the [concurrency control](concurrency_control.md#model-c-multi-writer) docs. Let us see it in action here.

In the below example, we have two streaming ingestion pipelines that concurrently update the same table. One of the
pipeline is responsible for the compaction and cleaning table services, while the other pipeline is just for data
ingestion.

```sql
-- set the interval as 30 seconds
execution.checkpointing.interval: 30000
state.backend: rocksdb

-- This is a datagen source that can generates records continuously
CREATE TABLE sourceT (
    uuid varchar(20),
    name varchar(10),
    age int,
    ts timestamp(3),
    `partition` as 'par1'
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '200'
);

-- pipeline1: by default enable the compaction and cleaning services
CREATE TABLE t1(
    uuid varchar(20),
    name varchar(10),
    age int,
    ts timestamp(3),
    `partition` varchar(20)
) WITH (
    'connector' = 'hudi',
    'path' = '/Users/chenyuzhao/workspace/hudi-demo/t1',
    'table.type' = 'MERGE_ON_READ',
    'index.type' = 'BUCKET',
    'hoodie.write.concurrency.mode' = 'NON_BLOCKING_CONCURRENCY_CONTROL',
    'write.tasks' = '2'
);

-- pipeline2: disable the compaction and cleaning services manually
CREATE TABLE t1_2(
    uuid varchar(20),
    name varchar(10),
    age int,
    ts timestamp(3),
    `partition` varchar(20)
) WITH (
    'connector' = 'hudi',
    'path' = '/Users/chenyuzhao/workspace/hudi-demo/t1',
    'table.type' = 'MERGE_ON_READ',
    'index.type' = 'BUCKET',
    'hoodie.write.concurrency.mode' = 'NON_BLOCKING_CONCURRENCY_CONTROL',
    'write.tasks' = '2',
    'compaction.schedule.enabled' = 'false',
    'compaction.async.enabled' = 'false',
    'clean.async.enabled' = 'false'
);

-- submit the pipelines
insert into t1 select * from sourceT;
insert into t1_2 select * from sourceT;

select * from t1 limit 20;
```

As you can see from the above example, we have two pipelines with multiple tasks that concurrently write to the
same table. To use the new concurrency mode, all you need to do is set the `hoodie.write.concurrency.mode`
to `NON_BLOCKING_CONCURRENCY_CONTROL`. The `write.tasks` option is used to specify the number of write tasks that will
be used for writing to the table. The `compaction.schedule.enabled`, `compaction.async.enabled`
and `clean.async.enabled` options are used to disable the compaction and cleaning services for the second pipeline.
This is done to ensure that the compaction and cleaning services are not executed twice for the same table.


## Java Writer
We can use plain java to write to hudi tables. To use Java client we can refere [here](https://github.com/apache/hudi/blob/master/hudi-examples/hudi-examples-java/src/main/java/org/apache/hudi/examples/java/HoodieJavaWriteClientExample.java)

