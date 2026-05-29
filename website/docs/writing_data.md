---
title: Batch Writes
keywords: [hudi, incremental, batch, processing]
last_modified_at: 2026-05-27T00:00:00-00:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Spark DataSource API

The `hudi-spark` module offers the DataSource API to write a Spark DataFrame into a Hudi table.

There are a number of options available:

**`HoodieWriteConfig`**:

**TABLE_NAME** <br/>


**`DataSourceWriteOptions`**:

**RECORDKEY_FIELD**: Primary key field(s). Record keys uniquely identify a record/row within each partition. If one wants to have a global uniqueness, there are two options. You could either make the dataset non-partitioned, or, you can leverage Global indexes to ensure record keys are unique irrespective of the partition path. Record keys can either be a single column or refer to multiple columns. `KEYGENERATOR_CLASS_OPT_KEY` property should be set accordingly based on whether it is a simple or complex key. For eg: `"col1"` for simple field, `"col1,col2,col3,etc"` for complex field. Nested fields can be specified using the dot notation eg: `a.b.c`. <br/>
Default value: `"uuid"`<br/>

**PARTITIONPATH_FIELD**: Columns to be used for partitioning the table. To prevent partitioning, provide empty string as value eg: `""`. Specify partitioning/no partitioning using `KEYGENERATOR_CLASS_OPT_KEY`. If partition path needs to be url encoded, you can set `URL_ENCODE_PARTITIONING_OPT_KEY`. If synchronizing to hive, also specify using `HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY.`<br/>
Default value: `"partitionpath"`<br/>

**ORDERING_FIELDS**: When two records within the same batch have the same key value, the record with the largest value from the ordering field specified will be chosen. If you are using default payload of OverwriteWithLatestAvroPayload for HoodieRecordPayload (`WRITE_PAYLOAD_CLASS`), an incoming record will always takes precedence compared to the one in storage ignoring this ordering field configuration. <br/>
No default value<br/>
Note: The config key `hoodie.datasource.write.precombine.field` is deprecated, use `hoodie.table.ordering.fields` instead.

**OPERATION**: The [write operations](write_operations.md) to use.<br/>
Available values:<br/>
`"upsert"` (default), `"bulk_insert"`, `"insert"`, `"delete"`

**TABLE_TYPE**: The [type of table](concepts.md#table-types) to write to. Note: After the initial creation of a table, this value must stay consistent when writing to (updating) the table using the Spark `SaveMode.Append` mode.<br/>
Available values:<br/>
[`COW_TABLE_TYPE_OPT_VAL`](concepts.md#copy-on-write-table) (default), [`MOR_TABLE_TYPE_OPT_VAL`](concepts.md#merge-on-read-table)

**KEYGENERATOR_CLASS_NAME**: Refer to [Key Generation](key_generation.md) section below.


Example:
Upsert a DataFrame, specifying the necessary field names for `recordKey => _row_key`, `partitionPath => partition`, and `orderingField => timestamp`

```java
inputDF.write()
       .format("hudi")
       .options(clientOpts) //Where clientOpts is of type Map[String, String]. clientOpts can include any other options necessary.
       .option("hoodie.datasource.write.recordkey.field", "_row_key")
       .option("hoodie.datasource.write.partitionpath.field", "partition")
       .option("hoodie.table.ordering.fields", "timestamp")
       .option("hoodie.table.name", tableName)
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
  option("hoodie.table.ordering.fields", "ts").
  option("hoodie.datasource.write.recordkey.field", "uuid").
  option("hoodie.datasource.write.partitionpath.field", "partitionpath").
  option("hoodie.table.name", tableName).
  mode(Overwrite).
  save(basePath)
```
:::info
`mode(Overwrite)` overwrites and recreates the table if it already exists.
You can check the data generated under `/tmp/hudi_trips_cow/<region>/<country>/<city>/`. We provided a record key
(`uuid` in [schema](https://github.com/apache/hudi/blob/6f9b02decb5bb2b83709b1b6ec04a97e4d102c11/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L60)), partition field (`region/country/city`) and combine logic (`ts` in
[schema](https://github.com/apache/hudi/blob/6f9b02decb5bb2b83709b1b6ec04a97e4d102c11/hudi-spark-datasource/hudi-spark/src/main/java/org/apache/hudi/QuickstartUtils.java#L60)) to ensure trip records are unique within each partition. For more info, refer to
[Modeling data stored in Hudi](/faq/general/#how-do-i-model-the-data-stored-in-hudi)
and for info on ways to ingest data into Hudi, refer to [Writing Hudi Tables](hoodie_streaming_ingestion.md).
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
    'hoodie.table.ordering.fields': 'ts',
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
[Modeling data stored in Hudi](/faq/general/#how-do-i-model-the-data-stored-in-hudi)
and for info on ways to ingest data into Hudi, refer to [Writing Hudi Tables](hoodie_streaming_ingestion.md).
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
  option("hoodie.datasource.write.operation","insert_overwrite_table").
  option("hoodie.table.ordering.fields", "ts").
  option("hoodie.datasource.write.recordkey.field", "uuid").
  option("hoodie.datasource.write.partitionpath.field", "partitionpath").
  option("hoodie.table.name", tableName).
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
  option("hoodie.datasource.write.operation","insert_overwrite").
  option("hoodie.table.ordering.fields", "ts").
  option("hoodie.datasource.write.recordkey.field", "uuid").
  option("hoodie.datasource.write.partitionpath.field", "partitionpath").
  option("hoodie.table.name", tableName).
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
  option("hoodie.datasource.write.operation", "upsert").
  option("hoodie.table.ordering.fields", "ts").
  option("hoodie.datasource.write.recordkey.field", "uuid").
  option("hoodie.datasource.write.partitionpath.field", "partitionpath").
  option("hoodie.table.name", tableName).
  mode(Append).
  save(basePath)
```

- **Hard Deletes** : A stronger form of deletion is to physically remove any trace of the record from the table. This can be achieved in 3 different ways. 

1. Using Datasource, set `"hoodie.datasource.write.operation"` to `"delete"`. This will remove all the records in the DataSet being submitted.

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
option("hoodie.datasource.write.operation","delete").
option("hoodie.table.ordering.fields", "ts").
option("hoodie.datasource.write.recordkey.field", "uuid").
option("hoodie.datasource.write.partitionpath.field", "partitionpath").
option("hoodie.table.name", tableName).
mode(Append).
save(basePath);
```

2. Using DataSource, set `PAYLOAD_CLASS_OPT_KEY` to `"org.apache.hudi.EmptyHoodieRecordPayload"`. This will remove all the records in the DataSet being submitted. 

This example will remove all the records from the table that exist in the DataSet `deleteDF`:
```scala
 deleteDF // dataframe containing just records to be deleted
   .write().format("org.apache.hudi")
   .option(...) // Add HUDI options like record-key, partition-path and others as needed for your setup
   // specify record_key, partition_key, ordering_fields & usual params
   .option(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY, "org.apache.hudi.EmptyHoodieRecordPayload")
```

3. Using DataSource or Hudi Streamer, add a column named `_hoodie_is_deleted` to DataSet. The value of this column must be set to `true` for all the records to be deleted and either `false` or left null for any records which are to be upserted.

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

### Writing VECTOR, BLOB, and VARIANT Columns

The 1.2.0 column types participate in writes the same way as standard SQL types. SQL `INSERT`
examples for all three live in [SQL DML](sql_dml.md#inserting-vector-columns); the DataFrame API
equivalents are below.

#### VECTOR via DataFrame

Stamp `hudi_type` metadata on the VECTOR column so the writer recognizes it:

```python
import pyarrow as pa

schema = pa.schema([
    pa.field("product_id", pa.string()),
    pa.field("embedding",  pa.list_(pa.float32()),
             metadata={b"hudi_type": b"VECTOR(768)"}),
])
```

#### BLOB via DataFrame

A BLOB column is internally a struct (see [BLOB](sql_ddl.md#blob)). Build it as a Spark `Row`:

```python
from pyspark.sql import Row

with open("logo.png", "rb") as f:
    raw_bytes = f.read()

row = Row(
    asset_id="asset_001",
    file_name="logo.png",
    mime_type="image/png",
    file_size=len(raw_bytes),
    content=Row(type="INLINE", data=raw_bytes, reference=None),
)
```

For PyArrow schemas, declare the struct explicitly:

```python
import pyarrow as pa

schema = pa.schema([
    pa.field("asset_id",  pa.string()),
    pa.field("file_name", pa.string()),
    pa.field("mime_type", pa.string()),
    pa.field("file_size", pa.int64()),
    pa.field("content",   pa.struct([
        pa.field("type",      pa.string()),
        pa.field("data",      pa.binary()),
        pa.field("reference", pa.struct([
            pa.field("external_path", pa.string()),
            pa.field("offset",        pa.int64()),
            pa.field("length",        pa.int64()),
            pa.field("managed",       pa.bool_()),
        ])),
    ]), metadata={b"hudi_type": b"BLOB"}),
])
```

#### VARIANT via DataFrame

On Spark 4.0+, use native `VariantType`:

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType, VariantType

schema = StructType([
    StructField("event_id", StringType()),
    StructField("payload",  VariantType()),
    StructField("ts",       LongType()),
])
```

On Spark 3.x (or 4.0+ when interoperating with 3.x), declare the underlying struct and tag it with
`hudi_type=VARIANT` so the writer recognizes it:

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType, BinaryType, MetadataBuilder

variant_metadata = MetadataBuilder().putString("hudi_type", "VARIANT").build()
variant_struct = StructType([
    StructField("metadata", BinaryType()),
    StructField("value",    BinaryType()),
])
schema = StructType([
    StructField("event_id", StringType()),
    StructField("payload",  variant_struct, metadata=variant_metadata),
    StructField("ts",       LongType()),
])
```

The simplest path for constructing VARIANT values from JSON strings is to build the DataFrame via
SQL and then write it:

```python
df = spark.sql("""
    SELECT 'evt_001' AS event_id,
           parse_json('{"action": "click", "x": 120, "y": 450}') AS payload,
           1000 AS ts
""")
df.write.format("hudi") \
    .option("hoodie.table.name", "events") \
    .option("hoodie.datasource.write.recordkey.field", "event_id") \
    .option("hoodie.datasource.write.precombine.field", "ts") \
    .mode("append") \
    .save("/path/to/events")
```

#### Lance base file format via DataFrame

Set `hoodie.table.base.file.format=lance` on the write options:

```python
(df.write
   .format("hudi")
   .option("hoodie.table.name", "my_ai_table")
   .option("hoodie.datasource.write.recordkey.field", "id")
   .option("hoodie.record.merger.impls",
           "org.apache.hudi.DefaultSparkRecordMerger")
   .option("hoodie.table.base.file.format", "lance")
   .mode("overwrite")
   .save("/path/to/my_ai_table"))
```

See [Storage Layouts → Lance](storage_layouts.md#lance-base-file-format) for full Lance behavior
and configs.

### Concurrency Control

Following is an example of how to use `optimistic_concurrency_control` via Spark DataSource API.

Read more in-depth details about concurrency control in the [concurrency control concepts](concurrency_control.md) section.

```java
inputDF.write.format("hudi")
       .options(getQuickstartWriteConfigs)
       .option("hoodie.table.ordering.fields", "ts")
       .option("hoodie.cleaner.policy.failed.writes", "LAZY")
       .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
       .option("hoodie.write.lock.zookeeper.url", "zookeeper")
       .option("hoodie.write.lock.zookeeper.port", "2181")
       .option("hoodie.write.lock.zookeeper.lock_key", "test_table")
       .option("hoodie.write.lock.zookeeper.base_path", "/test")
       .option("hoodie.datasource.write.recordkey.field", "uuid")
       .option("hoodie.datasource.write.partitionpath.field", "partitionpath")
       .option("hoodie.table.name", tableName)
       .mode(Overwrite)
       .save(basePath)
```

### Rolling Extra Metadata

Rolling extra metadata allows you to automatically carry forward selected commit metadata keys to every subsequent commit and clean instant without having to walk the full timeline. This is particularly useful for persisting checkpoint information such as Kafka offsets or Flink checkpoints across commits.

| Config | Default | Description |
|---|---|---|
| `hoodie.write.rolling.metadata.keys` | `""` (disabled) | Comma-separated list of extra metadata keys to carry forward to each new commit and clean instant. Values are read from recent completed instants and written into the new commit metadata, so they remain accessible without walking the timeline. New values override old ones. Only applies to data table commits and clean instants. |
| `hoodie.write.rolling.metadata.timeline.lookback.commits` | `10` | Maximum number of completed instants to walk back when searching for the configured rolling metadata keys. Higher values improve resilience at a small performance cost. |

**Example:**

```java
inputDF.write.format("hudi")
       .option("hoodie.write.rolling.metadata.keys", "kafka.offset.partition.0,kafka.offset.partition.1")
       .option("hoodie.write.rolling.metadata.timeline.lookback.commits", "10")
       // ... other options
       .save(basePath)
```

### Advanced Storage Options

The following advanced storage configuration options were added in Hudi 1.2.0:

| Config | Default | Description |
|---|---|---|
| `hoodie.parquet.write.config.injector.class` | (none) | Fully-qualified class name of a custom `HoodieParquetConfigInjector` implementation. Use this to inject custom Parquet writer properties (e.g., disable dictionary encoding, set bloom filter sizes) without modifying the Hudi source. The implementing class must implement `org.apache.hudi.io.HoodieParquetConfigInjector`. |
| `hoodie.table.base.file.format` | `parquet` | Base file format for the table. Accepts `parquet`, `orc`, `hfile`, or `lance`. See [Storage Layouts → Lance](storage_layouts.md#lance-base-file-format) for the Lance-specific options. |


## Java Client
We can use plain java to write to hudi tables. To use Java client we can refere [here](https://github.com/apache/hudi/blob/master/hudi-examples/hudi-examples-java/src/main/java/org/apache/hudi/examples/java/HoodieJavaWriteClientExample.java)
