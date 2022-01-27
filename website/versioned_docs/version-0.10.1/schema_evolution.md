---
title: Schema Evolution
keywords: [hudi, incremental, batch, stream, processing, schema, evolution]
summary: In this page, we will discuss schema evolution support in Hudi.
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

Schema evolution is a very important aspect of data management.
Hudi supports common schema evolution scenarios, such as adding a nullable field or promoting a datatype of a field, out-of-the-box.
Furthermore, the evolved schema is queryable across engines, such as Presto, Hive and Spark SQL.
The following table presents a summary of the types of schema changes compatible with different Hudi table types.

|  Schema Change  | COW | MOR | Remarks |
|  -----------  | -------  | ------- | ------- |
| Add a new nullable column at root level at the end | Yes | Yes | `Yes` means that a write with evolved schema succeeds and a read following the write succeeds to read entire dataset. |
| Add a new nullable column to inner struct (at the end) | Yes | Yes |
| Add a new complex type field with default (map and array) | Yes | Yes |  |
| Add a new nullable column and change the ordering of fields | No | No | Write succeeds but read fails if the write with evolved schema updated only some of the base files but not all. Currently, Hudi does not maintain a schema registry with history of changes across base files. Nevertheless, if the upsert touched all base files then the read will succeed. |
| Add a custom nullable Hudi meta column, e.g. `_hoodie_meta_col` | Yes | Yes |  |
| Promote datatype from `int` to `long` for a field at root level | Yes | Yes | For other types, Hudi supports promotion as specified in [Avro schema resolution](http://avro.apache.org/docs/current/spec#Schema+Resolution). |
| Promote datatype from `int` to `long` for a nested field | Yes | Yes |
| Promote datatype from `int` to `long` for a complex type (value of map or array) | Yes | Yes |  |
| Add a new non-nullable column at root level at the end | No | No | In case of MOR table with Spark data source, write succeeds but read fails. As a **workaround**, you can make the field nullable. |
| Add a new non-nullable column to inner struct (at the end) | No | No |  |
| Change datatype from `long` to `int` for a nested field | No | No |  |
| Change datatype from `long` to `int` for a complex type (value of map or array) | No | No |  |

Let us walk through an example to demonstrate the schema evolution support in Hudi.
In the below example, we are going to add a new string field and change the datatype of a field from int to long.

```java
Welcome to
    ____              __
    / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
    /___/ .__/\_,_/_/ /_/\_\   version 3.1.2
    /_/

    Using Scala version 2.12.10 (OpenJDK 64-Bit Server VM, Java 1.8.0_292)
    Type in expressions to have them evaluated.
    Type :help for more information.

scala> import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.QuickstartUtils._

scala> import scala.collection.JavaConversions._
import scala.collection.JavaConversions._

scala> import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SaveMode._

scala> import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceReadOptions._

scala> import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.DataSourceWriteOptions._

scala> import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.config.HoodieWriteConfig._

scala> import org.apache.spark.sql.types._
import org.apache.spark.sql.types._

scala> import org.apache.spark.sql.Row
import org.apache.spark.sql.Row

scala> val tableName = "hudi_trips_cow"
    tableName: String = hudi_trips_cow
scala> val basePath = "file:///tmp/hudi_trips_cow"
    basePath: String = file:///tmp/hudi_trips_cow
scala> val schema = StructType( Array(
    | StructField("rowId", StringType,true),
    | StructField("partitionId", StringType,true),
    | StructField("preComb", LongType,true),
    | StructField("name", StringType,true),
    | StructField("versionId", StringType,true),
    | StructField("intToLong", IntegerType,true)
    | ))
    schema: org.apache.spark.sql.types.StructType = StructType(StructField(rowId,StringType,true), StructField(partitionId,StringType,true), StructField(preComb,LongType,true), StructField(name,StringType,true), StructField(versionId,StringType,true), StructField(intToLong,IntegerType,true))
    
scala> val data1 = Seq(Row("row_1", "part_0", 0L, "bob", "v_0", 0),
    |                Row("row_2", "part_0", 0L, "john", "v_0", 0),
    |                Row("row_3", "part_0", 0L, "tom", "v_0", 0))
    data1: Seq[org.apache.spark.sql.Row] = List([row_1,part_0,0,bob,v_0,0], [row_2,part_0,0,john,v_0,0], [row_3,part_0,0,tom,v_0,0])

scala> var dfFromData1 = spark.createDataFrame(data1, schema)
scala> dfFromData1.write.format("hudi").
    |   options(getQuickstartWriteConfigs).
    |   option(PRECOMBINE_FIELD_OPT_KEY.key, "preComb").
    |   option(RECORDKEY_FIELD_OPT_KEY.key, "rowId").
    |   option(PARTITIONPATH_FIELD_OPT_KEY.key, "partitionId").
    |   option("hoodie.index.type","SIMPLE").
    |   option(TABLE_NAME.key, tableName).
    |   mode(Overwrite).
    |   save(basePath)

scala> var tripsSnapshotDF1 = spark.read.format("hudi").load(basePath + "/*/*")
    tripsSnapshotDF1: org.apache.spark.sql.DataFrame = [_hoodie_commit_time: string, _hoodie_commit_seqno: string ... 9 more fields]

scala> tripsSnapshotDF1.createOrReplaceTempView("hudi_trips_snapshot")

scala> spark.sql("desc hudi_trips_snapshot").show()
    +--------------------+---------+-------+
    |            col_name|data_type|comment|
    +--------------------+---------+-------+
    | _hoodie_commit_time|   string|   null|
    |_hoodie_commit_seqno|   string|   null|
    |  _hoodie_record_key|   string|   null|
    |_hoodie_partition...|   string|   null|
    |   _hoodie_file_name|   string|   null|
    |               rowId|   string|   null|
    |         partitionId|   string|   null|
    |             preComb|   bigint|   null|
    |                name|   string|   null|
    |           versionId|   string|   null|
    |           intToLong|      int|   null|
    +--------------------+---------+-------+
    
scala> spark.sql("select rowId, partitionId, preComb, name, versionId, intToLong from hudi_trips_snapshot").show()
    +-----+-----------+-------+----+---------+---------+
    |rowId|partitionId|preComb|name|versionId|intToLong|
    +-----+-----------+-------+----+---------+---------+
    |row_3|     part_0|      0| tom|      v_0|        0|
    |row_2|     part_0|      0|john|      v_0|        0|
    |row_1|     part_0|      0| bob|      v_0|        0|
    +-----+-----------+-------+----+---------+---------+

// In the new schema, we are going to add a String field and 
// change the datatype `intToLong` field from  int to long.
scala> val newSchema = StructType( Array(
    | StructField("rowId", StringType,true),
    | StructField("partitionId", StringType,true),
    | StructField("preComb", LongType,true),
    | StructField("name", StringType,true),
    | StructField("versionId", StringType,true),
    | StructField("intToLong", LongType,true),
    | StructField("newField", StringType,true)
    | ))
    newSchema: org.apache.spark.sql.types.StructType = StructType(StructField(rowId,StringType,true), StructField(partitionId,StringType,true), StructField(preComb,LongType,true), StructField(name,StringType,true), StructField(versionId,StringType,true), StructField(intToLong,LongType,true), StructField(newField,StringType,true))

scala> val data2 = Seq(Row("row_2", "part_0", 5L, "john", "v_3", 3L, "newField_1"),
    |                Row("row_5", "part_0", 5L, "maroon", "v_2", 2L, "newField_1"),
    |                Row("row_9", "part_0", 5L, "michael", "v_2", 2L, "newField_1"))
    data2: Seq[org.apache.spark.sql.Row] = List([row_2,part_0,5,john,v_3,3,newField_1], [row_5,part_0,5,maroon,v_2,2,newField_1], [row_9,part_0,5,michael,v_2,2,newField_1])

scala> var dfFromData2 = spark.createDataFrame(data2, newSchema)
scala> dfFromData2.write.format("hudi").
    |   options(getQuickstartWriteConfigs).
    |   option(PRECOMBINE_FIELD_OPT_KEY.key, "preComb").
    |   option(RECORDKEY_FIELD_OPT_KEY.key, "rowId").
    |   option(PARTITIONPATH_FIELD_OPT_KEY.key, "partitionId").
    |   option("hoodie.index.type","SIMPLE").
    |   option(TABLE_NAME.key, tableName).
    |   mode(Append).
    |   save(basePath)

scala> var tripsSnapshotDF2 = spark.read.format("hudi").load(basePath + "/*/*")
    tripsSnapshotDF2: org.apache.spark.sql.DataFrame = [_hoodie_commit_time: string, _hoodie_commit_seqno: string ... 10 more fields]

scala> tripsSnapshotDF2.createOrReplaceTempView("hudi_trips_snapshot")

scala> spark.sql("desc hudi_trips_snapshot").show()
    +--------------------+---------+-------+
    |            col_name|data_type|comment|
    +--------------------+---------+-------+
    | _hoodie_commit_time|   string|   null|
    |_hoodie_commit_seqno|   string|   null|
    |  _hoodie_record_key|   string|   null|
    |_hoodie_partition...|   string|   null|
    |   _hoodie_file_name|   string|   null|
    |               rowId|   string|   null|
    |         partitionId|   string|   null|
    |             preComb|   bigint|   null|
    |                name|   string|   null|
    |           versionId|   string|   null|
    |           intToLong|   bigint|   null|
    |            newField|   string|   null|
    +--------------------+---------+-------+


scala> spark.sql("select rowId, partitionId, preComb, name, versionId, intToLong, newField from hudi_trips_snapshot").show()
    +-----+-----------+-------+-------+---------+---------+----------+
    |rowId|partitionId|preComb|   name|versionId|intToLong|  newField|
    +-----+-----------+-------+-------+---------+---------+----------+
    |row_3|     part_0|      0|    tom|      v_0|        0|      null|
    |row_2|     part_0|      5|   john|      v_3|        3|newField_1|
    |row_1|     part_0|      0|    bob|      v_0|        0|      null|
    |row_5|     part_0|      5| maroon|      v_2|        2|newField_1|
    |row_9|     part_0|      5|michael|      v_2|        2|newField_1|
    +-----+-----------+-------+-------+---------+---------+----------+

```
