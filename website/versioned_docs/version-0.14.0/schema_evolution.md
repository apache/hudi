---
title: Schema Evolution
keywords: [hudi, incremental, batch, stream, processing, schema, evolution]
summary: In this page, we will discuss schema evolution support in Hudi.
toc: true
last_modified_at: 2022-04-27T15:59:57-04:00
---

Schema evolution is an essential aspect of data management, and Hudi supports schema evolution on write out-of-the-box,
and experimental support for schema evolution on read. This page will discuss the schema evolution support in Hudi.

## Schema Evolution on Write
Hudi supports backwards-compatible schema evolution scenarios out of the box, such as adding a nullable field or promoting a field's datatype.

:::info
We recommend employing this approach as much as possible. This is a practical and efficient way to evolve schemas, proven at large-scale
data lakes at companies like Uber, Walmart, and LinkedIn. It is also implemented at scale by vendors like Confluent for streaming data.
Given the continuous nature of streaming data, there are no boundaries to define a schema change that can be incompatible with
the previous schema (e.g., renaming a column).   
:::

Furthermore, the evolved schema is queryable across high-performance engines like Presto and Spark SQL without additional overhead for column ID translations or
type reconciliations. The following table summarizes the schema changes compatible with different Hudi table types.

| Schema Change                                                                    | COW      | MOR     | Remarks                                                                                                                                                                                                                                                                                       |
|:---------------------------------------------------------------------------------|:---------|:--------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Add a new nullable column at root level at the end                               | Yes      | Yes     | `Yes` means that a write with evolved schema succeeds and a read following the write succeeds to read entire dataset.                                                                                                                                                                         |
| Add a new nullable column to inner struct (at the end)                           | Yes      | Yes     |
| Add a new complex type field with default (map and array)                        | Yes      | Yes     |                                                                                                                                                                                                                                                                                               |
| Add a new nullable column and change the ordering of fields                      | No       | No      | Write succeeds but read fails if the write with evolved schema updated only some of the base files but not all. Currently, Hudi does not maintain a schema registry with history of changes across base files. Nevertheless, if the upsert touched all base files then the read will succeed. |
| Add a custom nullable Hudi meta column, e.g. `_hoodie_meta_col`                  | Yes      | Yes     |                                                                                                                                                                                                                                                                                               |
| Promote datatype from `int` to `long` for a field at root level                  | Yes      | Yes     | For other types, Hudi supports promotion as specified in [Avro schema resolution](https://avro.apache.org/docs/++version++/specification/#schema-resolution).                                                                                                                                                |
| Promote datatype from `int` to `long` for a nested field                         | Yes      | Yes     |
| Promote datatype from `int` to `long` for a complex type (value of map or array) | Yes      | Yes     |                                                                                                                                                                                                                                                                                               |
| Add a new non-nullable column at root level at the end                           | No       | No      | In case of MOR table with Spark data source, write succeeds but read fails. As a **workaround**, you can make the field nullable.                                                                                                                                                             |
| Add a new non-nullable column to inner struct (at the end)                       | No       | No      |                                                                                                                                                                                                                                                                                               |
| Change datatype from `long` to `int` for a nested field                          | No       | No      |                                                                                                                                                                                                                                                                                               |
| Change datatype from `long` to `int` for a complex type (value of map or array)  | No       | No      |                                                                                                                                                                                                                                                                                               |


## Schema Evolution on read

There are often scenarios where it's desirable to have the ability to evolve the schema more flexibly.
For example,

1. Columns (including nested columns) can be added, deleted, modified, and moved.
2. Renaming of columns (including nested columns).
3. Add, delete, or perform operations on nested columns of the Array type.

Hudi has experimental support for allowing backward incompatible schema evolution scenarios on write while resolving
it during read time. To enable this feature, `hoodie.schema.on.read.enable=true` needs to be set on the writer config (Datasource) or table property (SQL).

:::note
Hudi versions > 0.11 and Spark versions > 3.1.x, and 3.2.1 are required. For Spark 3.2.1 and above,
`spark.sql.catalog.spark_catalog` must also be set. If schema on read is enabled, it cannot be disabled again
since the table would have accepted such schema changes already.
:::

### Adding Columns

```sql
-- add columns
ALTER TABLE tableName ADD COLUMNS(col_spec[, col_spec ...])
```

Column specification consists of five field, next to each other. 

| Parameter    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|:-------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| col_name     | name of the new column. To add sub-column col1 to a nested map type column member map\<string, struct\<n: string, a: int>>, set this field to member.value.col1                                                                                                                                                                                                                                                                               |
| col_type     | type of the new column.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| nullable     | whether or not the new column allows null values. (optional)                                                                                                                                                                                                                                                                                                                                                                                          |
| comment      | comment of the new column. (optional)                                                                                                                                                                                                                                                                                                                                                                                                       |
| col_position | The position where the new column is added. The value can be *FIRST* or *AFTER origin_col*. If it is set to *FIRST*, the new column will be added before the first column of the table. If it is set to *AFTER origin_col*, the new column will be added after the original column.  *FIRST* can be used only when new sub-columns are added to nested columns and not in top-level columns. There are no restrictions on the usage of *AFTER*. |

**Examples**

```sql
ALTER TABLE h0 ADD COLUMNS(ext0 string);
ALTER TABLE h0 ADD COLUMNS(new_col int not null comment 'add new column' AFTER col1);
ALTER TABLE complex_table ADD COLUMNS(col_struct.col_name string comment 'add new column to a struct col' AFTER col_from_col_struct);
```

### Altering Columns
**Syntax**
```sql
-- alter table ... alter column
ALTER TABLE tableName ALTER [COLUMN] col_old_name TYPE column_type [COMMENT] col_comment[FIRST|AFTER] column_name
```

**Parameter Description**

| Parameter        | Description                                                                                                                                          |
|:-----------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------|
| tableName        | Table name.                                                                                                                                          |
| col_old_name     | Name of the column to be altered.                                                                                                                    |
| column_type      | Type of the target column.                                                                                                                           |
| col_comment      | Optional comments on the altered column.                                                                                                             |
| column_name      | The new position to place the altered column. For example, *AFTER* **column_name** indicates that the target column is placed after **column_name**. |


**Examples**

```sql
--- Changing the column type
ALTER TABLE table1 ALTER COLUMN a.b.c TYPE bigint

--- Altering other attributes
ALTER TABLE table1 ALTER COLUMN a.b.c COMMENT 'new comment'
ALTER TABLE table1 ALTER COLUMN a.b.c FIRST
ALTER TABLE table1 ALTER COLUMN a.b.c AFTER x
ALTER TABLE table1 ALTER COLUMN a.b.c DROP NOT NULL
```

**column type change**

| Source\Target      | long  | float | double | string | decimal | date | int |
|--------------------|-------|-------|--------|--------|---------|------|-----|
| int                |   Y   |   Y   |    Y   |    Y   |    Y    |   N  |  Y  |
| long               |   Y   |   Y   |    Y   |    Y   |    Y    |   N  |  N  |
| float              |   N   |   Y   |    Y   |    Y   |    Y    |   N  |  N  |
| double             |   N   |   N   |    Y   |    Y   |    Y    |   N  |  N  |
| decimal            |   N   |   N   |    N   |    Y   |    Y    |   N  |  N  |
| string             |   N   |   N   |    N   |    Y   |    Y    |   Y  |  N  |
| date               |   N   |   N   |    N   |    Y   |    N    |   Y  |  N  |

### Deleting Columns
**Syntax**
```sql
-- alter table ... drop columns
ALTER TABLE tableName DROP COLUMN|COLUMNS cols
```

**Examples**

```sql
ALTER TABLE table1 DROP COLUMN a.b.c
ALTER TABLE table1 DROP COLUMNS a.b.c, x, y
```

### Renaming columns
**Syntax**
```sql
-- alter table ... rename column
ALTER TABLE tableName RENAME COLUMN old_columnName TO new_columnName
```

**Examples**

```sql
ALTER TABLE table1 RENAME COLUMN a.b.c TO x
```

:::note
When using hive metastore, please disable  `hive.metastore.disallow.incompatible.col.type.changes` if you encounter this error:
`The following columns have types incompatible with the existing columns in their respective positions`.
:::

## Schema Evolution in Action 

Let us walk through an example to demonstrate the schema evolution support in Hudi. In the below example, we are going to add a new string field and change the datatype of a field from int to long.

```scala
scala> :paste 
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val tableName = "hudi_trips_cow"
val basePath = "file:///tmp/hudi_trips_cow"
val schema = StructType( Array(
     StructField("rowId", StringType,true),
     StructField("partitionId", StringType,true),
     StructField("preComb", LongType,true),
     StructField("name", StringType,true),
     StructField("versionId", StringType,true),
     StructField("intToLong", IntegerType,true)
))
        
        
val data1 = Seq(Row("row_1", "part_0", 0L, "bob", "v_0", 0),
                Row("row_2", "part_0", 0L, "john", "v_0", 0),
                Row("row_3", "part_0", 0L, "tom", "v_0", 0))
        
var dfFromData1 = spark.createDataFrame(data1, schema)
dfFromData1.write.format("hudi").
   options(getQuickstartWriteConfigs).
   option(PRECOMBINE_FIELD.key, "preComb").
   option(RECORDKEY_FIELD.key, "rowId").
   option(PARTITIONPATH_FIELD.key, "partitionId").
   option("hoodie.index.type","SIMPLE").
   option(TBL_NAME.key, tableName).
   mode(Overwrite).
   save(basePath)

var tripsSnapshotDF1 = spark.read.format("hudi").load(basePath + "/*/*")
tripsSnapshotDF1.createOrReplaceTempView("hudi_trips_snapshot")

ctrl+D

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
scala> :paste 
val newSchema = StructType( Array(
    StructField("rowId", StringType,true),
    StructField("partitionId", StringType,true),
    StructField("preComb", LongType,true),
    StructField("name", StringType,true),
    StructField("versionId", StringType,true),
    StructField("intToLong", LongType,true),
    StructField("newField", StringType,true)
))

val data2 = Seq(Row("row_2", "part_0", 5L, "john", "v_3", 3L, "newField_1"),
               Row("row_5", "part_0", 5L, "maroon", "v_2", 2L, "newField_1"),
               Row("row_9", "part_0", 5L, "michael", "v_2", 2L, "newField_1"))

var dfFromData2 = spark.createDataFrame(data2, newSchema)
dfFromData2.write.format("hudi").
    options(getQuickstartWriteConfigs).
    option(PRECOMBINE_FIELD.key, "preComb").
    option(RECORDKEY_FIELD.key, "rowId").
    option(PARTITIONPATH_FIELD.key, "partitionId").
    option("hoodie.index.type","SIMPLE").
    option(TBL_NAME.key, tableName).
    mode(Append).
    save(basePath)

var tripsSnapshotDF2 = spark.read.format("hudi").load(basePath + "/*/*")
tripsSnapshotDF2.createOrReplaceTempView("hudi_trips_snapshot")

Ctrl + D

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


