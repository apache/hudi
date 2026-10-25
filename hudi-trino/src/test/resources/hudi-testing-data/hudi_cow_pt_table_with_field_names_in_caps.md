<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

## Create script

Structure of table:
- COW partitioned table with field names containing caps
- No log files

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val schema = StructType(Seq(
    StructField("Id", StringType, nullable = false),
    StructField("Name", StringType, nullable = true),
    StructField("Age", IntegerType, nullable = true),
    StructField("Country", StringType, nullable = true)
))


var data = Seq(
    Row("1", "Alice", 30, "IND"),
    Row("2", "Bob", 25, "US")
)

var df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    schema
)

df.show()

var basePath = "file:///tmp/hudi_cow_pt_table_with_field_names_in_caps/"

df.write.format("hudi").mode("Append").option("hoodie.table.name", "hudi_cow_pt_table_with_field_names_in_caps").option("hoodie.datasource.write.table.type", "COPY_ON_WRITE").option("hoodie.datasource.write.recordkey.field","Id").option("hoodie.datasource.write.operation","bulk_insert").option("hoodie.metadata.index.column.stats.enable", "true").option("hoodie.metadata.record.index.enable", "true").option("hoodie.datasource.write.secondarykey.column", "Name").option("hoodie.datasource.write.partitionpath.field", "Country").save(basePath)

data = Seq(
    Row("3", "Charlie", 30, "IND"),
    Row("4", "David", 25, "US")
)

df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    schema
)

df.show()

df.write.format("hudi").mode("Append")
        .option("hoodie.table.name", "hudi_cow_pt_table_with_field_names_in_caps")
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
        .option("hoodie.datasource.write.recordkey.field","Id")
        .option("hoodie.datasource.write.operation","bulk_insert")
        .option("hoodie.metadata.index.column.stats.enable", "true")
        .option("hoodie.metadata.record.index.enable", "true")
        .option("hoodie.datasource.write.secondarykey.column", "Name")
        .option("hoodie.datasource.write.partitionpath.field", "Country")
        .save(basePath)
```
