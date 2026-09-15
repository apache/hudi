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

# Hudi Test Resources

## Generating Hudi Resources

Follow these steps to create the `hudi_non_part_mor` test table and utilize it for testing.

### Download and install spark version 3.3 - 3.5
### Open spark scala shell

Execute the following command in the terminal to create the table locally:

### Generate Resources

* Open the `spark-shell` terminal using below command
```
./spark-shell \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.538 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```
* Execute the following Spark scala code to create the `hudi_non_part_mor` table:

```
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val schema = StructType(Seq(
  StructField("id", StringType, nullable = false),
  StructField("name", StringType, nullable = true),
  StructField("age", IntegerType, nullable = true)
))


val data = Seq(
  Row("1", "Alice", 30),
  Row("2", "Bob", 25)
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)

var basePath = "file:///tmp/hudi_non_part_mor"

df.write()
  .format("hudi")
  .mode("Append")
  .option("hoodie.table.name", "hudi_non_part_mor")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.operation", "bulk_insert")
  .option("hoodie.metadata.index.column.stats.enable", "true")
  .save(basePath);

val data = Seq(
  Row("1", "Cathy", 30),
  Row("2", "David", 25)
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)

df.write
    .format("hudi")
    .mode("Append")
    .option("hoodie.table.name", "hudi_non_part_mor")
    .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
    .option("hoodie.datasource.write.recordkey.field","id")
    .option("hoodie.datasource.write.operation","upsert")
    .option("hoodie.metadata.index.column.stats.enable", "true")
    .save(basePath)
```
