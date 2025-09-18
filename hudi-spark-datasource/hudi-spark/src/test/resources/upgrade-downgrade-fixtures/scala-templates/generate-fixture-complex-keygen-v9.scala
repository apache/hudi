/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.sql.SaveMode
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.DataSourceWriteOptions._
import spark.implicits._

val tableName = "${TABLE_NAME}"
val basePath = "${BASE_PATH}"

// Create simple test data with consistent schema
val testData = Seq(
  ("id1", "Alice", 1000L, "2023-01-01", "a"),
  ("id2", "Bob", 2000L, "2023-01-01", "b"),
  ("id3", "Charlie", 3000L, "2023-01-02", "a"),
  ("id4", "David", 4000L, "2023-01-02", "b"),
  ("id5", "Eve", 5000L, "2023-01-03", "a")
)

val df = testData.toDF("id", "name", "ts", "partition", "category")

// Write initial batch (creates base files)
df.write.format("hudi").
  option(HoodieTableConfig.ORDERING_FIELDS.key, "ts").
  option(RECORDKEY_FIELD.key, "id").
  option(PARTITIONPATH_FIELD.key, "partition,category").
  option("hoodie.table.name", tableName).
  option("hoodie.datasource.write.table.type", "MERGE_ON_READ").
  option("hoodie.datasource.write.operation", "insert").
  option("hoodie.index.bloom.num_entries", "20").
  option("hoodie.bloom.index.false.positive.rate", "0.1").
  option("hoodie.parquet.max.file.size", "51200"). // 50KB max file size
  option("hoodie.parquet.small.file.limit", "25600"). // 25KB small file threshold
  option("hoodie.parquet.compression.codec", "snappy").
  mode(SaveMode.Overwrite).
  save(basePath)

println("Initial batch written")

// Write update batch (creates log files)
val updateData = Seq(
  ("id1", "Alice_Updated", 1001L, "2023-01-01", "a"),
  ("id2", "Bob_Updated", 2001L, "2023-01-01", "b"),
  ("id6", "Frank", 6000L, "2023-01-03", "a")
)

val updateDf = updateData.toDF("id", "name", "ts", "partition", "category")

updateDf.write.format("hudi").
  option(HoodieTableConfig.ORDERING_FIELDS.key, "ts").
  option(RECORDKEY_FIELD.key, "id").
  option(PARTITIONPATH_FIELD.key, "partition,category").
  option("hoodie.table.name", tableName).
  option("hoodie.datasource.write.table.type", "MERGE_ON_READ").
  option("hoodie.datasource.write.operation", "upsert").
  option("hoodie.index.bloom.num_entries", "20").
  option("hoodie.bloom.index.false.positive.rate", "0.1").
  option("hoodie.parquet.max.file.size", "51200"). // 50KB max file size
  option("hoodie.parquet.small.file.limit", "25600"). // 25KB small file threshold
  option("hoodie.parquet.compression.codec", "snappy").
  mode(SaveMode.Append).
  save(basePath)

println("Update batch written")

// Create one more insert to have multiple commits
val insertData = Seq(
  ("id7", "Grace", 7000L, "2023-01-04", "b"),
  ("id8", "Henry", 8000L, "2023-01-04", "b")
)

val insertDf = insertData.toDF("id", "name", "ts", "partition", "category")

insertDf.write.format("hudi").
  option(HoodieTableConfig.ORDERING_FIELDS.key, "ts").
  option(RECORDKEY_FIELD.key, "id").
  option(PARTITIONPATH_FIELD.key, "partition,category").
  option("hoodie.table.name", tableName).
  option("hoodie.datasource.write.table.type", "MERGE_ON_READ").
  option("hoodie.datasource.write.operation", "insert").
  option("hoodie.index.bloom.num_entries", "20").
  option("hoodie.bloom.index.false.positive.rate", "0.1").
  option("hoodie.parquet.max.file.size", "51200"). // 50KB max file size
  option("hoodie.parquet.small.file.limit", "25600"). // 25KB small file threshold
  option("hoodie.parquet.compression.codec", "snappy").
  mode(SaveMode.Append).
  save(basePath)

println("Additional insert written")
println(s"Fixture ${FIXTURE_NAME} generated successfully!")

System.exit(0)
