
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

val tableName = "${TABLE_NAME}"  // Backticks for SQL identifiers with hyphens
val basePath = "${BASE_PATH}"

println("Generating mor table with archival and clustering...")

// Base mor settings used by all steps
val morConfig = Map(
  "hoodie.compact.inline" -> "true",
  "hoodie.clustering.inline" -> "true",
  "hoodie.compact.inline.max.delta.commits" -> "3",
  "hoodie.clustering.inline.max.commits" -> "4",
  "hoodie.metadata.compact.max.delta.commits" -> "3",
  "hoodie.keep.min.commits" -> "5",
  "hoodie.keep.max.commits" -> "6",
  "hoodie.cleaner.commits.retained" -> "5"
)

// Initial setup (table creation + file size settings for steps 1-2)
val initialSetupConfig = Map(
  HoodieTableConfig.PRECOMBINE_FIELD.key -> "ts",
  RECORDKEY_FIELD.key -> "id",
  PARTITIONPATH_FIELD.key -> "partition",
  "hoodie.table.name" -> tableName,
  "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
  "hoodie.parquet.max.file.size" -> "2048", // 2KB - very small files
  "hoodie.parquet.small.file.limit" -> "1024", // 1KB threshold
  "hoodie.clustering.plan.strategy.small.file.limit" -> "10240", // 10KB
  "hoodie.clustering.plan.strategy.target.file.max.bytes" -> "10240" // 10KB target
)

// Archival settings
val archivalConfig = Map(
  "hoodie.archive.automatic" -> "true",
  "hoodie.commits.archival.batch" -> "1",
  "hoodie.archive.merge.files.batch.size" -> "1"
)

val initialData = Seq(
  ("id1", "Alice", 1000L, "2023-01-01"),
  ("id2", "Bob", 1001L, "2023-01-01"),
  ("id3", "Charlie", 1002L, "2023-01-01"),
  ("id4", "David", 1003L, "2023-01-02"),
  ("id5", "Eve", 1004L, "2023-01-02")
)

val initialDf = initialData.toDF("id", "name", "ts", "partition")

(initialDf.write.format("hudi").
  options(initialSetupConfig ++ morConfig).
  option("hoodie.datasource.write.operation", "insert").
  mode(SaveMode.Overwrite)).
  save(basePath)


println("Step 1: Initial data written as table")

val moreData1 = Seq(
  ("id6", "Frank", 2000L, "2023-01-01"),
  ("id7", "Grace", 2001L, "2023-01-01")
)

((moreData1.toDF("id", "name", "ts", "partition").write.format("hudi").
  options(initialSetupConfig ++ morConfig).
  option("hoodie.datasource.write.operation", "insert").
  mode(SaveMode.Append))).
  save(basePath)


println("Step 2: Added more small files")

val update1 = Seq(("id1", "Alice_v2", 3000L, "2023-01-01"))
update1.toDF("id", "name", "ts", "partition").write.format("hudi").
  options(morConfig).
  option("hoodie.datasource.write.operation", "upsert").
  mode(SaveMode.Append).
  save(basePath)


println("Step 3: First update - will trigger COMPACTION after 3 delta commits (max.delta.commits=3)")

val update2 = Seq(("id2", "Bob_v2", 4000L, "2023-01-01"))
update2.toDF("id", "name", "ts", "partition").write.format("hudi").
  options(morConfig).
  option("hoodie.datasource.write.operation", "upsert").
  mode(SaveMode.Append).
  save(basePath)


println("Step 4: Second update - will trigger CLUSTERING after 4 commits (max.commits=4)")

val finalData = Seq(("id8", "Final", 5000L, "2023-01-01"))
finalData.toDF("id", "name", "ts", "partition").write.format("hudi").
  options(morConfig ++ archivalConfig).
  option("hoodie.datasource.write.operation", "insert").
  mode(SaveMode.Append).
  save(basePath)


println("Step 5: Insert - will trigger CLEANING (retained=5) and begin ARCHIVAL setup")

val extraData = Seq(("id9", "Extra", 6000L, "2023-01-01"))
extraData.toDF("id", "name", "ts", "partition").write.format("hudi").
  options(morConfig ++ archivalConfig).
  option("hoodie.datasource.write.operation", "insert").
  mode(SaveMode.Append).
  save(basePath)

println("Step 6: Extra insert - will trigger ARCHIVAL (keep.max.commits=6 exceeded)")

val moreExtraData = Seq(("id10", "MoreExtra", 7000L, "2023-01-01"))
moreExtraData.toDF("id", "name", "ts", "partition").write.format("hudi").
  options(morConfig ++ archivalConfig).
  option("hoodie.datasource.write.operation", "insert").
  mode(SaveMode.Append).
  save(basePath)

println("Step 7: More extra insert - ensures ARCHIVAL is completed")

val deleteData = Seq(("id1", "Alice_v2", 9000L, "2023-01-01"))
deleteData.toDF("id", "name", "ts", "partition").write.format("hudi").
  option("hoodie.datasource.write.operation", "delete").
  option("hoodie.compact.inline", "false"). // Disable compaction to keep log files uncompacted
  option("hoodie.clustering.inline", "false").
  option("hoodie.clean.automatic", "false").
  option("hoodie.archive.automatic", "false").
  mode(SaveMode.Append).
  save(basePath)

println("Step 8: Delete operation (creates uncompacted log files)")

println(s"mor table fixture ${FIXTURE_NAME} generated!")
System.exit(0)