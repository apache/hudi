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
import org.apache.hudi.common.model.debezium.DebeziumConstants
import org.apache.hudi.common.model.DefaultHoodieRecordPayload
import spark.implicits._

val baseDir = "${BASE_PATH}"

println("Generating payload tables for all 8 payload classes...")

// Define all payload classes with their short names
val payloadClasses = Seq(
  ("default", "org.apache.hudi.common.model.DefaultHoodieRecordPayload"),
  ("overwrite", "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"),
  ("partial", "org.apache.hudi.common.model.PartialUpdateAvroPayload"),
  ("postgres", "org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload"),
  ("mysql", "org.apache.hudi.common.model.debezium.MySqlDebeziumAvroPayload"),
  ("awsdms", "org.apache.hudi.common.model.AWSDmsAvroPayload"),
  ("eventtime", "org.apache.hudi.common.model.EventTimeAvroPayload"),
  ("overwritenondefaults", "org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload")
)

// Define columns matching the TestPayloadDeprecationFlow structure
val columns = Seq("ts", "_event_lsn", "rider", "driver", "fare", "Op", "_event_seq",
  DebeziumConstants.FLATTENED_FILE_COL_NAME, DebeziumConstants.FLATTENED_POS_COL_NAME, DebeziumConstants.FLATTENED_OP_COL_NAME)

// SIMPLIFIED test data for v6 fixture
// Strategy: Keep it minimal and predictable
// 1. Initial insert with 5 records (ts=10)
// 2. Update batch with higher ordering (ts=11)
// 3. Delete batch (ts=12)

val initialData = Seq(
  (10, 1L, "rider-A", "driver-A", 19.10, "i", "10.1", 10, 1, "i"),
  (10, 2L, "rider-B", "driver-B", 27.70, "i", "10.1", 10, 1, "i"),
  (10, 3L, "rider-C", "driver-C", 33.90, "i", "10.1", 10, 1, "i"),
  (10, 4L, "rider-D", "driver-D", 34.15, "i", "10.1", 10, 1, "i"),
  (10, 5L, "rider-E", "driver-E", 17.85, "i", "10.1", 10, 1, "i")
)

val updateData = Seq(
  (11, 1L, "rider-X", "driver-X", 19.10, "u", "11.1", 11, 1, "u"),
  (11, 2L, "rider-Y", "driver-Y", 27.70, "u", "11.1", 11, 1, "u"),
  (11, 3L, "rider-CC", "driver-CC", 33.90, "u", "11.1", 11, 1, "u")
)

val deleteData = Seq(
  (12, 1L, "rider-X", "driver-X", 19.10, "i", "12.1", 12, 1, "i"),
  (12, 5L, "rider-E", "driver-E", 17.85, "i", "12.1", 12, 1, "i")
)

// Function to create table for a specific payload
def createPayloadTable(payloadName: String, payloadClass: String): Unit = {
  val tableName = s"hudi_v6_table_payload_$payloadName"
  val tableBasePath = s"$baseDir/hudi-v6-table-payload-$payloadName"

  println(s"Creating table for payload: $payloadName at $tableBasePath")

  // Base configuration for all payload tables
  val baseConfig = Map(
    "hoodie.table.name" -> tableName,
    "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
    "hoodie.parquet.max.file.size" -> "2048",
    "hoodie.parquet.small.file.limit" -> "1024",
    "hoodie.metadata.enable" -> "false"
  )

  // Payload-specific configuration
  val payloadConfig = Map(
    "hoodie.datasource.write.payload.class.name" -> payloadClass
  )

  // Determine ordering fields based on payload class
  val orderingFields = if (payloadClass.contains("MySqlDebeziumAvroPayload")) {
    "_event_seq"
  } else if (payloadClass.contains("PostgresDebeziumAvroPayload")) {
    "_event_lsn"
  } else {
    "ts"
  }

  // Record key and partition configuration
  val tableStructureConfig = Map(
    RECORDKEY_FIELD.key() -> "_event_lsn",
    HoodieTableConfig.PRECOMBINE_FIELD.key() -> orderingFields,
    "hoodie.datasource.write.precombine.field" -> orderingFields, // required for ordering field to be set
    PARTITIONPATH_FIELD.key() -> ""  // Non-partitioned table
  )

  // Delete marker configuration for certain payloads
  val deleteConfig = if (payloadClass.contains("DefaultHoodieRecordPayload")) {
    Map(
      DefaultHoodieRecordPayload.DELETE_KEY -> "Op",
      DefaultHoodieRecordPayload.DELETE_MARKER -> "D"
    )
  } else {
    Map.empty[String, String]
  }

  // Service configurations to enable table management operations
  val serviceConfig = Map(
    "hoodie.clustering.inline" -> "true",
    "hoodie.clustering.inline.max.commits" -> "2",
    "hoodie.clustering.plan.strategy.small.file.limit" -> "512000",
    "hoodie.clustering.plan.strategy.target.file.max.bytes" -> "512000"
  )

  // Combine all configurations
  val allConfig = baseConfig ++ payloadConfig ++ tableStructureConfig ++ deleteConfig ++ serviceConfig

  // 1. Initial insert - 5 records at ts=10
  val initialDf = spark.createDataFrame(initialData).toDF(columns: _*)
  initialDf.write.format("hudi")
    .options(allConfig)
    .option(OPERATION.key(), BULK_INSERT_OPERATION_OPT_VAL)
    .mode(SaveMode.Overwrite)
    .save(tableBasePath)

  // 2. Update batch - higher ordering (ts=11)
  val updateDf = spark.createDataFrame(updateData).toDF(columns: _*)
  updateDf.write.format("hudi")
    .options(allConfig)
    .option(OPERATION.key(), "upsert")
    .mode(SaveMode.Append)
    .save(tableBasePath)

  // 3. Delete operations - ts=12
  val deleteDf = spark.createDataFrame(deleteData).toDF(columns: _*)
  deleteDf.write.format("hudi")
    .options(allConfig)
    .option(OPERATION.key(), "delete")
    .mode(SaveMode.Append)
    .save(tableBasePath)

  println(s"Completed table creation for payload: $payloadName")
}

// Create all payload tables
payloadClasses.foreach { case (payloadName, payloadClass) =>
  createPayloadTable(payloadName, payloadClass)
}

println("All payload tables generated successfully!")
System.exit(0)