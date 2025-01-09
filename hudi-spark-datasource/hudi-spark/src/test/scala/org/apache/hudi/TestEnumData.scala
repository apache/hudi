/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hudi.DataSourceWriteOptions.{MOR_TABLE_TYPE_OPT_VAL, TABLE_TYPE}
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig, RecordMergeMode}
import org.apache.hudi.common.model.{HoodieAvroRecordMerger, HoodieRecordMerger, OverwriteWithLatestMerger}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

class TestEnumData extends SparkClientFunctionalTestHarness {
  val avroSchema: String =
    """
      {
        "type": "record",
        "name": "User",
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "name", "type": "string"},
          {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["ACTIVE", "INACTIVE", "PENDING"]}}
        ]
      }
    """

  @ParameterizedTest
  @MethodSource(Array("provideParams"))
  def testEnumTypeData(recordType: String,
                       mergeMode: String): Unit = { // Define the schema
    val schema = new Schema.Parser().parse(avroSchema)
    // Create Avro records
    val record1 = new GenericData.Record(schema)
    record1.put("id", "1")
    record1.put("name", "Alice")
    record1.put("status", new GenericData.EnumSymbol(schema.getField("status").schema(), "ACTIVE"))
    val record2 = new GenericData.Record(schema)
    record2.put("id", "2")
    record2.put("name", "Bob")
    record2.put("status", new GenericData.EnumSymbol(schema.getField("status").schema(), "INACTIVE"))
    val record3 = new GenericData.Record(schema)
    record3.put("id", "1")
    record3.put("name", "Alice")
    record3.put("status", new GenericData.EnumSymbol(schema.getField("status").schema(), "INACTIVE"))
    val record4 = new GenericData.Record(schema)
    record4.put("id", "2")
    record4.put("name", "Bob")
    record4.put("status", new GenericData.EnumSymbol(schema.getField("status").schema(), "ACTIVE"))

    // Define Hudi options.
    val tableName = "hudi_enum_test"
    val mergeStrategyId = if (mergeMode.equals(RecordMergeMode.EVENT_TIME_ORDERING.name)) {
      HoodieRecordMerger.DEFAULT_MERGE_STRATEGY_UUID
    } else {
      HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID
    }
    val mergeOpts: Map[String, String] = Map(
      HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key ->
        (if (recordType.equals("SPARK")) classOf[DefaultSparkRecordMerger].getName
        else classOf[HoodieAvroRecordMerger].getName),
      HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key -> mergeStrategyId)
    val fgReaderOpts: Map[String, String] = Map(
      HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> "true",
      HoodieWriteConfig.RECORD_MERGE_MODE.key -> mergeMode,
      HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key ->
        (if (recordType.equals("SPARK")) "parquet" else "avro"))
    val hudiOptions = Map(
      "hoodie.datasource.write.recordkey.field" -> "id",
      "hoodie.datasource.write.table.name" -> tableName,
      "hoodie.datasource.write.precombine.field" -> "name",
      "hoodie.datasource.write.operation" -> "upsert",
      TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL)
    val opts = mergeOpts ++ fgReaderOpts ++ hudiOptions

    // Write and read.
    val avroRecords = Seq(record1, record2)
    val updateRecords = Seq(record3, record4)

    // Convert Avro records to DataFrame
    val rows = avroRecords.map { record =>
      (record.get("id").toString, record.get("name").toString, record.get("status").toString)
    }
    val df = spark.createDataFrame(rows).toDF("id", "name", "status")
    // Insert to Hudi
    df.write.format("hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    // Update to Hudi.
    val updates = updateRecords.map { record =>
      (record.get("id").toString, record.get("name").toString, record.get("status").toString)
    }
    val updateDf = spark.createDataFrame(updates).toDF("id", "name", "status")
    updateDf.write.format("hudi")
      .options(opts)
      .mode(SaveMode.Append)
      .save(basePath)
    // Read the data back as a DataFrame
    val readDf = spark.read.options(opts).format("hudi")
      .load(basePath)

    // Validate the records
    val actual = readDf.select("id", "name", "status").collect()
      .map(row => (row.getString(0), row.getString(1), row.getString(2))).toSet
    val expected = Set(("1", "Alice", "INACTIVE"), ("2", "Bob", "ACTIVE"))
    readDf.show(false)
    assert(actual == expected)
  }
}

object TestEnumData {
  def provideParams(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      Arguments.of("AVRO", "EVENT_TIME_ORDERING"),
      Arguments.of("SPARK", "EVENT_TIME_ORDERING"),
      Arguments.of("AVRO", "COMMIT_TIME_ORDERING"),
      Arguments.of("SPARK", "COMMIT_TIME_ORDERING")
    )
  }
}

