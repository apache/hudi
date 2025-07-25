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

package org.apache.hudi.functional

import org.apache.hudi.common.model._
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util.stream.Stream

class TestTableCreationForTableVersion9 extends SparkClientFunctionalTestHarness {

  @ParameterizedTest
  @MethodSource(Array("provideTableCreationTestCases"))
  def testTableCreationWithVersion9(tableType: String,
                                   recordMergeMode: String,
                                   payloadClassName: String,
                                   recordMergeStrategyId: String,
                                   expectedConfigs: Map[String, String]): Unit = {
    withTempDir { tmp =>
      val tableName = "test_table_v9"
      val basePath = tmp.getCanonicalPath

      // Create table with version 9
      val createTableSql = s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  ts long,
         |  dt string
         | ) using hudi
         | tblproperties (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  hoodie.write.table.version = 9
         | )
         | partitioned by(dt)
         | location '$basePath'
         |""".stripMargin

      spark.sql(createTableSql)

      // Verify table was created successfully
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()))
        .build()

      val tableConfig = metaClient.getTableConfig

      // Verify table version is 9
      assertEquals(9, tableConfig.getTableVersion.versionCode())

      // Verify expected configs are set correctly
      expectedConfigs.foreach { case (key, expectedValue) =>
        if (expectedValue != null) {
          assertEquals(expectedValue, tableConfig.getString(key), s"Config $key should be $expectedValue")
        } else {
          assertFalse(tableConfig.contains(key), s"Config $key should not be present")
        }
      }

      // Test basic write operations
      val testData = Seq((1, "test1", 1000L, "2023-01-01"), (2, "test2", 2000L, "2023-01-02"))
      val df = spark.createDataFrame(testData).toDF("id", "name", "ts", "dt")

      df.write.format("hudi")
        .option("hoodie.write.table.version", "9")
        .option("hoodie.datasource.write.operation", "insert")
        .mode(SaveMode.Append)
        .save(basePath)

      // Verify data was written correctly
      val readDf = spark.read.format("hudi").load(basePath)
      assertEquals(2, readDf.count())
    }
  }

  @ParameterizedTest
  @MethodSource(Array("providePayloadClassTestCases"))
  def testTableCreationWithDifferentPayloadClasses(tableType: String,
                                                  payloadClassName: String,
                                                  expectedConfigs: Map[String, String]): Unit = {
    withTempDir { tmp =>
      val tableName = "test_table_v9_payload"
      val basePath = tmp.getCanonicalPath

      // Create table with specific payload class
      val createTableSql = s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  ts long,
         |  dt string
         | ) using hudi
         | tblproperties (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  hoodie.write.table.version = 9,
         |  hoodie.datasource.write.payload.class = '$payloadClassName'
         | )
         | partitioned by(dt)
         | location '$basePath'
         |""".stripMargin

      spark.sql(createTableSql)

      // Verify table was created successfully
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()))
        .build()

      val tableConfig = metaClient.getTableConfig

      // Verify table version is 9
      assertEquals(9, tableConfig.getTableVersion.versionCode())

      // Verify expected configs are set correctly
      expectedConfigs.foreach { case (key, expectedValue) =>
        if (expectedValue != null) {
          assertEquals(expectedValue, tableConfig.getString(key), s"Config $key should be $expectedValue")
        } else {
          assertFalse(tableConfig.contains(key), s"Config $key should not be present")
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource(Array("provideTableConfigValidationTestCases"))
  def testTableConfigValidationWhenAddingData(tableType: String,
                                             initialPayloadClass: String,
                                             writePayloadClass: String,
                                             shouldSucceed: Boolean): Unit = {
    withTempDir { tmp =>
      val tableName = "test_table_v9_validation"
      val basePath = tmp.getCanonicalPath

      // Create table with initial payload class
      val createTableSql = s"""
         | create table $tableName (
         |  id int,
         |  name string,
         |  ts long,
         |  dt string
         | ) using hudi
         | tblproperties (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  hoodie.write.table.version = 9
         | )
         | partitioned by(dt)
         | location '$basePath'
         |""".stripMargin

      spark.sql(createTableSql)

      // Insert initial data
      val initialData = Seq((1, "test1", 1000L, "2023-01-01"), (2, "test2", 2000L, "2023-01-02"))
      val initialDf = spark.createDataFrame(initialData).toDF("id", "name", "ts", "dt")

      initialDf.write.format("hudi")
        .option("hoodie.write.table.version", "9")
        .option("hoodie.datasource.write.operation", "insert")
        .mode(SaveMode.Append)
        .save(basePath)

      // Try to add more data with potentially different payload class
      val additionalData = Seq((3, "test3", 3000L, "2023-01-03"), (4, "test4", 4000L, "2023-01-04"))
      val additionalDf = spark.createDataFrame(additionalData).toDF("id", "name", "ts", "dt")

      val writeOptions = Map(
        "hoodie.write.table.version" -> "9",
        "hoodie.datasource.write.operation" -> "upsert"
      ) ++ (if (writePayloadClass != null) Map("hoodie.datasource.write.payload.class" -> writePayloadClass) else Map.empty)

      if (shouldSucceed) {
        // This should succeed
        additionalDf.write.format("hudi")
          .options(writeOptions)
          .mode(SaveMode.Append)
          .save(basePath)

        // Verify data was written correctly
        val readDf = spark.read.format("hudi").load(basePath)
        assertEquals(4, readDf.count())
      } else {
        // This should fail with validation error
        val exception = assertThrows(classOf[Exception], () => {
          additionalDf.write.format("hudi")
            .options(writeOptions)
            .mode(SaveMode.Append)
            .save(basePath)
        })

        // Verify the exception contains validation error message
        assertTrue(exception.getMessage.contains("payload") ||
                  exception.getMessage.contains("config") ||
                  exception.getMessage.contains("validation"),
                  s"Exception should contain validation error: ${exception.getMessage}")
      }
    }
  }

  def provideTableCreationTestCases(): Stream[Arguments] = {
    Stream.of(
      // Test case 1: Default table creation with version 9 (no payload class specified)
      Arguments.of(
        "COPY_ON_WRITE",
        null,
        null,
        null,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "COMMIT_TIME_ORDERING",
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
        )
      ),
      // Test case 2: MERGE_ON_READ table
      Arguments.of(
        "MERGE_ON_READ",
        null,
        null,
        null,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "COMMIT_TIME_ORDERING",
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
        )
      )
    )
  }

  def providePayloadClassTestCases(): Stream[Arguments] = {
    Stream.of(
      // Test case 1: DefaultHoodieRecordPayload (event time based)
      Arguments.of(
        "COPY_ON_WRITE",
        classOf[DefaultHoodieRecordPayload].getName,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[DefaultHoodieRecordPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> "org.apache.hudi.common.model.DefaultHoodieRecordPayload"
        )
      ),
      // Test case 2: OverwriteWithLatestAvroPayload (commit time based)
      Arguments.of(
        "COPY_ON_WRITE",
        classOf[OverwriteWithLatestAvroPayload].getName,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "COMMIT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[OverwriteWithLatestAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
        )
      ),
      // Test case 3: PartialUpdateAvroPayload (should set partial update mode)
      Arguments.of(
        "COPY_ON_WRITE",
        classOf[PartialUpdateAvroPayload].getName,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[PartialUpdateAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> "org.apache.hudi.common.model.PartialUpdateAvroPayload",
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> "IGNORE_DEFAULTS"
        )
      ),
      // Test case 4: PostgresDebeziumAvroPayload (should set partial update mode and custom properties)
      Arguments.of(
        "COPY_ON_WRITE",
        classOf[PostgresDebeziumAvroPayload].getName,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[PostgresDebeziumAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> "org.apache.hudi.common.model.PostgresDebeziumAvroPayload",
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> "IGNORE_MARKERS",
          HoodieTableConfig.MERGE_CUSTOM_PROPERTY_PREFIX + HoodieTableConfig.PARTIAL_UPDATE_CUSTOM_MARKER -> "__debezium_unavailable_value"
        )
      )
    )
  }

  def provideTableConfigValidationTestCases(): Stream[Arguments] = {
    Stream.of(
      // Test case 1: No payload class in table, no payload class in write - should succeed
      Arguments.of("COPY_ON_WRITE", null, null, true),
      // Test case 2: No payload class in table, empty payload class in write - should succeed
      Arguments.of("COPY_ON_WRITE", null, "", true),
      // Test case 3: No payload class in table, different payload class in write - should fail
      Arguments.of("COPY_ON_WRITE", null, classOf[DefaultHoodieRecordPayload].getName, false),
      // Test case 4: MERGE_ON_READ table with same scenario
      Arguments.of("MERGE_ON_READ", null, null, true)
    )
  }
}
