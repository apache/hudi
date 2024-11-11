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

import org.apache.hudi.DataSourceWriteOptions.{DELETE_OPERATION_OPT_VAL, INSERT_OPERATION_OPT_VAL, OPERATION, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE, UPSERT_OPERATION_OPT_VAL}
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.metadata.HoodieMetadataPayload.SECONDARY_INDEX_RECORD_KEY_SEPARATOR
import org.apache.hudi.metadata.SecondaryIndexKeyUtils
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{Tag, Test}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

@Tag("functional")
class TestSecondaryIndexWithDataSource extends HoodieSparkSqlTestBase {

  val tableName = "hoodie_test"
  var instantTime: AtomicInteger = new AtomicInteger(1)
  val metadataOpts: Map[String, String] = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "true"
  )
  val commonOpts: Map[String, String] = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    HoodieWriteConfig.TBL_NAME.key -> tableName,
    RECORDKEY_FIELD.key -> "_row_key",
    PARTITIONPATH_FIELD.key -> "partition_path",
    PRECOMBINE_FIELD.key -> "timestamp",
    HoodieClusteringConfig.INLINE_CLUSTERING.key -> "true",
    HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key -> "4",
    HoodieCompactionConfig.INLINE_COMPACT.key() -> "true",
    HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "3"
  ) ++ metadataOpts
  var mergedDfList: List[DataFrame] = List.empty
  var dataGen = new HoodieTestDataGenerator()
  var metaClient: HoodieTableMetaClient = _
  var basePath: String = _

  @Test
  def testCreateAndUpdateTableWithSecondaryIndex(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir { tmp =>
        basePath = s"${tmp.getCanonicalPath}/$tableName"
        // Step 1: Initial Insertion of Records
        val initialRecords = recordsToStrings(dataGen.generateInserts(getInstantTime, 50, true)).asScala
        val initialDf = spark.read.json(spark.sparkContext.parallelize(initialRecords.toSeq, 2))
        val hudiOpts = commonOpts + (TABLE_TYPE.key -> "MERGE_ON_READ")
        initialDf.write.format("hudi")
          .options(hudiOpts)
          .option(OPERATION.key, INSERT_OPERATION_OPT_VAL)
          .mode(SaveMode.Overwrite)
          .save(basePath)

        // Step 2: Create table and secondary index on 'rider' column
        spark.sql(s"CREATE TABLE $tableName USING hudi LOCATION '$basePath'")
        spark.sql(s"create index idx_rider on $tableName using secondary_index(rider)")

        // Verify initial state of secondary index
        val initialKeys = spark.sql(s"select _row_key from $tableName limit 5").collect().map(_.getString(0))
        validateSecondaryIndex(basePath, initialKeys)

        // Step 3: Perform Update Operations on Subset of Records
        var updateRecords = recordsToStrings(dataGen.generateUniqueUpdates(getInstantTime, 10, HoodieTestDataGenerator.TRIP_FLATTENED_SCHEMA)).asScala
        var updateDf = spark.read.json(spark.sparkContext.parallelize(updateRecords.toSeq, 2))
        updateDf.write.format("hudi")
          .options(hudiOpts)
          .option(OPERATION.key, UPSERT_OPERATION_OPT_VAL)
          .mode(SaveMode.Append)
          .save(basePath)
        // Verify secondary index after updates
        var updateKeys = updateDf.select("_row_key").collect().map(_.getString(0))
        validateSecondaryIndex(basePath, updateKeys)

        // Step 4: Trigger Compaction with this update as the compaction frequency is set to 3 commits
        updateRecords = recordsToStrings(dataGen.generateUniqueUpdates(getInstantTime, 10, HoodieTestDataGenerator.TRIP_FLATTENED_SCHEMA)).asScala
        updateDf = spark.read.json(spark.sparkContext.parallelize(updateRecords.toSeq, 2))
        updateDf.write.format("hudi")
          .options(hudiOpts)
          .option(OPERATION.key, UPSERT_OPERATION_OPT_VAL)
          .mode(SaveMode.Append)
          .save(basePath)
        // Verify compaction
        metaClient = HoodieTableMetaClient.builder()
          .setBasePath(basePath)
          .setConf(HoodieTestUtils.getDefaultStorageConf)
          .build()
        assertTrue(metaClient.getActiveTimeline.getCommitTimeline.filterCompletedInstants.lastInstant.isPresent)
        // Verify secondary index after compaction
        updateKeys = updateDf.select("_row_key").collect().map(_.getString(0))
        validateSecondaryIndex(basePath, updateKeys)

        // Step 5: Trigger Clustering with this update as the clustering frequency is set to 4 commits
        updateRecords = recordsToStrings(dataGen.generateUniqueUpdates(getInstantTime, 10, HoodieTestDataGenerator.TRIP_FLATTENED_SCHEMA)).asScala
        updateDf = spark.read.json(spark.sparkContext.parallelize(updateRecords.toSeq, 2))
        updateDf.write.format("hudi")
          .options(hudiOpts)
          .option(OPERATION.key, UPSERT_OPERATION_OPT_VAL)
          .mode(SaveMode.Append)
          .save(basePath)
        // Verify clustering
        metaClient = HoodieTableMetaClient.reload(metaClient)
        assertTrue(metaClient.getActiveTimeline.getCompletedReplaceTimeline.lastInstant.isPresent)
        // Verify secondary index after clustering
        updateKeys = updateDf.select("_row_key").collect().map(_.getString(0))
        validateSecondaryIndex(basePath, updateKeys)

        // Step 6: Perform Deletes on Records and Validate Secondary Index
        val deleteKeys = initialKeys.take(3) // pick a subset of keys to delete
        val deleteDf = spark.read.format("hudi").load(basePath).filter(s"_row_key in ('${deleteKeys.mkString("','")}')")
        deleteDf.write.format("hudi")
          .options(hudiOpts)
          .option(OPERATION.key, DELETE_OPERATION_OPT_VAL)
          .mode(SaveMode.Append)
          .save(basePath)
        // Verify secondary index for deletes
        validateSecondaryIndex(basePath, deleteKeys, hasDeleteKeys = true)
        // Verify for non deleted keys
        val nonDeletedKeys = initialKeys.diff(deleteKeys)
        validateSecondaryIndex(basePath, nonDeletedKeys)

        // Step 7: Final Update and Validation
        val finalUpdateRecords = recordsToStrings(dataGen.generateUniqueUpdates(getInstantTime, 10, HoodieTestDataGenerator.TRIP_FLATTENED_SCHEMA)).asScala
        val finalUpdateDf = spark.read.json(spark.sparkContext.parallelize(finalUpdateRecords.toSeq, 2))
        finalUpdateDf.write.format("hudi")
          .options(hudiOpts)
          .option(OPERATION.key, UPSERT_OPERATION_OPT_VAL)
          .mode(SaveMode.Append)
          .save(basePath)
        // Verify secondary index after final updates
        val finalUpdateKeys = finalUpdateDf.select("_row_key").collect().map(_.getString(0))
        validateSecondaryIndex(basePath, nonDeletedKeys)
        validateSecondaryIndex(basePath, finalUpdateKeys)
      }
    }
  }

  def validateSecondaryIndex(basePath: String, recordKeys: Array[String], hasDeleteKeys: Boolean = false): Unit = {
    // Check secondary index metadata for the selected keys
    recordKeys.foreach { key =>
      val secondaryKeys = spark.sql(
        s"SELECT key FROM hudi_metadata('$basePath') WHERE type=7 AND key LIKE '%${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}$key'"
      ).collect().map(indexKey => SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(indexKey.getString(0)))

      // Ensure secondary keys are correctly present or deleted
      if (hasDeleteKeys) {
        assertTrue(secondaryKeys.isEmpty)
      } else {
        assertTrue(secondaryKeys.nonEmpty, s"Secondary index entry missing for key: $key")
        secondaryKeys.foreach { secKey =>
          assert(
            spark.sql(s"SELECT _row_key FROM $tableName WHERE _row_key = '$key'").count() > 0,
            s"Record key '$key' with secondary key '$secKey' should be present in the data but is missing"
          )
        }
      }
    }
  }

  def getInstantTime: String = {
    String.format("%03d", new Integer(instantTime.incrementAndGet()))
  }
}
