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

package org.apache.spark.sql.hudi.command.index

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.{HoodieMetadataConfig, RecordMergeMode}
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeCommitMetadata
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.metadata.HoodieMetadataPayload.SECONDARY_INDEX_RECORD_KEY_SEPARATOR
import org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX
import org.apache.hudi.metadata.SecondaryIndexKeyUtils
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieSparkUtils}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

class TestSecondaryIndex extends HoodieSparkSqlTestBase {

  var instantTime: AtomicInteger = new AtomicInteger(1)
  val metadataOpts: Map[String, String] = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "true"
  )
  val commonOpts: Map[String, String] = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    RECORDKEY_FIELD.key -> "_row_key",
    PARTITIONPATH_FIELD.key -> "partition_path",
    PRECOMBINE_FIELD.key -> "timestamp",
    HoodieClusteringConfig.INLINE_CLUSTERING.key -> "true",
    HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key -> "4",
    HoodieCompactionConfig.INLINE_COMPACT.key() -> "true",
    HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "3",
    HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key() -> "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
    DataSourceWriteOptions.RECORD_MERGE_MODE.key() -> RecordMergeMode.COMMIT_TIME_ORDERING.name()
  ) ++ metadataOpts

  test("Test Create/Show/Drop Secondary Index with External Table") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  preCombineField = 'ts',
             |  hoodie.metadata.enable = 'true',
             |  hoodie.metadata.record.index.enable = 'true',
             |  hoodie.metadata.index.secondary.enable = 'true',
             |  hoodie.datasource.write.payload.class = 'org.apache.hudi.common.model.OverwriteWithLatestAvroPayload'
             | )
             | partitioned by(ts)
             | location '$basePath'
       """.stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

        spark.sql(s"""DROP TABLE if exists $tableName""")
        // Use the same base path as above
        spark.sql(
          s"""CREATE TABLE $tableName USING hudi options (
             |     hoodie.metadata.record.index.enable = 'true'
             | ) LOCATION '$basePath'""".stripMargin)

        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")
        checkAnswer(s"show indexes from default.$tableName")(
          Seq("column_stats", "column_stats", ""),
          Seq("record_index", "record_index", "")
        )

        // Secondary index can not be created for two columns at once
        checkException(s"create index idx_name_price on $tableName (name,price)")(
          "Only one column can be indexed for functional or secondary index."
        )
        // Secondary index is created by default for non record key column when index type is not specified
        spark.sql(s"create index idx_name on $tableName (name)")
        checkAnswer(s"show indexes from default.$tableName")(
          Seq("column_stats", "column_stats", ""),
          Seq("secondary_index_idx_name", "secondary_index", "name"),
          Seq("record_index", "record_index", "")
        )

        spark.sql(s"create index idx_price on $tableName (price)")
        // Create an index with the occupied name
        checkException(s"create index idx_price on $tableName (price)")(
          "Index already exists: idx_price"
        )

        // Both indexes should be shown
        checkAnswer(s"show indexes from $tableName")(
          Seq("column_stats", "column_stats", ""),
          Seq("secondary_index_idx_name", "secondary_index", "name"),
          Seq("secondary_index_idx_price", "secondary_index", "price"),
          Seq("record_index", "record_index", "")
        )

        checkAnswer(s"drop index idx_name on $tableName")()
        // show index shows only one index after dropping
        checkAnswer(s"show indexes from $tableName")(
          Seq("column_stats", "column_stats", ""),
          Seq("secondary_index_idx_price", "secondary_index", "price"),
          Seq("record_index", "record_index", "")
        )

        // can not drop already dropped index
        checkException(s"drop index idx_name on $tableName")("Index does not exist: idx_name")
        // create index again
        spark.sql(s"create index idx_name on $tableName (name)")
        // drop index should work now
        checkAnswer(s"drop index idx_name on $tableName")()
        checkAnswer(s"show indexes from $tableName")(
          Seq("column_stats", "column_stats", ""),
          Seq("secondary_index_idx_price", "secondary_index", "price"),
          Seq("record_index", "record_index", "")
        )

        // Drop the second index and show index should show no index
        // Try a partial delete scenario where table config does not have the partition path
        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(basePath)
          .setConf(HoodieTestUtils.getDefaultStorageConf)
          .build()
        assertFalse(metaClient.getTableConfig.getRelativeIndexDefinitionPath.get().contains(metaClient.getBasePath))
        assertTrue(metaClient.getIndexDefinitionPath.contains(metaClient.getBasePath.toString))
        val indexDefinition = metaClient.getIndexMetadata.get().getIndexDefinitions.values().stream()
          .filter(indexDefn => indexDefn.getIndexType.equals(PARTITION_NAME_SECONDARY_INDEX)).findFirst().get()

        metaClient.getTableConfig.setMetadataPartitionState(metaClient, indexDefinition.getIndexName, false)
        checkAnswer(s"drop index idx_price on $tableName")()
        checkAnswer(s"show indexes from $tableName")(
          Seq("column_stats", "column_stats", ""),
          Seq("record_index", "record_index", "")
        )

        // Drop the record index and show index should show no index
        checkAnswer(s"drop index record_index on $tableName")()
        checkAnswer(s"drop index column_stats on $tableName")()
        checkAnswer(s"show indexes from $tableName")()

        checkException(s"drop index idx_price on $tableName")("Index does not exist: idx_price")

        checkExceptionContain(s"create index idx_price_1 on $tableName (field_not_exist)")(
          "Missing field field_not_exist"
        )
      }
    }
  }

  test("Test Secondary Index Creation With hudi_metadata TVF") {
    withTempDir {
      tmp => {
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"

        createTempTableAndInsert(tableName, basePath)

        // validate record_index created successfully
        val metadataDF = spark.sql(s"select key from hudi_metadata('$basePath') where type=5")
        assert(metadataDF.count() == 2)

        var metaClient = HoodieTableMetaClient.builder()
          .setBasePath(basePath)
          .setConf(HoodieTestUtils.getDefaultStorageConf)
          .build()
        assert(metaClient.getTableConfig.getMetadataPartitions.contains("record_index"))
        // create secondary index
        spark.sql(s"create index idx_city on $tableName (city)")
        metaClient = HoodieTableMetaClient.builder()
          .setBasePath(basePath)
          .setConf(HoodieTestUtils.getDefaultStorageConf)
          .build()
        assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_city"))
        assert(metaClient.getTableConfig.getMetadataPartitions.contains("record_index"))

        checkAnswer(s"select key, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
          Seq(s"austin${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}e96c4396-3fad-413a-a942-4cb36106d720", false),
          Seq(s"san_francisco${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}334e26e9-8355-45cc-97c6-c31daf0df330", false)
        )
      }
    }
  }

  test("Test Secondary Index Creation Failure For Multiple Fields") {
    withTempDir {
      tmp => {
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"

        createTempTableAndInsert(tableName, basePath)

        // validate record_index created successfully
        val metadataDF = spark.sql(s"select key from hudi_metadata('$basePath') where type=5")
        assert(metadataDF.count() == 2)

        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(basePath)
          .setConf(HoodieTestUtils.getDefaultStorageConf)
          .build()
        assert(metaClient.getTableConfig.getMetadataPartitions.contains("record_index"))
        // create secondary index throws error when trying to create on multiple fields at a time
        checkException(sql = s"create index idx_city on $tableName (city,state)")(
          "Only one column can be indexed for functional or secondary index."
        )
      }
    }
  }

  test("Test Secondary Index With Updates Compaction Clustering Deletes") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      // Step 1: Initial Insertion of Records
      val dataGen = new HoodieTestDataGenerator()
      val hudiOpts: Map[String, String] = loadInitialBatchAndCreateSecondaryIndex(tableName, basePath, dataGen)

      // Verify initial state of secondary index
      val initialKeys = spark.sql(s"select _row_key from $tableName limit 5").collect().map(_.getString(0))
      validateSecondaryIndex(basePath, tableName, initialKeys)
      val initialRecordsCount = spark.sql(s"select _row_key from $tableName").count()

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
      validateSecondaryIndex(basePath, tableName, updateKeys)

      // Step 4: Trigger Compaction with this update as the compaction frequency is set to 3 commits
      updateRecords = recordsToStrings(dataGen.generateUniqueUpdates(getInstantTime, 10, HoodieTestDataGenerator.TRIP_FLATTENED_SCHEMA)).asScala
      updateDf = spark.read.json(spark.sparkContext.parallelize(updateRecords.toSeq, 2))
      updateDf.write.format("hudi")
        .options(hudiOpts)
        .option(OPERATION.key, UPSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)
      // Verify compaction
      var metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assertTrue(metaClient.getActiveTimeline.getCommitTimeline.filterCompletedInstants.lastInstant.isPresent)
      // Verify secondary index after compaction
      updateKeys = updateDf.select("_row_key").collect().map(_.getString(0))
      validateSecondaryIndex(basePath, tableName, updateKeys)
      // Verify count of records
      assertEquals(initialRecordsCount, spark.sql(s"select _row_key from $tableName").count())

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
      validateSecondaryIndex(basePath, tableName, updateKeys)

      // Step 6: Perform Deletes on Records and Validate Secondary Index
      val deleteKeys = initialKeys.take(1) // pick a subset of keys to delete
      val deleteDf = spark.read.format("hudi").load(basePath).filter(s"_row_key in ('${deleteKeys.mkString("','")}')")
      deleteDf.write.format("hudi")
        .options(hudiOpts)
        .option(OPERATION.key, DELETE_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)
      // Verify secondary index for deletes
      validateSecondaryIndex(basePath, tableName, deleteKeys, hasDeleteKeys = true)
      // Verify for non deleted keys
      val nonDeletedKeys = initialKeys.diff(deleteKeys)
      validateSecondaryIndex(basePath, tableName, nonDeletedKeys)

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
      validateSecondaryIndex(basePath, tableName, nonDeletedKeys)
      validateSecondaryIndex(basePath, tableName, finalUpdateKeys)
      dataGen.close()
    }
  }

  test("Test Secondary Index With Overwrite and Delete Partition") {
    withTempDir { tmp =>
      Seq(
        WriteOperationType.INSERT_OVERWRITE.value(),
        WriteOperationType.INSERT_OVERWRITE_TABLE.value(),
        WriteOperationType.DELETE_PARTITION.value()
      ).foreach { operationType =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        // Step 1: Initial Insertion of Records
        val dataGen = new HoodieTestDataGenerator()
        val hudiOpts: Map[String, String] = loadInitialBatchAndCreateSecondaryIndex(tableName, basePath, dataGen)

        // Verify initial state of secondary index
        val initialKeys = spark.sql(s"select _row_key from $tableName limit 5").collect().map(_.getString(0))
        validateSecondaryIndex(basePath, tableName, initialKeys)

        // Step 3: Perform Update Operations on Subset of Records
        val records = recordsToStrings(dataGen.generateUniqueUpdates(getInstantTime, 10, HoodieTestDataGenerator.TRIP_FLATTENED_SCHEMA)).asScala
        val df = spark.read.json(spark.sparkContext.parallelize(records.toSeq, 2))
        // Verify secondary index update fails
        checkException(() => df.write.format("hudi")
          .options(hudiOpts)
          .option(OPERATION.key, operationType)
          .mode(SaveMode.Append)
          .save(basePath))(
          "Can not perform operation " + WriteOperationType.fromValue(operationType) + " on secondary index")
        // disable secondary index and retry
        df.write.format("hudi")
          .options(hudiOpts)
          .option(HoodieMetadataConfig.SECONDARY_INDEX_ENABLE_PROP.key, "false")
          .option(OPERATION.key, operationType)
          .mode(SaveMode.Append)
          .save(basePath)
        dataGen.close()
      }
    }
  }

  test("Test Secondary Index With Time Travel Query") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      // Step 1: Initial Insertion of Records
      val dataGen = new HoodieTestDataGenerator()
      val numInserts = 5
      val hudiOpts: Map[String, String] = loadInitialBatchAndCreateSecondaryIndex(tableName, basePath, dataGen, numInserts)

      // Verify initial state of secondary index
      val initialKeys = spark.sql(s"select _row_key from $tableName limit 5").collect().map(_.getString(0))
      validateSecondaryIndex(basePath, tableName, initialKeys)

      // Step 3: Perform Update Operations on Subset of Records
      val updateRecords = recordsToStrings(dataGen.generateUniqueUpdates(getInstantTime, 1, HoodieTestDataGenerator.TRIP_FLATTENED_SCHEMA)).asScala
      val updateDf = spark.read.json(spark.sparkContext.parallelize(updateRecords.toSeq, 1))
      val updateKeys = updateDf.select("_row_key").collect().map(_.getString(0))
      val recordKeyToUpdate = updateKeys.head
      val initialSecondaryKey = spark.sql(
        s"SELECT key FROM hudi_metadata('$basePath') WHERE type=7 AND key LIKE '%$SECONDARY_INDEX_RECORD_KEY_SEPARATOR$recordKeyToUpdate'"
      ).collect().map(indexKey => SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(indexKey.getString(0))).head
      // update the record
      updateDf.write.format("hudi")
        .options(hudiOpts)
        .option(OPERATION.key, UPSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)
      // Verify secondary index after updates
      validateSecondaryIndex(basePath, tableName, updateKeys)

      // Step 4: Perform Time Travel Query
      // get the first instant on the timeline
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      val firstInstant = metaClient.reloadActiveTimeline().filterCompletedInstants().firstInstant().get()
      // do a time travel query with data skipping enabled
      val readOpts = hudiOpts ++ Map(
        HoodieMetadataConfig.ENABLE.key -> "true",
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true"
      )
      val timeTravelDF = spark.read.format("hudi")
        .options(readOpts)
        .option("as.of.instant", firstInstant.requestedTime)
        .load(basePath)
      assertEquals(numInserts, timeTravelDF.count())
      // updated record should still show in time travel view
      assertEquals(1, timeTravelDF.where(s"_row_key = '$recordKeyToUpdate'").count())
      // rider field (secondary key) should point to previous value
      val secondaryKey = timeTravelDF.where(s"_row_key = '$recordKeyToUpdate'").select("rider").collect().head.getString(0)
      assertEquals(initialSecondaryKey, secondaryKey)

      // Perform Deletes on Records and Validate Secondary Index
      val deleteDf = spark.read.format("hudi").load(basePath).filter(s"_row_key in ('${updateKeys.mkString("','")}')")
      // Get fileId for the delete record
      val deleteFileId = deleteDf.select("_hoodie_file_name").collect().head.getString(0)
      deleteDf.write.format("hudi")
        .options(hudiOpts)
        .option(OPERATION.key, DELETE_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)
      // Verify secondary index for deletes
      validateSecondaryIndex(basePath, tableName, updateKeys, hasDeleteKeys = true)
      // Corrupt the data file that was written for the delete key in the first instant.
      // We are doing it to guard against a scenario where time travel query unintentionally looks up secondary index,
      // according to which the data file is supposed to be skipped. Consider following scenario:
      // 1. A record is deleted that was inserted in the first instant.
      // 2. Secondary index gets updated and as per the latest snapshot of the index should not have the deleted record.
      // 3. A time travel query is performed with data skipping enabled.
      // 4. If it was to look up the secondary index, it would have skipped the data file, but the data file is still present.
      // 5. Time travel query should throw an exception in this case.
      val firstCommitMetadata = deserializeCommitMetadata(metaClient.reloadActiveTimeline().getInstantDetails(firstInstant).get())
      val partitionToWriteStats = firstCommitMetadata.getPartitionToWriteStats.asScala.mapValues(_.asScala.toList)
      // Find the path for the given fileId
      val matchingPath: Option[String] = partitionToWriteStats.values.flatten
        .find(_.getFileId == deleteFileId)
        .map(_.getPath)
      assertTrue(matchingPath.isDefined)
      // Corrupt the data file
      val dataFile = new StoragePath(basePath, matchingPath.get)
      val storage = metaClient.getStorage
      storage.deleteFile(dataFile)
      storage.createNewFile(dataFile)
      // Time travel query should now throw an exception
      checkExceptionContain(() => spark.read.format("hudi")
        .options(readOpts)
        .option("as.of.instant", firstInstant.requestedTime)
        .load(basePath).count())(s"${dataFile.toString} is not a Parquet file")

      dataGen.close()
    }
  }

  /**
   * Test secondary index with auto generation of record keys
   */
  test("Test Secondary Index With Auto Record Key Generation") {
    withTempDir { tmp =>
      val tableName = generateTableName + s"_si_auto_keygen"
      val basePath = s"${tmp.getCanonicalPath}/$tableName"

      spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")

      spark.sql(
        s"""
           CREATE TABLE $tableName (
           |    ts LONG,
           |    id STRING,
           |    rider STRING,
           |    driver STRING,
           |    fare DOUBLE,
           |    dateDefault STRING,
           |    date STRING,
           |    city STRING,
           |    state STRING
           |) USING HUDI
           |options(
           |    type = 'mor',
           |    hoodie.metadata.enable = 'true',
           |    hoodie.enable.data.skipping = 'true',
           |    hoodie.metadata.record.index.enable = 'true',
           |    hoodie.datasource.write.payload.class = 'org.apache.hudi.common.model.OverwriteWithLatestAvroPayload'
           |)
           |PARTITIONED BY (state)
           |location '$basePath'
           |""".stripMargin)

      spark.sql("set hoodie.parquet.small.file.limit=0")
      if (HoodieSparkUtils.gteqSpark3_4) {
        spark.sql("set spark.sql.defaultColumn.enabled=false")
      }

      spark.sql(
        s"""
           |INSERT INTO $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
           |  (1695414520,'trip2','rider-C','driver-M',27.70,'2024-11-30 01:30:40', '2024-11-30', 'sunnyvale','california'),
           |  (1699349649,'trip5','rider-A','driver-Q',3.32, '2019-11-30 01:30:40', '2019-11-30', 'san_diego','texas')
           |""".stripMargin)

      // create secondary index
      spark.sql(s"CREATE INDEX idx_city on $tableName (city)")
      // validate secondary index
      var expectedSecondaryKeys = spark.sql(s"SELECT _hoodie_record_key, city from $tableName")
        .collect().map(row => SecondaryIndexKeyUtils.constructSecondaryIndexKey(row.getString(1), row.getString(0)))
      var actualSecondaryKeys = spark.sql(s"SELECT key FROM hudi_metadata('$basePath') WHERE type=7 AND key LIKE '%$SECONDARY_INDEX_RECORD_KEY_SEPARATOR%'")
        .collect().map(indexKey => indexKey.getString(0))
      assertEquals(expectedSecondaryKeys.toSet, actualSecondaryKeys.toSet)

      // update record
      spark.sql(s"UPDATE $tableName SET city = 'san_francisco' WHERE rider = 'rider-C'")
      // validate secondary index
      expectedSecondaryKeys = spark.sql(s"SELECT _hoodie_record_key, city from $tableName")
        .collect().map(row => SecondaryIndexKeyUtils.constructSecondaryIndexKey(row.getString(1), row.getString(0)))
      actualSecondaryKeys = spark.sql(s"SELECT key FROM hudi_metadata('$basePath') WHERE type=7 AND key LIKE '%$SECONDARY_INDEX_RECORD_KEY_SEPARATOR%'")
        .collect().map(indexKey => indexKey.getString(0))
      assertEquals(expectedSecondaryKeys.toSet, actualSecondaryKeys.toSet)
    }
  }

  private def loadInitialBatchAndCreateSecondaryIndex(tableName: String, basePath: String, dataGen: HoodieTestDataGenerator, numInserts: Integer = 50) = {
    val initialRecords = recordsToStrings(dataGen.generateInserts(getInstantTime, numInserts, true)).asScala
    val initialDf = spark.read.json(spark.sparkContext.parallelize(initialRecords.toSeq, 2))
    val hudiOpts = commonOpts ++ Map(TABLE_TYPE.key -> "MERGE_ON_READ", HoodieWriteConfig.TBL_NAME.key -> tableName)
    initialDf.write.format("hudi")
      .options(hudiOpts)
      .option(OPERATION.key, INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // Step 2: Create table and secondary index on 'rider' column
    spark.sql(s"CREATE TABLE $tableName USING hudi LOCATION '$basePath'")
    spark.sql(s"create index idx_rider on $tableName (rider)")
    hudiOpts
  }

  private def validateSecondaryIndex(basePath: String, tableName: String, recordKeys: Array[String], hasDeleteKeys: Boolean = false): Unit = {
    // Check secondary index metadata for the selected keys
    recordKeys.foreach { key =>
      val secondaryKeys = spark.sql(
        s"SELECT key FROM hudi_metadata('$basePath') WHERE type=7 AND key LIKE '%$SECONDARY_INDEX_RECORD_KEY_SEPARATOR$key'"
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

  private def getInstantTime: String = {
    String.format("%03d", new Integer(instantTime.incrementAndGet()))
  }

  private def createTempTableAndInsert(tableName: String, basePath: String) = {
    spark.sql(
      s"""
         |create table $tableName (
         |  ts bigint,
         |  id string,
         |  rider string,
         |  driver string,
         |  fare int,
         |  city string,
         |  state string
         |) using hudi
         | options (
         |  primaryKey ='id',
         |  type = 'mor',
         |  preCombineField = 'ts',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.record.index.enable = 'true',
         |  hoodie.metadata.index.secondary.enable = 'true',
         |  hoodie.datasource.write.recordkey.field = 'id',
         |  hoodie.datasource.write.payload.class = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
         | )
         | partitioned by(state)
         | location '$basePath'
       """.stripMargin)
    spark.sql(
      s"""
         | insert into $tableName
         | values
         | (1695159649087, '334e26e9-8355-45cc-97c6-c31daf0df330', 'rider-A', 'driver-K', 19, 'san_francisco', 'california'),
         | (1695091554787, 'e96c4396-3fad-413a-a942-4cb36106d720', 'rider-B', 'driver-M', 27, 'austin', 'texas')
         | """.stripMargin
    )
  }
}
