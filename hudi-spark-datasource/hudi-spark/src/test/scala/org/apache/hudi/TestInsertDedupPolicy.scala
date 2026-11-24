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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieReaderConfig}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.exception.{HoodieDuplicateKeyException, HoodieInsertException}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.junit.jupiter.api.{Assertions, Test}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, CsvSource, MethodSource}

class TestInsertDedupPolicy extends SparkClientFunctionalTestHarness {
  import TestInsertDedupPolicy._

  val firstInsertData: Seq[(Int, String, String, String, Double, Int)] = Seq(
    (10, "1", "rider-A", "driver-A", 19.10, 7),
    (10, "2", "rider-B", "driver-B", 27.70, 1),
    (10, "3", "rider-C", "driver-C", 33.90, 10))
  val secondInsertData: Seq[(Int, String, String, String, Double, Int)] = Seq(
    (11, "1", "rider-A", "driver-A", 1.1, 1),
    (11, "2", "rider-B", "driver-B", 2.2, 2),
    (11, "5", "rider-C", "driver-C", 3.3, 3))
  val expectedForDrop: Seq[(Int, String, String, String, Double, Int)] = Seq(
    (10, "1", "rider-A", "driver-A", 19.10, 7),
    (10, "2", "rider-B", "driver-B", 27.70, 1),
    (10, "3", "rider-C", "driver-C", 33.90, 10),
    (11, "5", "rider-C", "driver-C", 3.3, 3))
  val expectedForNone: Seq[(Int, String, String, String, Double, Int)] = Seq(
    (10, "1", "rider-A", "driver-A", 19.10, 7),
    (11, "1", "rider-A", "driver-A", 1.1, 1),
    (10, "2", "rider-B", "driver-B", 27.70, 1),
    (11, "2", "rider-B", "driver-B", 2.2, 2),
    (10, "3", "rider-C", "driver-C", 33.90, 10),
    (11, "5", "rider-C", "driver-C", 3.3, 3))

  @ParameterizedTest
  @MethodSource(Array("provideParams"))
  def testInsertLogic(tableType: String,
                      dedupPolicy: String): Unit = {
    val fgReaderOpts: Map[String, String] = Map(
      HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key -> "true",
      HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key -> "true",
      HoodieWriteConfig.RECORD_MERGE_MODE.key -> "EVENT_TIME_ORDERING",
      DataSourceWriteOptions.OPERATION.key -> INSERT_OPERATION_OPT_VAL)
    val insertDedupPolicy: Map[String, String] = Map(INSERT_DUP_POLICY.key -> dedupPolicy)
    val opts = fgReaderOpts ++ insertDedupPolicy
    val columns = Seq("ts", "key", "rider", "driver", "fare", "number")

    // Write the first batch of data.
    val inserts = spark.createDataFrame(firstInsertData).toDF(columns: _*)
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key, "key").
      option(HoodieTableConfig.ORDERING_FIELDS.key, "ts").
      option(TABLE_TYPE.key, tableType).
      option(DataSourceWriteOptions.TABLE_NAME.key, "test_table").
      option(HoodieCompactionConfig.INLINE_COMPACT.key, "false").
      option(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key, "true").
      options(opts).
      mode(SaveMode.Overwrite).
      save(basePath)

    val insertsWithDup = spark.createDataFrame(secondInsertData).toDF(columns: _*)
    if (dedupPolicy.equals(FAIL_INSERT_DUP_POLICY)) {
      // Write and check throws.
      Assertions.assertThrows(
        classOf[HoodieInsertException],
        () => insertsWithDup.write.format("hudi").options(opts).mode(SaveMode.Append).save(basePath)
      )
    } else {
      // Write data.
      insertsWithDup.write.format("hudi").
        option(HoodieMetadataConfig.SECONDARY_INDEX_ENABLE_PROP.key, "true").
        option(HoodieMetadataConfig.SECONDARY_INDEX_NAME.key, "idx_rider").
        option(HoodieMetadataConfig.SECONDARY_INDEX_COLUMN.key, "rider").
        options(opts).
        mode(SaveMode.Append).
        save(basePath)
      // Validate the data.
      val df = spark.read.format("hudi").options(opts).load(basePath)
      val finalDf = df.select("ts", "key", "rider", "driver", "fare", "number").sort("key")
      val expected = if (dedupPolicy.equals(DROP_INSERT_DUP_POLICY)) expectedForDrop else expectedForNone
      val expectedDf = spark.createDataFrame(expected).toDF(columns: _*).sort("key")
      TestInsertDedupPolicy.validate(expectedDf, finalDf)

      // Validate data with record key filter
      // RLI will be used with record key filter
      val recordKeyFilterDf = df.filter("key = '5'").select("ts", "key", "rider", "driver", "fare", "number").sort("key")
      val expectedRecordKeyFilter = if (dedupPolicy.equals(DROP_INSERT_DUP_POLICY)) expectedForDrop.filter(_._2 == "5") else expectedForNone.filter(_._2 == "5")
      val expectedRecordKeyFilterDf = spark.createDataFrame(expectedRecordKeyFilter).toDF(columns: _*).sort("key")
      TestInsertDedupPolicy.validate(expectedRecordKeyFilterDf, recordKeyFilterDf)

      // Validate data with secondary key filter
      // Secondary index will be used with secondary key filter
      val secondaryKeyFilterDf = df.filter("rider = 'rider-C'").select("ts", "key", "rider", "driver", "fare", "number").sort("key")
      val expectedSecondaryKeyFilter = if (dedupPolicy.equals(DROP_INSERT_DUP_POLICY)) expectedForDrop.filter(_._3 == "rider-C") else expectedForNone.filter(_._3 == "rider-C")
      val expectedSecondaryKeyFilterDf = spark.createDataFrame(expectedSecondaryKeyFilter).toDF(columns: _*).sort("key")
      TestInsertDedupPolicy.validate(expectedSecondaryKeyFilterDf, secondaryKeyFilterDf)
    }
  }

  /**
   * Without a policy, a record key that repeats among the inserted records fails an insert into a table of a
   * non-native format, while one the table holds is looked up only through the record level index, which the test
   * format does not serve; the fail policy checks the table. A native table keeps the duplicates.
   */
  @ParameterizedTest
  @CsvSource(Array("COPY_ON_WRITE,SIMPLE", "MERGE_ON_READ,SIMPLE"))
  def testDuplicateKeysFailInsertIntoTableWithRecordKeyByDefault(tableType: String, indexType: String): Unit = {
    val keyed = s"$basePath/keyed"
    val keyless = s"$basePath/keyless"
    val key = Some("id")
    val indexOpts = Map(
      HoodieTableConfig.TABLE_FORMAT.key -> TEST_TABLE_FORMAT,
      HoodieIndexConfig.INDEX_TYPE.key -> indexType,
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> (indexType == "RECORD_INDEX").toString)
    def write(path: String, recordKey: Option[String], rows: Seq[(Int, String, Long)],
              extraOpts: Map[String, String] = Map.empty, operation: String = INSERT_OPERATION_OPT_VAL): Unit =
      spark.createDataFrame(rows).toDF("id", "name", "ts").write.format("hudi")
        .option(TABLE_NAME.key, "test_table")
        .option(TABLE_TYPE.key, tableType)
        .option(OPERATION.key, operation)
        .options(recordKey.map(RECORDKEY_FIELD.key -> _).toMap ++ extraOpts)
        .mode(SaveMode.Append)
        .save(path)
    def assertDuplicateKey(duplicateKey: String, detail: String, failingWrite: => Unit): Unit = {
      val root = HoodieTestUtils.getRootCause(Assertions.assertThrows(classOf[Throwable], () => failingWrite))
      assertTrue(root.isInstanceOf[HoodieDuplicateKeyException], s"Expected a duplicate key failure, got: $root")
      assertTrue(root.getMessage.startsWith(s"Duplicate key found for insert statement, key is: $duplicateKey. $detail"), root.getMessage)
    }
    def rows(path: String): Seq[(Int, String, Long)] = spark.read.format("hudi").load(path)
      .select("id", "name", "ts").collect().map(r => (r.getInt(0), r.getString(1), r.getLong(2))).sorted.toSeq

    assertDuplicateKey("1", DUPLICATE_INCOMING_KEY, write(keyed, key, Seq((1, "a1", 10L), (1, "a2", 11L)), indexOpts))
    write(keyed, key, Seq((1, "a1", 10L)), indexOpts)
    assertDuplicateKey("1", DUPLICATE_EXISTING_KEY,
      write(keyed, key, Seq((1, "a2", 11L)), indexOpts + (INSERT_DUP_POLICY.key -> FAIL_INSERT_DUP_POLICY)))
    val unchecked = s"$basePath/unchecked"
    write(unchecked, key, Seq((1, "a1", 10L)), indexOpts)
    write(unchecked, key, Seq((1, "a2", 11L)), indexOpts)
    Assertions.assertEquals(Seq((1, "a1", 10L), (1, "a2", 11L)), rows(unchecked))
    assertDuplicateKey("2", DUPLICATE_INCOMING_KEY, write(keyed, key, Seq((2, "b1", 20L), (3, "c1", 30L), (2, "b2", 21L)), indexOpts))
    Assertions.assertEquals(Seq((1, "a1", 10L)), rows(keyed))

    // An explicit policy wins, and an upsert is not turned into an insert by the default policy
    write(keyed, key, Seq((1, "a2", 11L), (2, "b1", 20L)), indexOpts + (INSERT_DUP_POLICY.key -> DROP_INSERT_DUP_POLICY))
    write(keyed, key, Seq((2, "b2", 21L)), indexOpts, UPSERT_OPERATION_OPT_VAL)
    Assertions.assertEquals(Seq((1, "a1", 10L), (2, "b2", 21L)), rows(keyed))

    val keptDuplicates = Seq((1, "a1", 10L), (1, "a2", 11L), (1, "a3", 12L))
    val keyedWithoutPolicy = s"$basePath/keyed_without_policy"
    val nonePolicy = Map(INSERT_DUP_POLICY.key -> NONE_INSERT_DUP_POLICY)
    write(keyedWithoutPolicy, key, Seq((1, "a1", 10L), (1, "a2", 11L)), indexOpts ++ nonePolicy)
    write(keyedWithoutPolicy, key, Seq((1, "a3", 12L)), indexOpts ++ nonePolicy)
    Assertions.assertEquals(keptDuplicates, rows(keyedWithoutPolicy))

    write(keyless, None, Seq((1, "a1", 10L), (1, "a2", 11L)), indexOpts)
    write(keyless, None, Seq((1, "a3", 12L)), indexOpts)
    Assertions.assertEquals(keptDuplicates, rows(keyless))

    val native = s"$basePath/native"
    val nativeOpts = indexOpts - HoodieTableConfig.TABLE_FORMAT.key
    write(native, key, Seq((1, "a1", 10L), (1, "a2", 11L)), nativeOpts)
    write(native, key, Seq((1, "a3", 12L)), nativeOpts)
    Assertions.assertEquals(keptDuplicates, rows(native))
  }

  /**
   * The duplicate key check leaves the inserted records in their order, so a base file holds them in the order
   * of the insert, into an empty table and into one whose records the check looks up. The check and the write
   * read the inserted records once.
   */
  @ParameterizedTest
  @CsvSource(Array("COPY_ON_WRITE,SIMPLE,", "COPY_ON_WRITE,GLOBAL_SIMPLE,", "COPY_ON_WRITE,BLOOM,",
    "MERGE_ON_READ,SIMPLE,", "COPY_ON_WRITE,SIMPLE,fail", "MERGE_ON_READ,SIMPLE,fail"))
  def testDuplicateKeyCheckKeepsOrderOfInsertedRecords(tableType: String, indexType: String, dedupPolicy: String): Unit = {
    val opts = Map(
      HoodieTableConfig.TABLE_FORMAT.key -> TEST_TABLE_FORMAT,
      HoodieIndexConfig.INDEX_TYPE.key -> indexType,
      HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> (indexType == "RECORD_INDEX").toString,
      // every insert writes a new base file
      HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key -> "0") ++ Option(dedupPolicy).map(INSERT_DUP_POLICY.key -> _)
    val reads = spark.sparkContext.longAccumulator
    def insert(ids: Seq[Int]): Unit = {
      reads.reset()
      val rows = spark.sparkContext.parallelize(ids.map(id => (id, s"name$id", id.toLong)), 1).map { row =>
        reads.add(1)
        row
      }
      spark.createDataFrame(rows).toDF("id", "name", "ts")
        .write.format("hudi")
        .option(TABLE_NAME.key, "test_table")
        .option(TABLE_TYPE.key, tableType)
        .option(OPERATION.key, INSERT_OPERATION_OPT_VAL)
        .option(RECORDKEY_FIELD.key, "id")
        .options(opts)
        .mode(SaveMode.Append)
        .save(basePath)
      Assertions.assertEquals(ids.size, reads.value)
    }
    def idsInBaseFileOf(id: Int): Seq[Int] = {
      val baseFile = spark.read.format("hudi").load(basePath).where(s"id = $id").select("_hoodie_file_name").head.getString(0)
      spark.read.parquet(s"$basePath/$baseFile").select("id").collect().map(_.getInt(0)).toSeq
    }

    // two permutations of 40 ids each, neither in the order of the ids
    val firstIds = (1 to 40).map(i => i * 17 % 41)
    val secondIds = (1 to 40).map(i => 100 + i * 23 % 41)
    insert(firstIds)
    Assertions.assertEquals(firstIds, idsInBaseFileOf(firstIds.head))
    insert(secondIds)
    Assertions.assertEquals(secondIds, idsInBaseFileOf(secondIds.head))
  }

  /**
   * Until the record index of a merge-on-read table is built, the index falls back to one that reads the keys of the
   * base files, which does not see a delete in a log file, so an insert of a deleted key must not fail by default.
   */
  @Test
  def testInsertOfDeletedKeyIntoMergeOnReadTableBeforeRecordIndexIsBuilt(): Unit = {
    val path = s"$basePath/mor_record_index_not_built"
    def write(rows: Seq[(Int, String, Long)], operation: String): Unit =
      spark.createDataFrame(rows).toDF("id", "name", "ts").write.format("hudi")
        .option(TABLE_NAME.key, "test_table")
        .option(TABLE_TYPE.key, "MERGE_ON_READ")
        .option(OPERATION.key, operation)
        .option(RECORDKEY_FIELD.key, "id")
        .option(HoodieTableConfig.TABLE_FORMAT.key, TEST_TABLE_FORMAT)
        .option(HoodieIndexConfig.INDEX_TYPE.key, "RECORD_INDEX")
        .option(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key, "false")
        .option(HoodieCompactionConfig.INLINE_COMPACT.key, "false")
        .mode(SaveMode.Append)
        .save(path)

    write(Seq((1, "a1", 10L), (2, "b1", 20L)), INSERT_OPERATION_OPT_VAL)
    write(Seq((1, "a1", 10L)), DELETE_OPERATION_OPT_VAL)
    write(Seq((1, "a2", 11L)), INSERT_OPERATION_OPT_VAL)
    Assertions.assertEquals(Seq((1, "a2", 11L), (2, "b1", 20L)), spark.read.format("hudi").load(path)
      .select("id", "name", "ts").collect().map(r => (r.getInt(0), r.getString(1), r.getLong(2))).sorted.toSeq)
  }
}

object TestInsertDedupPolicy {
  val DUPLICATE_EXISTING_KEY = "The table already has a record with this key."
  val DUPLICATE_INCOMING_KEY = "More than one inserted record has this key."
  // A non-native table format, whose tables fail an insert of duplicate record keys by default
  val TEST_TABLE_FORMAT = "test-format"

  def provideParams(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      Arguments.of("MERGE_ON_READ", NONE_INSERT_DUP_POLICY),
      Arguments.of("MERGE_ON_READ", DROP_INSERT_DUP_POLICY),
      Arguments.of("MERGE_ON_READ", FAIL_INSERT_DUP_POLICY),
      Arguments.of("COPY_ON_WRITE", NONE_INSERT_DUP_POLICY),
      Arguments.of("COPY_ON_WRITE", DROP_INSERT_DUP_POLICY),
      Arguments.of("COPY_ON_WRITE", FAIL_INSERT_DUP_POLICY)
    )
  }

  def validate(expectedDf: Dataset[Row], actualDf: Dataset[Row]): Unit = {
    val expectedMinusActual = expectedDf.except(actualDf)
    val actualMinusExpected = actualDf.except(expectedDf)
    assertTrue(expectedMinusActual.isEmpty && actualMinusExpected.isEmpty)
  }
}


