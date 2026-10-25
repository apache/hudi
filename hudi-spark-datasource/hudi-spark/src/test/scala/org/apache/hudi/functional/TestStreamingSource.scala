/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.{DataSourceReadOptions, HoodieSparkUtils}
import org.apache.hudi.DataSourceReadOptions.{START_OFFSET, STREAMING_READ_TABLE_VERSION}
import org.apache.hudi.DataSourceWriteOptions.{ORDERING_FIELDS, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.config.HoodieReaderConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.model.HoodieTableType.{COPY_ON_WRITE, MERGE_ON_READ}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieCompactionConfig}
import org.apache.hudi.config.HoodieWriteConfig.{DELETE_PARALLELISM_VALUE, INSERT_PARALLELISM_VALUE, TBL_NAME, UPSERT_PARALLELISM_VALUE, WRITE_TABLE_VERSION}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.util.JavaConversions

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.streaming.StreamTest
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

class TestStreamingSource extends StreamTest {

  import testImplicits._
  protected val commonOptions: Map[String, String] = Map(
    RECORDKEY_FIELD.key -> "id",
    HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
    INSERT_PARALLELISM_VALUE.key -> "4",
    UPSERT_PARALLELISM_VALUE.key -> "4",
    DELETE_PARALLELISM_VALUE.key -> "4"
  )
  private val columns = Seq("id", "name", "price", "ts")

  org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)

  override protected def sparkConf = {
    super.sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
  }

  test("test cow stream source") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_cow_stream"
      HoodieTableMetaClient.newTableBuilder()
        .setTableType(COPY_ON_WRITE)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setOrderingFields("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      val df = spark.readStream
        .format("org.apache.hudi")
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("1", "a1", "10", "000")), lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath, Seq(("1", "a1", "12", "000"))),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("1", "a1", "12", "000")), lastOnly = true, isSorted = false),

        addDataToQuery(tablePath,
          Seq(("2", "a2", "12", "000"),
            ("3", "a3", "12", "000"),
            ("4", "a4", "12", "000"))),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(
          Seq(Row("2", "a2", "12", "000"),
            Row("3", "a3", "12", "000"),
            Row("4", "a4", "12", "000")),
          lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath, Seq(("5", "a5", "12", "000"))),
        addDataToQuery(tablePath, Seq(("6", "a6", "12", "000"))),
        addDataToQuery(tablePath, Seq(("5", "a5", "15", "000"))),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(
          Seq(Row("6", "a6", "12", "000"),
            Row("5", "a5", "15", "000")),
          lastOnly = true, isSorted = false)
      )
    }
  }

  test("test mor stream source") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_mor_stream"
      HoodieTableMetaClient.newTableBuilder()
        .setTableType(MERGE_ON_READ)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setOrderingFields("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      val df = spark.readStream
        .format("org.apache.hudi")
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("1", "a1", "10", "000")), lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath,
          Seq(("2", "a2", "12", "000"),
            ("3", "a3", "12", "000"),
            ("2", "a2", "10", "001"))),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(
          Seq(Row("3", "a3", "12", "000"),
            Row("2", "a2", "10", "001")),
          lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath, Seq(("5", "a5", "12", "000"))),
        addDataToQuery(tablePath, Seq(("6", "a6", "12", "000"))),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(
          Seq(Row("5", "a5", "12", "000"),
            Row("6", "a6", "12", "000")),
          lastOnly = true, isSorted = false)
      )
    }
  }

  test("Test cow from latest offset") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_cow_stream"
      HoodieTableMetaClient.newTableBuilder()
        .setTableType(COPY_ON_WRITE)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setOrderingFields("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      val df = spark.readStream
        .format("org.apache.hudi")
        .option(START_OFFSET.key(), "latest")
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        // Start from the latest, should contains no data
        CheckAnswerRows(Seq(), lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath, Seq(("2", "a1", "12", "000"))),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("2", "a1", "12", "000")), lastOnly = false, isSorted = false)
      )
    }
  }

  test("Test cow from specified offset") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_cow_stream"
      val metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(COPY_ON_WRITE)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setOrderingFields("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      addData(tablePath, Seq(("2", "a1", "11", "001")))
      addData(tablePath, Seq(("3", "a1", "12", "002")))

      val timestamp =
        metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants()
          .firstInstant().get().getCompletionTime

      val df = spark.readStream
        .format("org.apache.hudi")
        .option(START_OFFSET.key(), timestamp)
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        // Start after the first commit
        CheckAnswerRows(Seq(Row("2", "a1", "11", "001"), Row("3", "a1", "12", "002")), lastOnly = true, isSorted = false)
      )
    }
  }

  test("Test mor streaming source with clustering") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_mor_stream_cluster"
      val metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(MERGE_ON_READ)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setOrderingFields("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      addData(tablePath, Seq(("2", "a1", "11", "001")))
      addData(tablePath, Seq(("3", "a1", "12", "002")))
      addData(tablePath, Seq(("4", "a1", "13", "003")), enableInlineCluster = true)
      addData(tablePath, Seq(("5", "a1", "14", "004")))

      val timestamp =
        metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants()
          .firstInstant().get().getCompletionTime

      val df = spark.readStream
        .format("org.apache.hudi")
        .option(START_OFFSET.key(), timestamp)
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        // Start after the first commit
        CheckAnswerRows(Seq(
          Row("2", "a1", "11", "001"),
          Row("3", "a1", "12", "002"),
          Row("4", "a1", "13", "003"),
          Row("5", "a1", "14", "004")), lastOnly = true, isSorted = false))
      assertTrue(metaClient.reloadActiveTimeline
        .filter(JavaConversions.getPredicate(
          e => e.isCompleted && HoodieTimeline.REPLACE_COMMIT_ACTION.equals(e.getAction)))
        .countInstants() > 0)
    }
  }

  test("test mor stream source with compaction") {
    Array("true", "false").foreach(skipCompact => {
      withTempDir { inputDir =>
        val tablePath = s"${inputDir.getCanonicalPath}/test_mor_stream_$skipCompact"
        val metaClient = HoodieTableMetaClient.newTableBuilder()
          .setTableType(MERGE_ON_READ)
          .setTableName(getTableName(tablePath))
          .setRecordKeyFields("id")
          .setOrderingFields("ts")
          .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

        addData(tablePath, Seq(("1", "a1", "10", "000")))
        val df = spark.readStream
          .format("org.apache.hudi")
          .option(DataSourceReadOptions.INCREMENTAL_READ_SKIP_COMPACT.key(), skipCompact)
          .load(tablePath)
          .select("id", "name", "price", "ts")

        addData(tablePath,
          Seq(("1", "a2", "12", "000"),
            ("2", "a3", "12", "000")))
        addData(tablePath, Seq(("2", "a5", "12", "000"), ("1", "a6", "12", "001")))
        // trigger compaction
        addData(tablePath, Seq(("3", "a6", "12", "000")), enableInlineCompaction = true)

        testStream(df)(
          AssertOnQuery {q => q.processAllAvailable(); true },
          CheckAnswerRows(Seq(Row("1", "a6", "12", "001"),
            Row("2", "a5", "12", "000"),
            Row("3", "a6", "12", "000")), lastOnly = true, isSorted = false),
          StopStream
        )
        assertTrue(metaClient.reloadActiveTimeline
          .filter(JavaConversions.getPredicate(
            e => e.isCompleted && HoodieTimeline.COMMIT_ACTION.equals(e.getAction)))
          .countInstants() > 0)
      }
    })
  }

  /**
   * Exercises the legacy incremental streaming path in [[HoodieStreamSourceV1]] (table version
   * below EIGHT) and [[HoodieStreamSourceV2]] (table version EIGHT and above), taken when the file
   * group reader is disabled. This drives the standalone incremental relations
   * ([[IncrementalRelationV1]] / [[MergeOnReadIncrementalRelationV1]] for version 6,
   * [[IncrementalRelationV2]] / [[MergeOnReadIncrementalRelationV2]] for version 8) via `getBatch`,
   * rather than the newer HadoopFsRelation factory path.
   */
  private def testLegacyIncrementalStreamSource(tableType: HoodieTableType,
                                                tableVersion: HoodieTableVersion): Unit = {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_${tableType.name}_v${tableVersion.versionCode}_legacy_stream"
      HoodieTableMetaClient.newTableBuilder()
        .setTableType(tableType)
        .setTableName(getTableName(tablePath))
        .setTableVersion(tableVersion)
        .setRecordKeyFields("id")
        .setOrderingFields("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")), tableVersion = tableVersion)
      val df = spark.readStream
        .format("org.apache.hudi")
        .option(WRITE_TABLE_VERSION.key, tableVersion.versionCode().toString)
        .option(STREAMING_READ_TABLE_VERSION.key, tableVersion.versionCode().toString)
        // force the legacy (non file-group-reader) incremental relation path
        .option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key, "false")
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("1", "a1", "10", "000")), lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath,
          Seq(("2", "a2", "12", "000"),
            ("3", "a3", "12", "000")),
          tableVersion = tableVersion),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        // The legacy branch of getBatch materializes the micro batch from an RDD via
        // internalCreateDataFrame, so the physical plan is a "Scan ExistingRDD"; this fails if the
        // legacy branch is dropped and the source silently falls back to the file group reader
        // path, which scans a HadoopFsRelation ("FileScan" / "Scan parquet") instead.
        AssertOnQuery { q =>
          val plan = q.lastExecution.executedPlan.toString
          assertTrue(plan.contains("Scan ExistingRDD"),
            "expected the legacy RDD-backed incremental batch, but got plan: " + plan)
          assertTrue(!plan.contains("FileScan"),
            "expected no file-group-reader HadoopFsRelation scan, but got plan: " + plan)
          true
        },
        CheckAnswerRows(
          Seq(Row("2", "a2", "12", "000"),
            Row("3", "a3", "12", "000")),
          lastOnly = true, isSorted = false),
        StopStream,

        // inline clustering on this write lands a replacecommit inside the next getBatch span, which
        // reaches the replaced-file-group filtering of the legacy incremental relations
        addDataToQuery(tablePath, Seq(("4", "a4", "13", "000")), enableInlineCluster = true,
          tableVersion = tableVersion),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("4", "a4", "13", "000")), lastOnly = true, isSorted = false)
      )
    }
  }

  test("test cow stream source with legacy file group reader disabled") {
    testLegacyIncrementalStreamSource(COPY_ON_WRITE, HoodieTableVersion.SIX)
  }

  test("test mor stream source with legacy file group reader disabled") {
    testLegacyIncrementalStreamSource(MERGE_ON_READ, HoodieTableVersion.SIX)
  }

  test("test cow stream source with legacy file group reader disabled on table version 8") {
    testLegacyIncrementalStreamSource(COPY_ON_WRITE, HoodieTableVersion.EIGHT)
  }

  test("test mor stream source with legacy file group reader disabled on table version 8") {
    testLegacyIncrementalStreamSource(MERGE_ON_READ, HoodieTableVersion.EIGHT)
  }

  /**
   * #19578: with the file group reader disabled, MOR streaming batches materialize through
   * [[HoodieMergeOnReadRDDV2]], the only user-facing path reading shredded variant base files
   * without a catalyst schema. The first stream covers the base-only split (first batch) and the
   * log-only split (second batch); the second stream covers the merged base + log split.
   *
   * The table carries a variant one struct member down, force-shredded exactly like a top-level
   * one. With a top-level `v` beside it the base-only split is re-routed to the file group reader
   * (HoodieMergeOnReadRDDV2.shouldRerouteVariantSplit), while the nested-only leg keeps that split
   * on requiredSchemaReaderSkipMerging - Spark's own parquet reader with a native VariantType
   * request one struct member down, which the Spark 4.1+ parquet reader reconstructs out of the
   * shredded group; the vectorized one at stock settings, as the legacy file format inherits
   * ParquetFileFormat.supportBatch and VariantType is atomic (#19775). That second leg is the one
   * HoodieMergeOnReadRDDV2's comment relies on.
   */
  private def testLegacyShreddedVariantStream(withTopLevelVariant: Boolean): Unit = {
    assume(HoodieSparkUtils.gteqSpark4_1, "Shredded variant base-file read requires Spark 4.1 or higher")

    withTempDir { inputDir =>
      val suffix = if (withTopLevelVariant) "" else "_nested_only"
      val tablePath = s"${inputDir.getCanonicalPath}/test_mor_variant_legacy_stream$suffix"
      HoodieTableMetaClient.newTableBuilder()
        .setTableType(MERGE_ON_READ)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setOrderingFields("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      // Whether the top-level `v` column exists at all is decided here and nowhere else: on the
      // nested-only leg the table is (id, s, ts) and every write, projection and expected row
      // below drops it.
      def rowSql(id: Int, topLevelKey: String, nestedKey: String, ts: Long): String = {
        val topLevelCol = if (withTopLevelVariant) s"""parse_json('{"key":"$topLevelKey"}') as v, """ else ""
        s"""select $id as id, ${topLevelCol}named_struct('inner', """ +
          s"""parse_json('{"key":"$nestedKey"}')) as s, ${ts}L as ts"""
      }

      def expectedRow(id: Int, topLevelKey: String, nestedKey: String, ts: Long): Row = {
        val topLevel = if (withTopLevelVariant) Seq(s"""{"key":"$topLevelKey"}""") else Seq.empty
        Row((Seq[Any](id) ++ topLevel ++ Seq(s"""{"key":"$nestedKey"}""", ts)): _*)
      }

      // INMEMORY index routes MOR inserts to log files, so the first base file is the
      // compaction's SHREDDED one; compact = true trips inline compaction on that write.
      def addVariantData(valuesSql: String, compact: Boolean): Unit = {
        // The write must use the short name: it resolves to Spark4DefaultSource, the only
        // provider overriding CreatableRelationProvider.supportsDataType to accept VariantType.
        // The fully qualified "org.apache.hudi" resolves to DefaultSource (short name "hudi_v1"),
        // which has no override, so DataSource.planForWriting on Spark 4.x rejects the variant
        // column with UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE.
        spark.sql(valuesSql).write.format("hudi")
          .options(commonOptions)
          .option(TBL_NAME.key, getTableName(tablePath))
          .option(TABLE_TYPE.key, MERGE_ON_READ.name)
          .option("hoodie.index.type", "INMEMORY")
          .option("hoodie.parquet.variant.write.shredding.enabled", "true")
          .option("hoodie.parquet.variant.force.shredding.schema.for.test", "key string")
          .option(HoodieCompactionConfig.INLINE_COMPACT.key, compact.toString)
          .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key, "2")
          .mode(SaveMode.Append)
          .save(tablePath)
      }

      // The read keeps the fully qualified name: it goes through StreamSourceProvider, which
      // never calls supportsDataType.
      def variantStreamDf(): DataFrame = {
        val topLevel = if (withTopLevelVariant) Seq("cast(v as string) as v") else Seq.empty
        spark.readStream
          .format("org.apache.hudi")
          // force the legacy (non file-group-reader) incremental relation path
          .option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key, "false")
          .load(tablePath)
          .selectExpr((Seq("id") ++ topLevel ++ Seq("cast(s.inner as string) as inner", "ts")): _*)
      }

      // The legacy branch of getBatch materializes the micro batch from an RDD via
      // internalCreateDataFrame, so the physical plan is a "Scan ExistingRDD"; this fails if the
      // legacy branch is dropped and the source silently falls back to the file group reader
      // path, which scans a HadoopFsRelation ("FileScan" / "Scan parquet") instead.
      val assertLegacyRddPlan = AssertOnQuery { q =>
        val plan = q.lastExecution.executedPlan.toString
        assertTrue(plan.contains("Scan ExistingRDD"),
          "expected the legacy RDD-backed incremental batch, but got plan: " + plan)
        assertTrue(!plan.contains("FileScan"),
          "expected no file-group-reader HadoopFsRelation scan, but got plan: " + plan)
        true
      }

      addVariantData(rowSql(1, "v1", "n1", 1000L), compact = false)
      addVariantData(rowSql(2, "v2", "n2", 1000L), compact = true)
      // Pin that the compacted base file is shredded at every depth the leg carries: without it
      // the streams below would pass just the same over an unshredded base and pin nothing about
      // shredded reads. The listing goes through FSUtils.isBaseFile rather than a ".parquet"
      // suffix test, which a native parquet log file (<fileId>_<token>_<instant>_<v>.log.parquet)
      // also matches - so the assertion would hold even if inline compaction never ran.
      val conf = spark.sessionState.newHadoopConf()
      val baseFiles = new Path(tablePath).getFileSystem(conf).listStatus(new Path(tablePath))
        .map(_.getPath).filter(p => FSUtils.isBaseFile(p.getName))
      assertTrue(baseFiles.nonEmpty,
        "expected a compacted BASE file under " + tablePath + " (log files excluded)")
      baseFiles.foreach { file =>
        val reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf))
        val footer = try reader.getFooter.getFileMetaData.getSchema finally reader.close()
        // getFieldIndex + getType(int): the String overload of getType is ambiguous from Scala.
        val s = footer.getType(footer.getFieldIndex("s")).asGroupType()
        val inner = s.getType(s.getFieldIndex("inner")).asGroupType()
        assertTrue(inner.containsField("typed_value"), "s.inner must be shredded in " + file + ":\n" + footer)
        if (withTopLevelVariant) {
          val v = footer.getType(footer.getFieldIndex("v")).asGroupType()
          assertTrue(v.containsField("typed_value"), "v must be shredded in " + file + ":\n" + footer)
        } else {
          // The absence of `v` is what keeps shouldRerouteVariantSplit false on this leg.
          assertTrue(!footer.containsField("v"),
            "the nested-only leg must not carry a top-level v in " + file + ":\n" + footer)
        }
      }

      testStream(variantStreamDf())(
        // Base-only split: this batch spans both deltacommits and the compaction commit, whose
        // affected files resolve to the compacted shredded base file with no log on top. With a
        // top-level `v` this is the split shouldRerouteVariantSplit sends to the file group
        // reader; without one it stays on requiredSchemaReaderSkipMerging, the leg that pins the
        // nested-shredded base read by Spark's own parquet reader.
        AssertOnQuery { q => q.processAllAvailable(); true },
        assertLegacyRddPlan,
        CheckAnswerRows(Seq(
          expectedRow(1, "v1", "n1", 1000L),
          expectedRow(2, "v2", "n2", 1000L)),
          lastOnly = true, isSorted = false),
        StopStream,

        // Log-only split: MergeOnReadIncrementalRelationV2 builds its file system view out of the
        // span's affected files alone, and this span covers only the update deltacommit, so the
        // slice is the appended log file with no base file.
        AssertOnQuery { _ =>
          addVariantData(rowSql(1, "v1-updated", "n1-updated", 1001L), compact = false)
          true
        },
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(expectedRow(1, "v1-updated", "n1-updated", 1001L)),
          lastOnly = true, isSorted = false)
      )

      // Merged split: a fresh testStream over a fresh streaming DataFrame gets its own
      // checkpoint, so it replays from the INIT offset and its first batch spans the compaction
      // commit and the update deltacommit together. Only such a span puts the shredded base file
      // and the update log file in one affected-file list, giving the base + log slice that
      // neither batch above produces.
      testStream(variantStreamDf())(
        AssertOnQuery { q => q.processAllAvailable(); true },
        assertLegacyRddPlan,
        CheckAnswerRows(Seq(
          expectedRow(1, "v1-updated", "n1-updated", 1001L),
          expectedRow(2, "v2", "n2", 1000L)),
          lastOnly = true, isSorted = false)
      )
    }
  }

  test("test mor stream source reads shredded variant with legacy file group reader disabled") {
    testLegacyShreddedVariantStream(withTopLevelVariant = true)
  }

  test("test mor stream source reads a nested-only shredded variant with legacy file group reader disabled") {
    testLegacyShreddedVariantStream(withTopLevelVariant = false)
  }

  private def testCheckpointTranslation(tableName: String,
                                        tableType: HoodieTableType,
                                        writeTableVersion: HoodieTableVersion,
                                        streamingReadVersions: List[Int]): Unit = {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/$tableName"
      val metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(tableType)
        .setTableName(getTableName(tablePath))
        .setTableVersion(writeTableVersion)
        .setRecordKeyFields("id")
        .setOrderingFields("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      // Add initial data
      addData(tablePath, Seq(("1", "a1", "10", "000")), tableVersion = writeTableVersion)
      addData(tablePath, Seq(("2", "a1", "11", "001")), tableVersion = writeTableVersion)
      addData(tablePath, Seq(("3", "a1", "12", "002")), tableVersion = writeTableVersion)

      // Add update for MOR tests
      if (tableType == MERGE_ON_READ) {
        addData(tablePath, Seq(("2", "a2_updated", "16", "003")), tableVersion = writeTableVersion)
      }

      val instants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants.getInstants
      val expectedInstantCount = if (tableType == MERGE_ON_READ) 4 else 3
      assertEquals(expectedInstantCount, instants.size())

      val startTimestampIndex = if (tableType == MERGE_ON_READ) 2 else 1
      val startTimestamp = instants.get(startTimestampIndex).requestedTime

      for (streamingReadTableVersion <- streamingReadVersions) {
        val df = spark.readStream
          .format("org.apache.hudi")
          .option(START_OFFSET.key, startTimestamp)
          .option(WRITE_TABLE_VERSION.key, writeTableVersion.versionCode().toString)
          .option(STREAMING_READ_TABLE_VERSION.key, streamingReadTableVersion.toString)
          .load(tablePath)
          .select("id", "name", "price", "ts")

        val expectedRows = if (tableType == MERGE_ON_READ) {
          if (streamingReadTableVersion == HoodieTableVersion.current().versionCode()) {
            Seq(Row("3", "a1", "12", "002"), Row("2", "a2_updated", "16", "003"))
          } else {
            Seq(Row("2", "a2_updated", "16", "003"))
          }
        } else {
          if (streamingReadTableVersion == HoodieTableVersion.current().versionCode()) {
            Seq(Row("2", "a1", "11", "001"), Row("3", "a1", "12", "002"))
          } else {
            Seq(Row("3", "a1", "12", "002"))
          }
        }

        testStream(df)(
          AssertOnQuery { q => q.processAllAvailable(); true },
          CheckAnswerRows(expectedRows, lastOnly = true, isSorted = false)
        )
      }
    }
  }

  test("Test checkpoint translation on COW table") {
    testCheckpointTranslation(
      "test_cow_stream_ckpt",
      COPY_ON_WRITE,
      HoodieTableVersion.current(),
      List(HoodieTableVersion.SIX.versionCode(), HoodieTableVersion.current().versionCode())
    )
  }

  test("Test checkpoint translation on MOR table") {
    testCheckpointTranslation(
      "test_mor_stream_ckpt",
      MERGE_ON_READ,
      HoodieTableVersion.current(),
      List(HoodieTableVersion.SIX.versionCode(), HoodieTableVersion.current().versionCode())
    )
  }

  test("Test checkpoint translation on COW table with table version 6") {
    testCheckpointTranslation(
      "test_cow_stream_ckpt_v6",
      COPY_ON_WRITE,
      HoodieTableVersion.SIX,
      List(HoodieTableVersion.SIX.versionCode())
    )
  }

  test("Test checkpoint translation on MOR table with table version 6") {
    testCheckpointTranslation(
      "test_mor_stream_ckpt_v6",
      MERGE_ON_READ,
      HoodieTableVersion.SIX,
      List(HoodieTableVersion.SIX.versionCode())
    )
  }

  private def addData(inputPath: String,
                      rows: Seq[(String, String, String, String)],
                      enableInlineCompaction: Boolean = false,
                      enableInlineCluster: Boolean = false,
                      tableVersion: HoodieTableVersion = HoodieTableVersion.current) : Unit = {
    rows.toDF(columns: _*)
      .write
      .format("org.apache.hudi")
      .options(commonOptions)
      .option(TBL_NAME.key, getTableName(inputPath))
      .option(WRITE_TABLE_VERSION.key, tableVersion.versionCode().toString)
      .option(HoodieCompactionConfig.INLINE_COMPACT.key(), enableInlineCompaction.toString)
      .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "2")
      .option(HoodieClusteringConfig.INLINE_CLUSTERING.key(), enableInlineCluster.toString)
      .option(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2")
      .option(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key, "0")
      .mode(SaveMode.Append)
      .save(inputPath)
  }

  private def addDataToQuery(inputPath: String,
                             rows: Seq[(String, String, String, String)],
                             enableInlineCluster: Boolean = false,
                             tableVersion: HoodieTableVersion = HoodieTableVersion.current): AssertOnQuery = {
    AssertOnQuery { _=>
      addData(inputPath, rows, enableInlineCluster = enableInlineCluster, tableVersion = tableVersion)
      true
    }
  }

  private def getTableName(inputPath: String): String = {
    val start = inputPath.lastIndexOf('/')
    inputPath.substring(start + 1)
  }
}
