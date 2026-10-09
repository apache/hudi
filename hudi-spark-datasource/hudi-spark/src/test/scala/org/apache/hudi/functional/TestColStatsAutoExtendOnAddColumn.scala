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

import org.apache.hudi.{ColumnStatsIndexSupport, DataSourceWriteOptions, HoodieSchemaConversionUtils, ScalaAssertionSupport, SparkAdapterSupport}
import org.apache.hudi.HoodieConversionUtils.{toJavaOption, toProperties}
import org.apache.hudi.common.config.{HoodieMetadataConfig, RecordMergeMode}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.{Row, SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.{BeforeEach, Tag}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util.function.Consumer

import scala.collection.JavaConverters._

/**
 * Tests for: when a new column is added to a Hudi table via schema evolution,
 * how is it treated by the MDT column_stats index?
 *
 * This is item #2 from a CDC schema-evolution improvements roadmap. The original
 * recommendation assumed Hudi never auto-extends col_stats to new columns. The
 * tests below confirm the actual behavior is more nuanced — it depends on
 * whether the user set an explicit {@code hoodie.metadata.index.column.stats.column.list}.
 *
 * Two scenarios:
 *
 *   (A) No explicit column.list — default "index all eligible columns" mode.
 *       Test: {@link #testNewColumnInDefaultModeIsAutoIndexed}.
 *       Expected and observed: the new column IS auto-indexed in subsequent
 *       commits. Pre-evolution files have null stats for the new column (the
 *       column didn't exist when those files were written); post-evolution
 *       files have stats. Working as intended.
 *
 *   (B) Explicit column.list that doesn't include the new column.
 *       Test: {@link #testNewColumnWithExplicitListIsNotAutoIndexed}.
 *       Expected: the new column should NOT be auto-indexed (the user opted into
 *       an explicit list). Confirmed.
 *
 * The user-facing implication, and the question this PR is opening for review:
 *
 *   - For users on the default mode, no action is needed when a column is added
 *     via schema evolution. col_stats auto-extends.
 *   - For users with an explicit column.list, a column added at the source is
 *     silently NOT indexed in Hudi unless they remember to extend the list.
 *     Data-skipping queries on the new column will silently fall back to
 *     full-file scans.
 *
 * Question for reviewers: should there be a config option that lets users have
 * an explicit list AND auto-extend to new columns added via schema evolution
 * (e.g., a column pattern like {@code "user_*"} or an "auto-extend" flag)?
 * Or is the current behavior the intended design (explicit list = strict opt-in)?
 *
 * This PR contains tests only. No production code change.
 */
@Tag("functional")
class TestColStatsAutoExtendOnAddColumn extends HoodieSparkClientTestBase
  with ScalaAssertionSupport with SparkAdapterSupport {

  var spark: SparkSession = _

  private val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key() -> "true",
    HoodieWriteConfig.RECORD_MERGE_MODE.key() -> RecordMergeMode.COMMIT_TIME_ORDERING.name(),
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "colstats_autoextend_test",
    HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key -> "true",
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
  )

  override def getSparkSessionExtensionsInjector: Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  /**
   * Scenario (A): no explicit column.list set, so col_stats defaults to "index all
   * eligible columns." After an ADD COLUMN schema-evolution event, the new column
   * IS auto-indexed in subsequent commits. Working as intended.
   *
   * This test codifies the expected behavior — if it ever starts failing, the
   * default-mode auto-extend has regressed.
   */
  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE",
    "MERGE_ON_READ"
  ))
  def testNewColumnInDefaultModeIsAutoIndexed(tableType: HoodieTableType): Unit = {
    val opts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.OPERATION.key -> "upsert"
    )

    // ---- Step 1: write initial batch with 3 columns ----
    val initialSchema = StructType(Seq(
      StructField("_row_key", StringType, nullable = false),
      StructField("partition", IntegerType, nullable = false),
      StructField("timestamp", IntegerType, nullable = false),
      StructField("col_a", IntegerType, nullable = true),
      StructField("col_b", LongType, nullable = true)
    ))
    val initialBatch = Seq(
      Row("1", 1, 1, 10, 100L),
      Row("2", 1, 1, 20, 200L),
      Row("3", 1, 1, 30, 300L)
    )
    sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, initialBatch, initialSchema)
      .write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // ---- Step 2: evolve schema (add col_c) and write second batch ----
    val evolvedSchema = StructType(initialSchema.fields :+
      StructField("col_c", StringType, nullable = true))
    val evolvedBatch = Seq(
      Row("4", 1, 1, 40, 400L, "vier"),
      Row("5", 1, 1, 50, 500L, "fünf")
    )
    sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, evolvedBatch, evolvedSchema)
      .write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Append)
      .save(basePath)

    // ---- Step 3: inspect the MDT col_stats partition ----
    metaClient = createMetaClient(spark, basePath)
    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(evolvedSchema, "record", "")
    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(opts.filter { case (k, _) =>
        k.startsWith("hoodie.metadata")
      }))
      .build()
    val colStats = new ColumnStatsIndexSupport(spark, evolvedSchema, hoodieSchema, metadataConfig, metaClient)

    // First: confirm col_a and col_b are indexed (baseline — these were present
    // from the start).
    colStats.loadTransposed(Seq("col_a", "col_b"), shouldReadInMemory = true) { transposedDf =>
      val rows = transposedDf.collectAsList().asScala
      assertTrue(rows.nonEmpty,
        "col_a and col_b should be present in col_stats (they were in the initial schema)")
    }

    // Then: the actual question — is col_c (the post-evolution column) indexed?
    colStats.loadTransposed(Seq("col_c"), shouldReadInMemory = true) { transposedDf =>
      val rows = transposedDf.collectAsList().asScala
      // Per the operational recommendation from F-s5s6-#1: this is expected to be
      // empty because Hudi doesn't auto-extend the col_stats list on schema evolution.
      // If it's NOT empty, the claim is wrong and the recommendation should be revisited.
      val nonNullColStatsForC = rows.filter { r =>
        // The transposed schema has columns like col_c_minValue, col_c_maxValue.
        // A "real" indexed entry has at least one non-null min/max value, OR
        // its file/partition entry exists (depending on how the index is laid out).
        val idx = r.schema.fieldIndex("col_c_minValue")
        !r.isNullAt(idx)
      }
      assertTrue(nonNullColStatsForC.nonEmpty,
        s"Default mode should auto-index the new column (tableType=$tableType). " +
          s"Pre-evolution files have null stats; post-evolution files should have " +
          s"populated min/max for col_c. If this assertion fails, the default-mode " +
          s"auto-extend has regressed.")
    }
  }

  /**
   * Scenario (B): user sets an explicit {@code hoodie.metadata.index.column.stats.column.list}
   * that does NOT include the column-to-be-added. After the ADD COLUMN evolution,
   * the new column is NOT auto-indexed (the list is treated as a strict opt-in).
   *
   * This is the silent data-skipping regression case. A user with an explicit
   * column.list who adds a column at the source will have queries on the new
   * column fall back to full-file scan without any warning.
   *
   * The PR is asking: is this the intended design (explicit list = strict opt-in),
   * or should there be a way to opt into auto-extend with an explicit list too?
   */
  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE",
    "MERGE_ON_READ"
  ))
  def testNewColumnWithExplicitListIsNotAutoIndexed(tableType: HoodieTableType): Unit = {
    val opts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.OPERATION.key -> "upsert",
      // Explicit list — only col_a and col_b are indexed. col_c is intentionally absent.
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "col_a,col_b"
    )

    val initialSchema = StructType(Seq(
      StructField("_row_key", StringType, nullable = false),
      StructField("partition", IntegerType, nullable = false),
      StructField("timestamp", IntegerType, nullable = false),
      StructField("col_a", IntegerType, nullable = true),
      StructField("col_b", LongType, nullable = true)
    ))
    val initialBatch = Seq(
      Row("1", 1, 1, 10, 100L),
      Row("2", 1, 1, 20, 200L),
      Row("3", 1, 1, 30, 300L)
    )
    sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, initialBatch, initialSchema)
      .write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val evolvedSchema = StructType(initialSchema.fields :+
      StructField("col_c", StringType, nullable = true))
    val evolvedBatch = Seq(
      Row("4", 1, 1, 40, 400L, "vier"),
      Row("5", 1, 1, 50, 500L, "fünf")
    )
    sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, evolvedBatch, evolvedSchema)
      .write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Append)
      .save(basePath)

    metaClient = createMetaClient(spark, basePath)
    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(evolvedSchema, "record", "")
    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(opts.filter { case (k, _) =>
        k.startsWith("hoodie.metadata")
      }))
      .build()
    val colStats = new ColumnStatsIndexSupport(spark, evolvedSchema, hoodieSchema, metadataConfig, metaClient)

    // Sanity: col_a is indexed (it was on the explicit list).
    colStats.loadTransposed(Seq("col_a"), shouldReadInMemory = true) { transposedDf =>
      val rows = transposedDf.collectAsList().asScala
      val withMin = rows.count(r => !r.isNullAt(r.schema.fieldIndex("col_a_minValue")))
      assertTrue(withMin > 0,
        s"col_a should be indexed (it's on the explicit list). tableType=$tableType")
    }

    // The gap: col_c was added via schema evolution but is NOT on the explicit list.
    // Expected behavior per design: no col_stats entries for col_c.
    colStats.loadTransposed(Seq("col_c"), shouldReadInMemory = true) { transposedDf =>
      val rows = transposedDf.collectAsList().asScala
      val withMin = rows.count(r => !r.isNullAt(r.schema.fieldIndex("col_c_minValue")))
      assertEquals(0, withMin,
        s"col_c should NOT be auto-indexed when an explicit column.list is set. " +
          s"This codifies the current behavior; if it changes, auto-extend on " +
          s"explicit list has been added (likely intentional and worth surfacing " +
          s"in release notes). tableType=$tableType")
    }
  }
}
