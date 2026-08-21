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

import org.apache.hudi.{DataSourceWriteOptions, ScalaAssertionSupport, SparkAdapterSupport}
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.common.config.{HoodieMetadataConfig, RecordMergeMode}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.{Row, SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.{BeforeEach, Tag}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util.function.Consumer

/**
 * Repro test for: when a Hudi column is promoted from {@code BinaryType} (parquet
 * binary, no STRING annotation) to {@code StringType} via schema evolution, the
 * promotion is documented as supported by
 * <a href="https://hudi.apache.org/docs/schema_evolution/">the Hudi schema-evolution
 * matrix</a>. Empirically, on Hudi 1.x master:
 *
 *   <ul>
 *     <li>The WRITE succeeds (initial bytes batch + evolved string batch both commit).</li>
 *     <li>Reading WITHOUT data-skipping succeeds and returns correct rows.</li>
 *     <li>Reading WITH data-skipping (the default) throws:
 *       <pre>
 *         java.lang.ClassCastException:
 *           class java.nio.HeapByteBuffer cannot be cast to class [B
 *       </pre>
 *       The cast appears to come from the MDT col_stats comparator path when it
 *       encounters mixed bytes + string union members for the same column.
 *     </li>
 *   </ul>
 *
 * This PR does NOT include a fix. The intent is for reviewers to confirm one of:
 *
 *   (a) the test correctly demonstrates a bug — bytes→string is documented as
 *       supported, so data-skipping queries should not crash after the promotion,
 *       OR
 *   (b) the test setup is missing a config / usage detail that the empirical
 *       crash depends on (in which case the docs should clarify the limitation).
 *
 * The test parameterizes across (tableType × data-skipping enabled/disabled) for
 * 4 cells. Per docs, all 4 should PASS. Observed: the 2 data-skipping-enabled
 * cells throw {@code ClassCastException}; the 2 disabled cells pass.
 *
 * Mirrors the parameterization style of {@link TestBasicSchemaEvolution} and the
 * "discuss before fix" framing of PR #18806 / PR #18807.
 */
@Tag("functional")
class TestBytesToStringPromotionDataSkipping extends HoodieSparkClientTestBase
  with ScalaAssertionSupport with SparkAdapterSupport {

  var spark: SparkSession = _

  private val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key() -> "true",
    HoodieWriteConfig.RECORD_MERGE_MODE.key() -> RecordMergeMode.COMMIT_TIME_ORDERING.name(),
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "bytes_to_string_test",
    HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key -> "true",
    // MDT col_stats explicitly enabled and listing col_promote, since the crash
    // happens in the MDT col_stats comparator path.
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
    HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "col_promote"
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
   * Promote {@code col_promote} from {@code BinaryType} to {@code StringType} via
   * schema evolution. After the promotion, query the table with a string predicate
   * and verify the read succeeds.
   *
   * Expected per docs: all 4 (tableType × dataSkipping) cells PASS.
   *
   * Observed on Hudi 1.x master: the 2 cells with {@code dataSkipping=true} throw
   * {@code ClassCastException: HeapByteBuffer cannot be cast to [B}. The 2 cells
   * with {@code dataSkipping=false} pass.
   *
   * If the test fails on the {@code dataSkipping=true} cells, that's the gap we
   * want reviewers to confirm: should the MDT col_stats comparator handle the
   * bytes → string union-member transition, or is this an unsupported edge case
   * the docs need to clarify?
   */
  @ParameterizedTest
  @CsvSource(value = Array(
    // tableType,      dataSkipping
    "COPY_ON_WRITE,    true",     // <-- empirically throws ClassCastException
    "COPY_ON_WRITE,    false",
    "MERGE_ON_READ,    true",     // <-- empirically throws ClassCastException
    "MERGE_ON_READ,    false"
  ))
  def testBytesToStringPromotionReadAfterEvolution(tableType: HoodieTableType,
                                                   dataSkippingEnabled: Boolean): Unit = {
    val opts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.OPERATION.key -> "insert",
      DataSourceWriteOptions.RECONCILE_SCHEMA.key -> "false"
    )

    // ---- Step 1: write initial batch with col_promote as BinaryType ----
    val initialSchema = StructType(Seq(
      StructField("_row_key", StringType, nullable = false),
      StructField("partition", IntegerType, nullable = false),
      StructField("timestamp", IntegerType, nullable = false),
      StructField("col_promote", BinaryType, nullable = true)
    ))
    val initialBatch = Seq(
      Row("1", 1, 1, Array[Byte](0x01, 0x02)),
      Row("2", 1, 1, Array[Byte](0x03, 0x04)),
      Row("3", 1, 1, Array[Byte](0x05, 0x06))
    )
    sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, initialBatch, initialSchema)
      .write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // ---- Step 2: write evolved batch with col_promote as StringType ----
    val evolvedSchema = StructType(Seq(
      StructField("_row_key", StringType, nullable = false),
      StructField("partition", IntegerType, nullable = false),
      StructField("timestamp", IntegerType, nullable = false),
      StructField("col_promote", StringType, nullable = true)
    ))
    val evolvedBatch = Seq(
      Row("4", 1, 1, "zz_alpha"),
      Row("5", 1, 1, "zz_beta")
    )
    sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, evolvedBatch, evolvedSchema)
      .write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Append)
      .save(basePath)

    // ---- Step 3: read back with a string predicate on the promoted column ----
    // A string predicate on col_promote forces the data-skipping path to consult
    // MDT col_stats for col_promote, which is where the crash happens — the
    // comparator path must reconcile bytes-typed stats records (pre-evolution
    // files) with the string predicate (post-evolution type).
    //
    // Per the Hudi schema-evolution docs, bytes → string is supported. The
    // read should succeed regardless of data-skipping value and return the
    // matching row.
    spark.conf.set("hoodie.enable.data.skipping", dataSkippingEnabled.toString)
    val df = spark.read.format("org.apache.hudi").load(basePath)
    df.createOrReplaceTempView("t")

    // Query matches exactly one row (the evolved row with col_promote="zz_alpha").
    val rows = spark.sql("SELECT _row_key FROM t WHERE col_promote = 'zz_alpha'").collect()
    assertEquals(1, rows.length,
      s"Expected 1 row matching col_promote='zz_alpha' after bytes→string promotion " +
        s"(tableType=$tableType, dataSkipping=$dataSkippingEnabled). If this throws " +
        s"ClassCastException (HeapByteBuffer cannot be cast to [B), it's the bug " +
        s"this PR is opening for discussion.")
  }
}
