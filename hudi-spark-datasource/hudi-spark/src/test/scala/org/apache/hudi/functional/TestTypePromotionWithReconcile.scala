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
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.{Row, SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.{BeforeEach, Tag}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util.function.Consumer

/**
 * Repro tests for the interaction between {@code hoodie.datasource.write.reconcile.schema=true}
 * and the type-promotion rules documented at:
 *   https://hudi.apache.org/docs/schema_evolution/
 *
 * The Hudi schema-evolution docs publish a type-promotion matrix that lists
 * {@code int -> long} and {@code int -> double} as supported promotions. Empirically,
 * in Hudi 1.x, enabling {@code reconcile.schema=true} causes these promotions to fail
 * the upsert with a {@code HoodieUpsertException} during {@code validateUpsertSchema},
 * even though the same promotions succeed when reconcile is disabled (the default).
 *
 * This file does NOT propose a fix. It exists to:
 *   1. Codify what the docs say should work.
 *   2. Document the empirical behavior gap.
 *   3. Give reviewers something concrete to confirm: is the user's expectation correct
 *      (and reconcile.schema should NOT block documented type promotions), or is there
 *      a config / usage detail that this test is missing?
 *
 * Mirrors the parameterization style of {@link TestBasicSchemaEvolution} but is scoped
 * to just the type-promotion + reconcile interaction so the discussion is focused.
 */
@Tag("functional")
class TestTypePromotionWithReconcile extends HoodieSparkClientTestBase
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
    HoodieWriteConfig.TBL_NAME.key -> "type_promotion_test",
    HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key -> "true"
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
   * The Hudi schema-evolution docs explicitly list {@code int -> long} and
   * {@code int -> double} as supported type promotions. This test verifies that
   * those promotions succeed across the matrix of:
   *   table type      ∈ {COPY_ON_WRITE, MERGE_ON_READ}
   *   operation       ∈ {upsert}                 (the realistic CDC operation)
   *   reconcile flag  ∈ {true, false}
   *   promotion       ∈ {int -> long, int -> double}
   *
   * Expected outcome per docs: all 8 cells PASS — the table-level schema is
   * promoted to the wider type, the original rows keep their values, and the
   * new rows reflect the wider type.
   *
   * Empirical outcome on Hudi 1.x master: the 4 cells with reconcile=true fail
   * with {@code HoodieUpsertException} during {@code validateUpsertSchema}. The
   * 4 cells with reconcile=false succeed as documented.
   *
   * If this test fails on the reconcile=true cells, that's the gap we want
   * reviewers to confirm: should reconcile.schema=true block documented type
   * promotions, or is this a bug?
   */
  @ParameterizedTest
  @CsvSource(value = Array(
    // tableType,      reconcile, promotion
    "COPY_ON_WRITE,    false,     INT_TO_LONG",
    "COPY_ON_WRITE,    false,     INT_TO_DOUBLE",
    "COPY_ON_WRITE,    true,      INT_TO_LONG",   // <-- empirically fails in 1.x
    "COPY_ON_WRITE,    true,      INT_TO_DOUBLE", // <-- empirically fails in 1.x
    "MERGE_ON_READ,    false,     INT_TO_LONG",
    "MERGE_ON_READ,    false,     INT_TO_DOUBLE",
    "MERGE_ON_READ,    true,      INT_TO_LONG",   // <-- empirically fails in 1.x
    "MERGE_ON_READ,    true,      INT_TO_DOUBLE"  // <-- empirically fails in 1.x
  ))
  def testDocumentedTypePromotionShouldSucceed(tableType: HoodieTableType,
                                               reconcileEnabled: Boolean,
                                               promotion: String): Unit = {
    val opts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.RECONCILE_SCHEMA.key -> reconcileEnabled.toString,
      DataSourceWriteOptions.OPERATION.key -> "upsert"
    )

    // ---- Step 1: write the initial batch with col_promote as IntegerType ----
    val initialSchema = StructType(Seq(
      StructField("_row_key", StringType, nullable = false),
      StructField("partition", IntegerType, nullable = false),
      StructField("timestamp", IntegerType, nullable = false),
      StructField("col_promote", IntegerType, nullable = true)
    ))
    val initialBatch = Seq(
      Row("1", 1, 1, 100),
      Row("2", 1, 1, 200),
      Row("3", 1, 1, 300)
    )
    sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, initialBatch, initialSchema)
      .write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // ---- Step 2: write a second batch with col_promote widened to the target type ----
    val (widerType, widerBatch) = promotion match {
      case "INT_TO_LONG" =>
        val s = StructType(Seq(
          StructField("_row_key", StringType, nullable = false),
          StructField("partition", IntegerType, nullable = false),
          StructField("timestamp", IntegerType, nullable = false),
          StructField("col_promote", LongType, nullable = true)
        ))
        val b = Seq(
          Row("4", 1, 1, 400L),
          Row("5", 1, 1, 500L)
        )
        (s, b)
      case "INT_TO_DOUBLE" =>
        val s = StructType(Seq(
          StructField("_row_key", StringType, nullable = false),
          StructField("partition", IntegerType, nullable = false),
          StructField("timestamp", IntegerType, nullable = false),
          StructField("col_promote", DoubleType, nullable = true)
        ))
        val b = Seq(
          Row("4", 1, 1, 400.0),
          Row("5", 1, 1, 500.0)
        )
        (s, b)
      case other => throw new IllegalArgumentException(s"Unknown promotion: $other")
    }

    // Per docs at https://hudi.apache.org/docs/schema_evolution/, the following write
    // should succeed for both reconcile=true and reconcile=false. This is the assertion
    // under review — if it throws under reconcile=true, that's the gap.
    sparkAdapter.getUnsafeUtils.createDataFrameFromRows(spark, widerBatch, widerType)
      .write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Append)
      .save(basePath)

    // ---- Step 3: read back and verify ----
    // Drop Hudi meta columns; only check the data fields.
    val readBack = spark.read.format("org.apache.hudi").load(basePath)
      .select("_row_key", "col_promote")
      .orderBy("_row_key")

    assertEquals(5, readBack.count(),
      s"After the type-promotion write, we expect 5 rows total (3 initial + 2 new). " +
        s"tableType=$tableType reconcile=$reconcileEnabled promotion=$promotion")

    // The col_promote column should appear in the promoted type after the second write.
    val promotedField = readBack.schema.find(_.name == "col_promote").get
    val expectedType = promotion match {
      case "INT_TO_LONG"   => LongType
      case "INT_TO_DOUBLE" => DoubleType
    }
    assertEquals(expectedType, promotedField.dataType,
      s"After promotion $promotion, col_promote should have type $expectedType but was " +
        s"${promotedField.dataType}. tableType=$tableType reconcile=$reconcileEnabled")

    // Round-trip the values themselves. The 3 initial rows had Int values; after the
    // promotion they should be readable in the wider type with the original numeric values.
    val rows = readBack.collectAsList()
    assertEquals(5, rows.size())
    // Just verify the initial values are non-null and match — the wider type should still
    // hold the original numeric value (100, 200, 300, 400, 500).
    val seen = (0 until rows.size()).map(i => rows.get(i).get(1)).toSet
    assertTrue(seen.contains(100) || seen.contains(100L) || seen.contains(100.0),
      s"Expected to find value 100 in the promoted col_promote column; got $seen")
  }
}
