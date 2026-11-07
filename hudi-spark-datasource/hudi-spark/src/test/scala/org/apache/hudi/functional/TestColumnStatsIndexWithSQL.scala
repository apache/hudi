/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.{AvroConversionUtils, ColumnStatsIndexSupport, DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex, HoodieSchemaConversionUtils}
import org.apache.hudi.DataSourceWriteOptions.{DELETE_OPERATION_OPT_VAL, RECORDKEY_FIELD}
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieTableType, WriteOperationType}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.table.timeline.{HoodieInstant, MetadataConversionUtils}
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.functional.ColumnStatIndexTestBase.{ColumnStatsTestCase, ColumnStatsTestParams}
import org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY
import org.apache.hudi.io.util.FileIOUtils
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadata}
import org.apache.hudi.util.JavaConversions

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BitwiseOr, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThanOrEqual, Literal, Or}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.hudi.DataSkippingUtils
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.extension.{AfterAllCallback, ExtendWith, ExtensionContext}
import org.junit.jupiter.api.parallel.{Execution, ExecutionMode}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.io.File
import java.lang.ref.WeakReference
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import scala.collection.JavaConverters._
import scala.collection.mutable

@Tag("functional-b")
@ExtendWith(Array(classOf[TestColumnStatsIndexWithSQL.SharedTablesCleanup]))
// the shared table cache in the companion object is not thread-safe
@Execution(ExecutionMode.SAME_THREAD)
class TestColumnStatsIndexWithSQL extends ColumnStatIndexTestBase {
  import TestColumnStatsIndexWithSQL._

  // Shared tables this test reads or copies; each one is checked for changes after the test.
  private var sharedTablesInUse: Seq[SharedTable] = Seq()
  private var initialSqlConf: Map[String, String] = Map()

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    sharedTablesInUse = Seq()
    // Every test must start from a fresh SparkSession: no SQL conf, temp views or cached plans
    // left over from a previous test that read the same shared table.
    assertTrue(lastSparkSession.get() ne spark, "SparkSession reused across tests")
    lastSparkSession = new WeakReference(spark)
    assertFalse(spark.catalog.tableExists("tbl"))
    initialSqlConf = spark.conf.getAll
  }

  @AfterEach
  override def tearDown(): Unit = {
    try {
      if (spark != null) {
        spark.catalog.dropTempView("tbl")
        spark.catalog.clearCache()
        val sqlConf = spark.sessionState.conf
        val currentConf = sqlConf.getAllConfs
        (currentConf.keySet -- initialSqlConf.keySet).foreach(sqlConf.unsetConf)
        initialSqlConf.foreach { case (k, v) => if (!currentConf.get(k).contains(v)) sqlConf.setConfString(k, v) }
      }
      // A test must never change a shared table (data files, timeline or metadata table timeline);
      // tests that write work on their own copy.
      sharedTablesInUse.foreach { table =>
        val state = captureTableState(table.path)
        if (state != table.state) {
          // evict it so that later tests rebuild a clean table and only this test fails
          sharedTables.retain((_, cached) => cached ne table)
        }
        assertEquals(table.state, state, s"Shared table ${table.path} was modified by the test")
      }
    } finally {
      super.tearDown()
    }
  }

  /**
   * Points the test at a table built by `build`, building it only the first time a test asks for it.
   * Tables are keyed by `recipe` plus the exact write options, so parameter cases that write the
   * same data with the same options (e.g. ones that only differ in how the index is read) share one table.
   * `recipe` must name every other input of `build` that affects the table or its validation.
   * With `copy = true` the test gets a private copy it may write to.
   */
  private def useSharedTable(recipe: String, writeOpts: Map[String, String], copy: Boolean)(build: => Unit): Unit = {
    val key = recipe + writeOpts.toSeq.sorted.mkString("[", ",", "]")
    val table = sharedTables.getOrElse(key, {
      basePath = Files.createTempDirectory(sharedTablesRoot, "table-").toString
      // same starting point as a per-test table: an empty table initialized by the harness, which the first
      // Overwrite write then replaces
      try {
        initMetaClient()
        build
      } catch {
        case e: Throwable =>
          FileIOUtils.deleteDirectory(new File(basePath))
          throw e
      }
      val built = SharedTable(basePath, captureTableState(basePath))
      sharedTables.put(key, built)
      built
    })
    sharedTablesInUse = sharedTablesInUse :+ table
    basePath = if (copy) {
      val target = Files.createTempDirectory(tempDir, "shared-table-copy-")
      copyDirectory(Paths.get(table.path), target)
      target.toString
    } else {
      table.path
    }
    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    assertEquals(table.state, captureTableState(basePath))
  }

  /**
   * Write options that create the table at `testCase.tableVersion`: v6 tables use timeline layout 1, later
   * versions layout 2. Being write options, they also separate the shared tables of different versions.
   */
  private def tableVersionOpts(testCase: ColumnStatsTestCase): Map[String, String] = Map(
    HoodieWriteConfig.WRITE_TABLE_VERSION.key -> testCase.tableVersion.toString,
    HoodieTableConfig.VERSION.key -> testCase.tableVersion.toString,
    HoodieTableConfig.TIMELINE_LAYOUT_VERSION.key -> (if (testCase.tableVersion == HoodieTableVersion.SIX.versionCode()) {
      TimelineLayoutVersion.LAYOUT_VERSION_1
    } else {
      TimelineLayoutVersion.LAYOUT_VERSION_2
    }).getVersion.toString
  )

  private def captureTableState(tablePath: String): TableState = {
    def completedInstants(path: String): Seq[String] = HoodieTableMetaClient.builder()
      .setBasePath(path).setConf(storageConf).build()
      .getActiveTimeline.filterCompletedInstants().getInstants.asScala
      .map(i => s"${i.requestedTime}:${i.getAction}:${i.getCompletionTime}").toSeq
    val root = Paths.get(tablePath)
    // size and modification time catch both new/removed files and in-place rewrites
    val files = walk(root)(_.filter(Files.isRegularFile(_))
      .map(f => root.relativize(f).toString -> ((Files.size(f), Files.getLastModifiedTime(f).toMillis))).toMap)
    val mdtPath = HoodieTableMetadata.getMetadataTableBasePath(tablePath)
    val mdtInstants = if (Files.exists(Paths.get(mdtPath))) completedInstants(mdtPath) else Seq()
    TableState(completedInstants(tablePath), mdtInstants, files)
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParams"))
  def testMetadataColumnStatsIndexWithSQL(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL
    ) ++ metadataOpts ++ tableVersionOpts(testCase)
    useSharedTable("setupTable(shouldValidate = true)", commonOpts, copy = false) {
      setupTable(testCase, metadataOpts, commonOpts, shouldValidate = true)
    }
    verifyFileIndexAndSQLQueries(commonOpts)
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParams"))
  def testMetadataColumnStatsIndexWithSQLWithLimitedIndexes(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_MAX_COLUMNS.key() -> "3"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL
    ) ++ metadataOpts ++ tableVersionOpts(testCase)
    useSharedTable("setupTable(shouldValidate = true, useShortSchema = true)", commonOpts, copy = false) {
      setupTable(testCase, metadataOpts, commonOpts, shouldValidate = true, useShortSchema = true,
        validationSortColumns = Seq("c1_maxValue", "c1_minValue", "c2_maxValue",
        "c2_minValue", "c3_maxValue", "c3_minValue"))
    }

    // predicate with c2. should prune based on col stats lookup
    var dataFilter: Expression = EqualTo(attribute("c2"), literal("619sdc"))
    verifyPruningFileCount(commonOpts, dataFilter)
    // predicate w/ c5. should not lookup in col stats since the column is not indexed.
    var dataFilter1: Expression = GreaterThan(attribute("c5"), literal("70"))
    verifyPruningFileCount(commonOpts, dataFilter1, false)

    // a mix of two cols, where c2 is indexed and c5 is not indexed. but since its 'AND', pruning should kick in.
    var dataFilter2 = And(dataFilter1, EqualTo(attribute("c2"), literal("619sdc")))
    verifyPruningFileCount(commonOpts, dataFilter2, true)
    // adding an AND clause
    dataFilter2 = And(dataFilter2, EqualTo(attribute("c5"), literal("100")))
    verifyPruningFileCount(commonOpts, dataFilter2, true)
    // adding an OR clause where the col is indexed. expected to prune
    var dataFilter2_1 = Or(dataFilter2, EqualTo(attribute("c2"), literal("619sda")))
    verifyPruningFileCount(commonOpts, dataFilter2_1, true)
    // adding another Or clause, but this time the col is not indexed. So, no pruning expected.
    dataFilter2_1 = Or(dataFilter2_1, EqualTo(attribute("c5"), literal("120")))
    verifyPruningFileCount(commonOpts, dataFilter2_1, false)

    // a mix of two cols, where c2 is indexed and c5 is not indexed. but since its 'OR', pruning should be by passed.
    var dataFilter3 = Or(dataFilter1, EqualTo(attribute("c2"), literal("619sdc")))
    verifyPruningFileCount(commonOpts, dataFilter3, false)
    // adding an OR clause
    dataFilter3 = Or(dataFilter3, EqualTo(attribute("c5"), literal("100")))
    verifyPruningFileCount(commonOpts, dataFilter3, false)
    // adding AND clause where the col is indexed. Expected to prune.
    var dataFilter3_1 = And(dataFilter3, EqualTo(attribute("c2"), literal("619sda")))
    verifyPruningFileCount(commonOpts, dataFilter3_1, true)
    // adding another AND clause where the col is not indexed. Still expected to prune since c2 = 619sda could still be pruned.
    dataFilter3_1 = And(dataFilter3_1, EqualTo(attribute("c5"), literal("200")))
    verifyPruningFileCount(commonOpts, dataFilter3_1, true)
    // adding an Or clause where the col is indexed. expected to prune.
    var dataFilter3_2 = Or(dataFilter3_1, EqualTo(attribute("c2"), literal("619sda")))
    verifyPruningFileCount(commonOpts, dataFilter3_2, true)
    // adding an Or clause where the col is not indexed. not expected to prune
    dataFilter3_2 = Or(dataFilter3_2, EqualTo(attribute("c5"), literal("250")))
    verifyPruningFileCount(commonOpts, dataFilter3_2, false)
  }

  @Test
  def testTranslateIntoColumnStatsIndexFilterExpr(): Unit = {
    var dataFilter: Expression = EqualTo(attribute("c1"), literal("619sdc"))
    var expectedExpr: Expression = generateColStatsExprForGreaterthanOrEquals("c1","619sdc")
    translateIntoColStatsExprAndValidate(dataFilter, Seq("c1"), expectedExpr, false)

    // if c1 is not indexed, we should get empty expr
    translateIntoColStatsExprAndValidate(dataFilter, Seq.empty, TrueLiteral, true)

    // c1 = 619sdc and c2 = 100, where both c1 and c2 are indexed.
    val dataFilter1 = And(dataFilter, EqualTo(attribute("c2"), literal("100")))
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c1","619sdc")
    expectedExpr = And(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c2","100"))
    translateIntoColStatsExprAndValidate(dataFilter1, Seq("c1","c2"), expectedExpr, false)

    // c1 = 619sdc and c2 = 100, where only c1 is indexed. we expect only c1 to be part of translated expr
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c1","619sdc")
    translateIntoColStatsExprAndValidate(dataFilter1, Seq("c1"), expectedExpr, false)

    // c1 = 619sdc and c2 = 100 and c3 = 200, where all 3 (c1, c2 and c3) are indexed.
    val dataFilter1_1 = And(dataFilter1, EqualTo(attribute("c3"), literal("200")))
    expectedExpr = And(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c2","100"))
    expectedExpr = And(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c3","200"))
    translateIntoColStatsExprAndValidate(dataFilter1_1, Seq("c1","c2","c3"), expectedExpr, false)

    // c1 = 619sdc and c2 = 100 and c3 = 200, where all only c1 and c3 are indexed.
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c1","619sdc")
    expectedExpr = And(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c3","200"))
    translateIntoColStatsExprAndValidate(dataFilter1_1, Seq("c1","c3"), expectedExpr, false)

    // c1 = 619sdc and c2 = 100 and c3 = 200, where only c1 is indexed. we expect only c1 to be part of translated expr
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c1","619sdc")
    translateIntoColStatsExprAndValidate(dataFilter1_1, Seq("c1"), expectedExpr, false)

    // c1 = 619sdc and c2 = 100 and c3 = 200, where none of c1, or c2 or c3 is indexed.
    translateIntoColStatsExprAndValidate(dataFilter1_1, Seq(""), TrueLiteral, true)

    // c1 = 619sdc Or c2 = 100, where both c1 and c2 are indexed.
    val dataFilter2 = Or(dataFilter, EqualTo(attribute("c2"), literal("100")))
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c1","619sdc")
    expectedExpr = Or(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c2","100"))
    translateIntoColStatsExprAndValidate(dataFilter2, Seq("c1","c2"), expectedExpr, false)

    // c1 = 619sdc Or c2 = 100, where only c1 is indexed. Since its a 'or' condition, we can't translate the expr and we expect TrueLiteral.
    translateIntoColStatsExprAndValidate(dataFilter2, Seq("c1"), TrueLiteral, true)

    // lets mix `and` and 'or' together.
    // c1 = 619sdc and c2 = 100 or c3 = 200, where all of them (c1, c2 and c3) are indexed.
    val dataFilter1_2 = Or(dataFilter1, EqualTo(attribute("c3"), literal("200")))
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c1","619sdc")
    expectedExpr = And(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c2","100"))
    expectedExpr = Or(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c3","200"))
    translateIntoColStatsExprAndValidate(dataFilter1_2, Seq("c1","c2","c3"), expectedExpr, false)

    // c1 = 619sdc and c2 = 100 or c3 = 200, where c3 is not indexed.
    translateIntoColStatsExprAndValidate(dataFilter1_2, Seq("c1","c2"), TrueLiteral, true)

    // c1 = 619sdc and c2 = 100 or c3 = 200, where c2 is not indexed.
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c1","619sdc")
    expectedExpr = Or(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c3","200"))
    translateIntoColStatsExprAndValidate(dataFilter1_2, Seq("c1","c3"), expectedExpr, false)

    // c1 = 619sdc and c2 = 100 or c3 = 200, where c1 is not indexed.
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c2","100")
    expectedExpr = Or(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c3","200"))
    translateIntoColStatsExprAndValidate(dataFilter1_2, Seq("c2","c3"), expectedExpr, false)

    // c1 = 619sdc and c2 = 100 or c3 = 200, where c1 and c3 is not indexed.
    translateIntoColStatsExprAndValidate(dataFilter1_2, Seq("c2"), TrueLiteral, true)

    // c1 = 619sdc and c2 = 100 or c3 = 200, where c1 and c2 is not indexed.
    translateIntoColStatsExprAndValidate(dataFilter1_2, Seq("c3"), TrueLiteral, true)

    // c1 = 619sdc and c2 = 100 or c3 = 200, where c2 and c3 is not indexed.
    translateIntoColStatsExprAndValidate(dataFilter1_2, Seq("c1"), TrueLiteral, true)

    val dataFilter2_2 = And(dataFilter2, EqualTo(attribute("c3"), literal("200")))
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c1","619sdc")
    expectedExpr = Or(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c2","100"))
    expectedExpr = And(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c3","200"))
    translateIntoColStatsExprAndValidate(dataFilter2_2, Seq("c1","c2","c3"), expectedExpr, false)

    // c1 = 619sdc or c2 = 100 and c3 = 200, where c3 is not indexed.
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c1","619sdc")
    expectedExpr = Or(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c2","100"))
    translateIntoColStatsExprAndValidate(dataFilter2_2, Seq("c1","c2"), expectedExpr, false)

    // c1 = 619sdc or c2 = 100 and c3 = 200, where c2 is not indexed.
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c3","200")
    translateIntoColStatsExprAndValidate(dataFilter2_2, Seq("c1","c3"), expectedExpr, false)

    // c1 = 619sdc or c2 = 100 and c3 = 200, where c1 is not indexed.
    translateIntoColStatsExprAndValidate(dataFilter2_2, Seq("c2","c3"), expectedExpr, false)

    // c1 = 619sdc or c2 = 100 and c3 = 200, where c1 and c3 is not indexed.
    translateIntoColStatsExprAndValidate(dataFilter2_2, Seq("c2"), TrueLiteral, true)

    // c1 = 619sdc or c2 = 100 and c3 = 200, where c1 and c2 is not indexed.
    translateIntoColStatsExprAndValidate(dataFilter2_2, Seq("c3"), expectedExpr, false)

    // c1 = 619sdc or c2 = 100 and c3 = 200, where c2 and c3 is not indexed.
    translateIntoColStatsExprAndValidate(dataFilter2_2, Seq("c1"), TrueLiteral, true)

    // nested And and Or case
    val dataFilter3 = And(
      Or(
        EqualTo(attribute("c1"), literal("619sdc")),
        And(EqualTo(attribute("c2"), literal("100")), EqualTo(attribute("c3"), literal("200")))
      ),
      EqualTo(attribute("c4"), literal("300"))
    )
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c2","100")
    expectedExpr = And(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c3","200"))
    expectedExpr = Or(generateColStatsExprForGreaterthanOrEquals("c1","619sdc"), expectedExpr)
    expectedExpr = And(expectedExpr, generateColStatsExprForGreaterthanOrEquals("c4","300"))
    translateIntoColStatsExprAndValidate(
      dataFilter3,
      Seq("c1", "c2", "c3", "c4"),
      expectedExpr,
      false
    )

    // unsupported filter type
    val dataFilter4 = BitwiseOr(
        EqualTo(attribute("c1"), literal("619sdc")),
        EqualTo(attribute("c2"), literal("100"))
      )
    translateIntoColStatsExprAndValidate(
      dataFilter4,
      Seq("c1", "c2"),
      TrueLiteral,
      true
    )

    // too many filters, out of which only half are indexed.
    val largeFilter = (1 to 100).map(i => EqualTo(attribute(s"c$i"), literal("value"))).reduce(And)
    val indexedColumns = (1 to 50).map(i => s"c$i")
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c1","value")
    (2 to 50).map(i => {
      expectedExpr = And(expectedExpr, generateColStatsExprForGreaterthanOrEquals(s"c$i","value"))
    })
    translateIntoColStatsExprAndValidate(largeFilter, indexedColumns, expectedExpr, false)

    // nested field part of data filters.
    val dataFilter5 = And(
      EqualTo(attribute("c1"), literal("val1")),
      EqualTo(attribute("nested.inner_c2"), literal("val2"))
    )
    expectedExpr = generateColStatsExprForGreaterthanOrEquals("c1","val1")
    translateIntoColStatsExprAndValidate(
      dataFilter5,
      Seq("c1"),
      expectedExpr,
      false
    )

  }

  def translateIntoColStatsExprAndValidate(dataFilter: Expression, indexedCols: Seq[String], expectedExpr: Expression,
                                           expectTrueLiteral: Boolean): Unit = {
    val transformedExpr = DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr(dataFilter, false, indexedCols)
    assertTrue(expectedExpr.equals(transformedExpr))
    if (expectTrueLiteral) {
      assertTrue(expectedExpr.equals(TrueLiteral))
    } else {
      assertTrue(!expectedExpr.equals(TrueLiteral))
    }
  }

  def generateColStatsExprForGreaterthanOrEquals(colName: String, colValue: String): Expression = {
    val expectedExpr: Expression = GreaterThanOrEqual(UnresolvedAttribute(colName + "_maxValue"), literal(colValue))
    And(LessThanOrEqual(UnresolvedAttribute(colName + "_minValue"), literal(colValue)), expectedExpr)
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsForMOR"))
  def testMetadataColumnStatsIndexSQLWithInMemoryIndex(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      HoodieIndexConfig.INDEX_TYPE.key() -> INMEMORY.name()
    ) ++ metadataOpts ++ tableVersionOpts(testCase)

    useSharedTable("insert", commonOpts, copy = false) {
      doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
        dataSourcePath = "index/colstats/input-table-json",
        expectedColStatsSourcePath = "index/colstats/column-stats-index-table.json",
        operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Overwrite,
        shouldValidateColStats = false))
    }

    assertEquals(4, getLatestDataFilesCount(commonOpts))
    assertEquals(0, getLatestDataFilesCount(commonOpts, includeLogFiles = false))
    var dataFilter = GreaterThan(attribute("c5"), literal("90"))
    verifyPruningFileCount(commonOpts, dataFilter)
    dataFilter = GreaterThan(attribute("c5"), literal("95"))
    verifyPruningFileCount(commonOpts, dataFilter)
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParams"))
  def testMetadataColumnStatsIndexDeletionWithSQL(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      HoodieWriteConfig.RECORD_MERGE_MODE.key -> "COMMIT_TIME_ORDERING"
    ) ++ metadataOpts ++ tableVersionOpts(testCase)
    useSharedTable("setupTable(shouldValidate = true)", commonOpts, copy = true) {
      setupTable(testCase, metadataOpts, commonOpts, shouldValidate = true)
    }
    // the last batch written by setupTable
    val lastDf = spark.read.schema(sourceTableSchema)
      .json(getClass.getClassLoader.getResource("index/colstats/update-input-table-json").toString)

    lastDf.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    verifyFileIndexAndSQLQueries(commonOpts, isTableDataSameAsAfterSecondInstant = true)

    // Add the last df back and verify the queries
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update-input-table-json",
      expectedColStatsSourcePath = "",
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false))
    verifyFileIndexAndSQLQueries(commonOpts, verifyFileCount = false)
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsForMOR"))
  def testMetadataColumnStatsIndexCompactionWithSQL(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      HoodieCompactionConfig.INLINE_COMPACT.key -> "true",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "1"
    ) ++ metadataOpts ++ tableVersionOpts(testCase)
    useSharedTable("setupTable(shouldValidate = false)", commonOpts, copy = false) {
      setupTable(testCase, metadataOpts, commonOpts, shouldValidate = false)
    }

    assertFalse(hasLogFiles())
    verifyFileIndexAndSQLQueries(commonOpts)
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsForMOR"))
  def testMetadataColumnStatsIndexScheduledCompactionWithSQL(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "1"
    ) ++ metadataOpts ++ tableVersionOpts(testCase)
    useSharedTable("setupTable(shouldValidate = false)", commonOpts, copy = true) {
      setupTable(testCase, metadataOpts, commonOpts, shouldValidate = false)
    }

    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), getWriteConfig(commonOpts))
    writeClient.scheduleCompaction(org.apache.hudi.common.util.Option.empty())
    writeClient.close()

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update-input-table-json",
      expectedColStatsSourcePath = "",
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false))
    verifyFileIndexAndSQLQueries(commonOpts)
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsForMOR"))
  def testGetPrunedPartitionsAndFileNames(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "20"
    ) ++ metadataOpts ++ tableVersionOpts(testCase)
    useSharedTable("setupTable(shouldValidate = false)", commonOpts, copy = true) {
      setupTable(testCase, metadataOpts, commonOpts, shouldValidate = false)
    }

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update-input-table-json",
      expectedColStatsSourcePath = "",
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false))
    verifyFileIndexAndSQLQueries(commonOpts)

    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + ("path" -> basePath), includeLogFiles = true)
    val metadataConfig = HoodieMetadataConfig.newBuilder.withMetadataIndexColumnStats(true).enable(true).build
    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(fileIndex.schema, "record", "")
    val cis = new ColumnStatsIndexSupport(spark, fileIndex.schema, hoodieSchema,  metadataConfig, metaClient)
    // unpartitioned table - get all file slices
    val fileSlices = fileIndex.prunePartitionsAndGetFileSlices(Seq.empty, Seq())
    var files = cis.getPrunedPartitionsAndFileNames(fileIndex, fileSlices._2)._2
    // Number of files obtained if file index has include log files as true is double of number of parquet files
    val numberOfParquetFiles = 9
    assertEquals(numberOfParquetFiles * 2, files.size)
    assertEquals(numberOfParquetFiles, files.count(f => FSUtils.isBaseFile(f)))

    fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + ("path" -> basePath), includeLogFiles = false)
    files = cis.getPrunedPartitionsAndFileNames(fileIndex, fileSlices._2)._2
    assertEquals(numberOfParquetFiles, files.size)
    assertEquals(numberOfParquetFiles, files.count(f => f.endsWith("parquet")))
  }

  @Test
  def testUpdateAndSkippingWithColumnStatIndex() {
    val tableName = "testUpdateAndSkippingWithColumnStatIndex"
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      HoodieWriteConfig.TBL_NAME.key -> tableName,
      DataSourceWriteOptions.TABLE_TYPE.key -> "mor",
      RECORDKEY_FIELD.key -> "id",
      HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "20",
      HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key() -> "0"
    ) ++ metadataOpts

    FileIOUtils.deleteDirectory(new File(basePath))
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  part long
         |) using hudi
         | options (
         |  primaryKey ='id',
         |  type = 'mor',
         |  orderingFields = 'name',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.index.column.stats.enable = 'true'
         | )
         | partitioned by(part)
         | location '$basePath'
       """.stripMargin)
    spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000), (2, 'a2', 10, 1001), (3, 'a3', 10, 1002)")
    spark.sql(s"update $tableName set price = 20 where id = 1")

    metaClient = HoodieTableMetaClient.reload(metaClient)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + ("path" -> basePath), includeLogFiles = true)
    val dataFilter = EqualTo(attribute("price"), literal("20"))
    var filteredPartitionDirectories = fileIndex.listFiles(Seq.empty, Seq(dataFilter))
    var filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    assertEquals(2, filteredFilesCount)
    assertEquals(1, spark.sql(s"select * from $tableName where price = 20").count())

    fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + ("path" -> basePath), includeLogFiles = false)
    filteredPartitionDirectories = fileIndex.listFiles(Seq.empty, Seq(dataFilter))
    filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    assertEquals(0, filteredFilesCount)
    val df = spark.read.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(basePath)
      .filter("price = 20")
    assertEquals(0, df.count())
  }

  private def setupTable(testCase: ColumnStatsTestCase, metadataOpts: Map[String, String], commonOpts: Map[String, String],
                         shouldValidate: Boolean, useShortSchema: Boolean = false,
                         validationSortColumns : Seq[String] = Seq("c1_maxValue", "c1_minValue", "c2_maxValue",
                           "c2_minValue", "c3_maxValue", "c3_minValue", "c5_maxValue", "c5_minValue")): Unit = {
    val filePostfix = if (useShortSchema) {
      "-short-schema"
    } else {
      ""
    }
    writeAndValidateBothReadModes(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = s"index/colstats/column-stats-index-table${filePostfix}.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      shouldValidateManually = !useShortSchema,
      saveMode = SaveMode.Overwrite,
      validationSortColumns = validationSortColumns))

    writeAndValidateBothReadModes(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/another-input-table-json",
      expectedColStatsSourcePath = s"index/colstats/updated-column-stats-index-table${filePostfix}.json",
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      shouldValidateManually = !useShortSchema,
      saveMode = SaveMode.Append,
      validationSortColumns = validationSortColumns))

    // NOTE: MOR and COW have different fixtures since MOR is bearing delta-log files (holding
    //       deferred updates), diverging from COW
    val expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      s"index/colstats/cow-updated2-column-stats-index-table${filePostfix}.json"
    } else {
      s"index/colstats/mor-updated2-column-stats-index-table${filePostfix}.json"
    }

    writeAndValidateBothReadModes(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidate,
      shouldValidateManually = !useShortSchema,
      validationSortColumns = validationSortColumns))
  }

  /**
   * Writes like [[doWriteAndValidateColumnStats]] but validates the column stats with both the
   * in-memory and the Spark-based index read, since the table may be shared by test cases of either mode.
   */
  private def writeAndValidateBothReadModes(params: ColumnStatsTestParams): Unit = {
    doWriteAndValidateColumnStats(params)
    if (params.shouldValidateColStats) {
      val validateManually = (params.testCase.tableType == HoodieTableType.COPY_ON_WRITE ||
        params.operation.equals(DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)) && params.shouldValidateManually
      validateColumnStatsIndex(params.testCase.copy(shouldReadInMemory = !params.testCase.shouldReadInMemory),
        params.metadataOpts, params.expectedColStatsSourcePath, validateManually, params.validationSortColumns)
    }
  }

  def verifyFileIndexAndSQLQueries(opts: Map[String, String], isTableDataSameAsAfterSecondInstant: Boolean = false, verifyFileCount: Boolean = true): Unit = {
    var commonOpts = opts
    val inputDF1 = spark.read.format("hudi")
      .options(commonOpts)
      .option("as.of.instant", metaClient.getActiveTimeline.getInstants.get(1).requestedTime)
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key, "false")
      .load(basePath)
    inputDF1.createOrReplaceTempView("tbl")
    val numRecordsForFirstQuery = spark.sql("select * from tbl where c5 > 70").count()
    val numRecordsForSecondQuery = spark.sql("select * from tbl where c5 > 70 and c6 >= '2020-03-28'").count()
    // verify snapshot query
    verifySQLQueries(numRecordsForFirstQuery, numRecordsForSecondQuery, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, commonOpts, isTableDataSameAsAfterSecondInstant)

    // verify read_optimized query
    verifySQLQueries(numRecordsForFirstQuery, numRecordsForSecondQuery, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, commonOpts, isTableDataSameAsAfterSecondInstant)

    // verify incremental query
    verifySQLQueries(numRecordsForFirstQuery, numRecordsForSecondQuery, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL, commonOpts, isTableDataSameAsAfterSecondInstant)
    commonOpts = commonOpts + (DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key -> "true")
    // TODO: https://issues.apache.org/jira/browse/HUDI-6657 - Investigate why below assertions fail with full table scan enabled.
    // verifySQLQueries(numRecordsForFirstQuery, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL, commonOpts, isTableDataSameAsAfterSecondInstant)

    var dataFilter: Expression = GreaterThan(attribute("c5"), literal("70"))
    verifyPruningFileCount(commonOpts, dataFilter)
    dataFilter = And(dataFilter, GreaterThan(attribute("c6"), literal("2020-03-28")))
    verifyPruningFileCount(commonOpts, dataFilter)
    dataFilter = GreaterThan(attribute("c5"), literal("90"))
    verifyPruningFileCount(commonOpts, dataFilter)
    dataFilter = And(dataFilter, GreaterThan(attribute("c6"), literal("2020-03-28")))
    verifyPruningFileCount(commonOpts, dataFilter)
  }

  private def verifyPruningFileCount(opts: Map[String, String], dataFilter: Expression, shouldPrune : Boolean = true): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> basePath)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    val filteredPartitionDirectories = fileIndex.listFiles(Seq(), Seq(dataFilter))
    val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    val latestDataFilesCount = getLatestDataFilesCount(opts)
    if (shouldPrune) {
      assertTrue(filteredFilesCount < latestDataFilesCount)
    } else {
      assertEquals(filteredFilesCount, latestDataFilesCount)
    }

    // with no data skipping
    fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
    val filesCountWithNoSkipping = fileIndex.listFiles(Seq(), Seq(dataFilter)).flatMap(s => s.files).size
    if (shouldPrune) {
      assertTrue(filteredFilesCount < filesCountWithNoSkipping)
    } else {
      assertEquals(filteredFilesCount, filesCountWithNoSkipping)
    }
  }

  private def getLatestDataFilesCount(opts: Map[String, String], includeLogFiles: Boolean = true) = {
    var totalLatestDataFiles = 0L
    val fsView = getTableFileSystemView(opts)
    fsView.loadAllPartitions()
    fsView.getPartitionPaths.asScala.flatMap { partitionPath =>
      val relativePath = FSUtils.getRelativePartitionPath(metaClient.getBasePath, partitionPath)
      fsView.getLatestMergedFileSlicesBeforeOrOn(relativePath, metaClient.reloadActiveTimeline().lastInstant().get().requestedTime).iterator().asScala.toSeq
    }.foreach(
      slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
        + (if (slice.getBaseFile.isPresent) 1 else 0))
    totalLatestDataFiles
  }

  private def getTableFileSystemView(opts: Map[String, String]): HoodieTableFileSystemView = {
    val writeConfig = getWriteConfig(opts)
    val metadataTable = new HoodieBackedTableMetadata(new HoodieSparkEngineContext(jsc), metaClient.getStorage,
      writeConfig.getMetadataConfig, writeConfig.getBasePath)
    new HoodieTableFileSystemView(metadataTable, metaClient, metaClient.getActiveTimeline)
  }

  protected def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(hudiOpts.asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }

  private def attribute(partition: String): AttributeReference = {
    AttributeReference(partition, StringType, true)()
  }

  private def literal(value: String): Literal = {
    Literal.create(value)
  }

  private def verifySQLQueries(numRecordsForFirstQueryAtPrevInstant: Long, numRecordsForSecondQueryAtPrevInstant: Long,
                               queryType: String, opts: Map[String, String], isLastOperationDelete: Boolean): Unit = {
    val firstQuery = "select * from tbl where c5 > 70"
    val secondQuery = "select * from tbl where c5 > 70 and c6 >= '2020-03-28'"
    // 2 records are updated with c5 greater than 70 and one record is inserted with c5 value greater than 70
    var commonOpts: Map[String, String] = opts
    createSQLTable(commonOpts, queryType)
    val incrementFirstQuery = if (queryType.equals(DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL) && hasLogFiles()) {
      1 // only one insert
    } else if (isLastOperationDelete) {
      0 // no increment
    } else {
      3 // one insert and two upserts
    }
    val incrementSecondQuery = if (queryType.equals(DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL) && hasLogFiles()) {
      1 // only one insert
    } else if (isLastOperationDelete) {
      0 // no increment
    } else {
      2 // one insert and two upserts
    }
    assertEquals(spark.sql(firstQuery).count(), numRecordsForFirstQueryAtPrevInstant + incrementFirstQuery)
    assertEquals(spark.sql(secondQuery).count(), numRecordsForSecondQueryAtPrevInstant + incrementSecondQuery)
    val numRecordsForFirstQueryWithDataSkipping = spark.sql(firstQuery).count()
    val numRecordsForSecondQueryWithDataSkipping = spark.sql(secondQuery).count()

    if (queryType.equals(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)) {
      createIncrementalSQLTable(commonOpts, metaClient.reloadActiveTimeline().getInstants.get(1).getCompletionTime)
      assertEquals(spark.sql(firstQuery).count(), if (isLastOperationDelete) 0 else 3)
      assertEquals(spark.sql(secondQuery).count(), if (isLastOperationDelete) 0 else 2)
    }

    commonOpts = opts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false")
    createSQLTable(commonOpts, queryType)
    val numRecordsForFirstQueryWithoutDataSkipping = spark.sql(firstQuery).count()
    val numRecordsForSecondQueryWithoutDataSkipping = spark.sql(secondQuery).count()
    assertEquals(numRecordsForFirstQueryWithDataSkipping, numRecordsForFirstQueryWithoutDataSkipping)
    assertEquals(numRecordsForSecondQueryWithDataSkipping, numRecordsForSecondQueryWithoutDataSkipping)
  }

  private def createSQLTable(hudiOpts: Map[String, String], queryType: String): Unit = {
    val opts = hudiOpts + (
      DataSourceReadOptions.QUERY_TYPE.key -> queryType,
      DataSourceReadOptions.START_COMMIT.key() -> metaClient.getActiveTimeline.getInstants.get(0).requestedTime.replaceFirst(".", "0")
    )
    val inputDF1 = spark.read.format("hudi").options(opts).load(basePath)
    inputDF1.createOrReplaceTempView("tbl")
  }

  private def createIncrementalSQLTable(hudiOpts: Map[String, String], startCompletionTime: String): Unit = {
    val opts = hudiOpts + (
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      DataSourceReadOptions.START_COMMIT.key() -> startCompletionTime
    )
    val inputDF1 = spark.read.format("hudi").options(opts).load(basePath)
    inputDF1.createOrReplaceTempView("tbl")
  }

  private def hasLogFiles(): Boolean = {
    isTableMOR && getLatestCompactionInstant() != metaClient.getActiveTimeline.lastInstant()
  }

  private def isTableMOR(): Boolean = {
    metaClient.getTableType == HoodieTableType.MERGE_ON_READ
  }

  protected def getLatestCompactionInstant(): org.apache.hudi.common.util.Option[HoodieInstant] = {
    metaClient.reloadActiveTimeline()
      .filter(JavaConversions.getPredicate(s => Option(
        try {
          val commitMetadata = MetadataConversionUtils.getHoodieCommitMetadata(metaClient, s)
            .orElse(new HoodieCommitMetadata())
          commitMetadata
        } catch {
          case _: Exception => new HoodieCommitMetadata()
        })
        .map(c => c.getOperationType == WriteOperationType.COMPACT)
        .get))
      .lastInstant()
  }
}

object TestColumnStatsIndexWithSQL {

  case class TableState(completedInstants: Seq[String], mdtCompletedInstants: Seq[String], files: Map[String, (Long, Long)])

  case class SharedTable(path: String, state: TableState)

  private val sharedTables = mutable.Map[String, SharedTable]()
  private var sharedTablesRootDir: Path = _
  private var lastSparkSession: WeakReference[SparkSession] = new WeakReference(null)

  private def sharedTablesRoot: Path = {
    if (sharedTablesRootDir == null) {
      sharedTablesRootDir = Files.createTempDirectory("hudi-colstats-sql-shared-")
    }
    sharedTablesRootDir
  }

  private def walk[T](root: Path)(f: Iterator[Path] => T): T = {
    val stream = Files.walk(root)
    try {
      f(stream.iterator().asScala)
    } finally {
      stream.close()
    }
  }

  private def copyDirectory(source: Path, target: Path): Unit = walk(source)(_.foreach { src =>
    val dst = target.resolve(source.relativize(src).toString)
    if (Files.isDirectory(src)) {
      Files.createDirectories(dst)
    } else {
      Files.copy(src, dst, StandardCopyOption.COPY_ATTRIBUTES)
    }
  })

  class SharedTablesCleanup extends AfterAllCallback {
    override def afterAll(context: ExtensionContext): Unit = {
      sharedTables.clear()
      lastSparkSession = new WeakReference(null)
      if (sharedTablesRootDir != null) {
        FileIOUtils.deleteDirectory(sharedTablesRootDir.toFile)
        sharedTablesRootDir = null
      }
    }
  }
}
