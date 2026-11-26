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

package org.apache.hudi

import org.apache.hudi.ColumnStatsIndexSupport.composeIndexSchema
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{HoodieIndexDefinition, HoodieIndexMetadata}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.core.read.BaseHoodieTableFileIndex.PartitionPath
import org.apache.hudi.metadata.HoodieIndexVersion
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.resolveExpr
import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito.{mock, never, verify, when}

import scala.collection.JavaConverters._

class TestPartitionStatsIndexSupport extends HoodieSparkClientTestBase {

  private var spark: SparkSession = _
  private val indexedColumns = Seq("A", "B")
  private val sourceSchema = StructType(Seq(
    StructField("A", LongType), StructField("B", StringType), StructField("D", LongType)))
  private val (indexSchema, _) = composeIndexSchema(indexedColumns, indexedColumns, sourceSchema)
  private val allPartitions = Set("missing", "missing_low", "all_null", "matching", "nonmatching", "unindexed")

  @BeforeEach
  override def setUp(): Unit = {
    initSparkContexts()
    spark = sqlContext.sparkSession
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testMissingPartitionStatistics(readInMemory: Boolean): Unit = {
    val indexDf = createIndex(readInMemory, Seq(
      Row("missing", 1L, 125L, 125L, 0L, null, null, null),
      Row("missing_low", 1L, 25L, 25L, 0L, null, null, null),
      Row("all_null", 1L, 125L, 125L, 0L, null, null, 1L),
      Row("matching", 1L, 125L, 125L, 0L, "alice", "alice", 0L),
      Row("nonmatching", 1L, 125L, 125L, 0L, "aaa", "aaa", 0L),
      // Stats can contain partitions outside the query's input paths.
      Row("outside_query", 1L, 125L, 125L, 0L, "alice", "alice", 0L)))
    val (support, fileIndex) = createSupport(indexDf, readInMemory)
    val cases = Seq(
      "B = 'alice'" -> Set("missing", "missing_low", "matching", "unindexed"),
      "A > 100L AND B = 'alice'" -> Set("missing", "matching", "unindexed"),
      "A > 100L OR B = 'alice'" -> allPartitions,
      "A > 100L AND D = 1L" -> Set("missing", "all_null", "matching", "nonmatching", "unindexed"))
    cases.foreach { case (predicate, expected) =>
      val filter = resolveExpr(spark, predicate, sourceSchema)
      val filterSequences = filter match {
        case And(left, right) => Seq(Seq(filter), Seq(left, right))
        case _ => Seq(Seq(filter))
      }
      filterSequences.foreach { filters =>
        assertEquals(Some(expected), support.prunePartitions(fileIndex, filters), predicate)
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testAbsentPartitionsAndUntranslatedFilters(readInMemory: Boolean): Unit = {
    val frames = Seq(
      createIndex(readInMemory, Seq.empty),
      createIndex(readInMemory, Seq(Row("nonmatching", 1L, 25L, 25L, 0L, "aaa", "aaa", 0L))))
    frames.foreach { indexDf =>
      val (support, fileIndex) = createSupport(indexDf, readInMemory)
      val expected = if (indexDf.isEmpty) None else Some(allPartitions - "nonmatching")
      assertEquals(expected, support.prunePartitions(fileIndex,
        Seq(resolveExpr(spark, "B = 'alice'", sourceSchema))))
      if (expected.isEmpty) {
        verify(fileIndex, never()).getAllQueryPartitionPaths
      }
      // This predicate references indexed A but translates to TrueLiteral.
      assertEquals(Some(allPartitions), support.prunePartitions(fileIndex,
        Seq(resolveExpr(spark, "A % 2L = 1L", sourceSchema))))
      // Empty/constant filters return None, allowing the caller to use its full inventory.
      assertTrue(support.prunePartitions(fileIndex, Seq.empty).isEmpty)
      assertTrue(support.prunePartitions(fileIndex, Seq(resolveExpr(spark, "true", sourceSchema))).isEmpty)
      assertTrue(support.prunePartitions(fileIndex, Seq(resolveExpr(spark, "NOT (B = 'alice')", sourceSchema))).isEmpty)
    }
  }

  private def createIndex(readInMemory: Boolean, rows: Seq[Row]): DataFrame = {
    if (readInMemory) {
      spark.createDataFrame(rows.asJava, indexSchema)
    } else {
      spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), indexSchema)
    }
  }

  private def createSupport(indexDf: DataFrame, readInMemory: Boolean): (PartitionStatsIndexSupport, HoodieFileIndex) = {
    val metaClient = mock(classOf[HoodieTableMetaClient])
    val tableConfig = mock(classOf[HoodieTableConfig])
    val definition = HoodieIndexDefinition.newBuilder()
      .withIndexName("column_stats").withIndexType("column_stats")
      .withVersion(HoodieIndexVersion.V2).withSourceFields(indexedColumns.asJava).build()
    when(metaClient.getIndexMetadata).thenReturn(HOption.of(
      new HoodieIndexMetadata(Map("column_stats" -> definition).asJava)))
    when(metaClient.getTableConfig).thenReturn(tableConfig)
    when(tableConfig.getTableInitialVersion).thenReturn(HoodieTableVersion.NINE)
    val fileIndex = mock(classOf[HoodieFileIndex])
    when(fileIndex.getAllQueryPartitionPaths).thenReturn(
      allPartitions.toSeq.map(path => new PartitionPath(path, Array.empty[AnyRef])).asJava)
    val support = new PartitionStatsIndexSupport(spark, sourceSchema, null,
      HoodieMetadataConfig.newBuilder().build(), metaClient) {
      override def isIndexAvailable: Boolean = true

      override protected def shouldReadInMemory(fileIndex: HoodieFileIndex, columns: Seq[String], threshold: Integer): Boolean = readInMemory

      override def loadTransposed[T](targetColumns: Seq[String], shouldReadInMemory: Boolean,
                                     prunedPartitions: Option[Set[String]], prunedFileNamesOpt: Option[Set[String]])(block: DataFrame => T): T = {
        assertEquals(readInMemory, shouldReadInMemory)
        block(indexDf)
      }
    }
    (support, fileIndex)
  }
}
