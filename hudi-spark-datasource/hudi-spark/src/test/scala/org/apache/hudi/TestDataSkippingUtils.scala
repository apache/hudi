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

package org.apache.hudi

import org.apache.hudi.index.zorder.ZOrderingIndexHelper
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation}
import org.apache.spark.sql.hudi.DataSkippingUtils
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, SparkSession}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, MethodSource, ValueSource}

import scala.collection.JavaConverters._

case class IndexRow(file: String, A_minValue: Long, A_maxValue: Long, A_num_nulls: Long)

case class TestCase()

class TestDataSkippingUtils extends HoodieClientTestBase {

  var spark: SparkSession = _

  @BeforeEach
  override def setUp(): Unit = {
    initSparkContexts()
    spark = sqlContext.sparkSession
  }

  val sourceTableSchema =
    StructType(
      Seq(
        StructField("A", LongType)
      )
    )

  val indexSchema =
    ZOrderingIndexHelper.composeIndexSchema(sourceTableSchema.fields.toSeq.asJava)

  @ParameterizedTest
  @MethodSource(Array("testLookupFilterExpressionsSource"))
  def testLookupFilterExpressions(expr: String, input: Seq[IndexRow], output: Seq[String]): Unit = {
    val resolvedExpr: Expression = resolveFilterExpr(expr, sourceTableSchema)

    val lookupFilter = DataSkippingUtils.createZIndexLookupFilter(resolvedExpr, indexSchema)

    val spark2 = spark
    import spark2.implicits._

    val indexDf = spark.createDataset(input)

    val rows = indexDf.where(new Column(lookupFilter))
      .select("file")
      .collect()
      .map(_.getString(0))
      .toSeq

    assertEquals(output, rows)
  }

  private def resolveFilterExpr(s: String, tableSchema: StructType) = {
    val expr = spark.sessionState.sqlParser.parseExpression(s)
    val schemaFields = tableSchema.fields
    spark.sessionState.analyzer.ResolveReferences(
      Filter(expr, LocalRelation(schemaFields.head, schemaFields.drop(1):_*))
    )
      .asInstanceOf[Filter].condition
  }
}

object TestDataSkippingUtils {
  def testLookupFilterExpressionsSource(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      arguments(
        "A = 0",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0)
        ),
        Seq("file_2")),
      arguments(
        "0 = A",
        Seq(
          IndexRow("file_1", 1, 2, 0),
          IndexRow("file_2", -1, 1, 0)
        ),
        Seq("file_2"))
    )
  }
}
