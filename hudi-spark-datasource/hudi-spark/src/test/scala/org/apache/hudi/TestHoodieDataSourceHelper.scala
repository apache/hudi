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

import org.apache.hudi.testutils.{DisabledOnSpark4, HoodieClientTestBase}

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.sources.Filter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@DisabledOnSpark4
// Temporarily disabled on Spark 4 because of:
// UnresolvedException: [INTERNAL_ERROR] Invalid call to dataType on unresolved object SQLSTATE: XX000
class TestHoodieDataSourceHelper extends HoodieClientTestBase with SparkAdapterSupport {

  def checkCondition(filter: Option[Filter], outputSet: Set[String], expected: Any): Unit = {
    val actual = HoodieDataSourceHelper.extractPredicatesWithinOutputSet(filter.get, outputSet)
    assertEquals(expected, actual)
  }

  @Test
  def testExtractPredicatesWithinOutputSet(): Unit = {
    val dataColsWithNoPartitionCols = Set("id", "extra_col")

    val expr1 = sparkAdapter.translateFilter(sparkAdapter.getExpressionFromColumn(expr("(region='reg2' and id = 1) or region='reg1'")))
    checkCondition(expr1, dataColsWithNoPartitionCols, None)

    val expr2 = sparkAdapter.translateFilter(sparkAdapter.getExpressionFromColumn(expr("region='reg2' and id = 1")))
    val expectedExpr2 = sparkAdapter.translateFilter(sparkAdapter.getExpressionFromColumn(expr("id = 1")))
    checkCondition(expr2, dataColsWithNoPartitionCols, expectedExpr2)

    // not (region='reg2' and id = 1) -- BooleanSimplification --> not region='reg2' or not id = 1
    val expr3 = sparkAdapter.translateFilter(sparkAdapter.getExpressionFromColumn(expr("not region='reg2' or not id = 1")))
    checkCondition(expr3, dataColsWithNoPartitionCols, None)

    // not (region='reg2' or id = 1) -- BooleanSimplification --> not region='reg2' and not id = 1
    val expr4 = sparkAdapter.translateFilter(sparkAdapter.getExpressionFromColumn(expr("not region='reg2' and not id = 1")))
    val expectedExpr4 = sparkAdapter.translateFilter(sparkAdapter.getExpressionFromColumn(expr("not(id=1)")))
    checkCondition(expr4, dataColsWithNoPartitionCols, expectedExpr4)
  }

}
