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

package org.apache.spark.sql.hudi

import org.apache.hudi.testutils.{HoodieClientTestBase, HoodieClientTestHarness}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.junit.Test
import org.junit.jupiter.api.BeforeEach

class TestDataSkippingUtils extends HoodieClientTestBase {

  var spark: SparkSession = _

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    spark = sqlContext.sparkSession
  }

  @Test
  def testZIndexLookupFilter(): Unit = {
    var indexSchema = StructType(
      Seq(StructField("A", LongType))
    )
    val expr = spark.sessionState.sqlParser.parseExpression("A != 0")
    val lookupFilter = DataSkippingUtils.createZIndexLookupFilter(expr, indexSchema)
  }

}
