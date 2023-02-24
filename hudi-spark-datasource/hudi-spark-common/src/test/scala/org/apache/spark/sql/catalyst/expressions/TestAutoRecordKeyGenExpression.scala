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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestAutoRecordKeyGenExpression {

  @Test
  def testAutoRecordKeyGenExpressionCodeGen(): Unit = {
    val nonce = "123456"
    val autoGenExpr = new AutoRecordKeyGenExpression(nonce)

    val projection = GenerateSafeProjection.generate(Seq(autoGenExpr))

    val partitionId = 13
    projection.initialize(partitionId)

    val row = projection.apply(InternalRow.empty)

    assertEquals(s"${nonce}_${partitionId}_0", row.getString(0))
  }

  @Test
  def testAutoRecordKeyGenExpressionInterpreted(): Unit = {
    val nonce = "123456"
    val autoGenExpr = new AutoRecordKeyGenExpression(nonce)

    val partitionId = 13
    autoGenExpr.initialize(partitionId)

    assertEquals(s"${nonce}_${partitionId}_0", autoGenExpr.eval(InternalRow.empty).toString)
  }
}
