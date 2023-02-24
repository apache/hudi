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

    val generatedKeys = Seq.range(0, 4)
      .map(_ => projection.apply(InternalRow.empty).copy())
      .map(_.getString(0))
    val expectedKeys = Seq.range(0, 4).map(id => s"${nonce}_${partitionId}_${id}")

    assertEquals(expectedKeys, generatedKeys)
  }

  @Test
  def testAutoRecordKeyGenExpressionInterpreted(): Unit = {
    val nonce = "123456"
    val autoGenExpr = new AutoRecordKeyGenExpression(nonce)

    val partitionId = 13
    autoGenExpr.initialize(partitionId)

    val generatedKeys = Seq.range(0, 4).map(_ => autoGenExpr.eval(InternalRow.empty).toString)
    val expectedKeys = Seq.range(0, 4).map(id => s"${nonce}_${partitionId}_${id}")

    assertEquals(expectedKeys, generatedKeys)
  }
}
