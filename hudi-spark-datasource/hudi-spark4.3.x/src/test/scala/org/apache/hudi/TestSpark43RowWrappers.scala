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

import org.apache.hudi.client.model.Spark43HoodieInternalRow

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.unsafe.types.{TimestampNanosVal, UTF8String}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

/**
 * Validates the nanosecond timestamp getters that Spark 4.3 adds to `SpecializedGetters` on the
 * Hudi row wrappers.
 */
class TestSpark43RowWrappers {

  private val ntz = new TimestampNanosVal(1L, 2.toShort)
  private val ltz = new TimestampNanosVal(3L, 4.toShort)

  @Test
  def testInternalRowRebasesNanosGettersPastMetaFields(): Unit = {
    val metaFields = Array.fill(5)(UTF8String.fromString("meta"))
    val row = new Spark43HoodieInternalRow(metaFields, new GenericInternalRow(Array[Any](ntz, ltz)), false)

    assertEquals(ntz, row.getTimestampNTZNanos(5))
    assertEquals(ltz, row.getTimestampLTZNanos(6))
    assertThrows(classOf[ClassCastException], () => row.getTimestampNTZNanos(0))
    assertThrows(classOf[ClassCastException], () => row.getTimestampLTZNanos(4))
  }

  @Test
  def testPartitionValuesDelegateNanosGetters(): Unit = {
    val values = Spark43HoodiePartitionValues(new GenericInternalRow(Array[Any](ntz, ltz)))

    assertEquals(ntz, values.getTimestampNTZNanos(0))
    assertEquals(ltz, values.getTimestampLTZNanos(1))
  }
}
