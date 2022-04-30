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

package org.apache.spark.sql

import org.apache.spark.sql.HoodieUnsafeRowUtils.{composeNestedFieldPath, getNestedInternalRowValue}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, fail}
import org.junit.jupiter.api.Test

class TestHoodieUnsafeRowUtils {

  @Test
  def testComposeNestedFieldPath(): Unit = {
    val schema = StructType(Seq(
      StructField("foo", StringType),
      StructField(
        name = "bar",
        dataType = StructType(Seq(
          StructField("baz", DateType),
          StructField("bor", LongType)
        ))
      )
    ))

    assertEquals(
      Seq((1, schema(1)), (0, schema(1).dataType.asInstanceOf[StructType](0))),
      composeNestedFieldPath(schema, "bar.baz").toSeq)

    assertThrows(classOf[IllegalArgumentException]) { () =>
      composeNestedFieldPath(schema, "foo.baz")
    }
  }

  @Test
  def testGetNestedRowValue(): Unit = {
    val schema = StructType(Seq(
      StructField("foo", StringType, nullable = false),
      StructField(
        name = "bar",
        dataType = StructType(Seq(
          StructField("baz", DateType),
          StructField("bor", LongType)
        ))
      )
    ))

    val row = InternalRow("str", InternalRow(123, 456L))

    assertEquals(
      123,
      getNestedInternalRowValue(row, composeNestedFieldPath(schema, "bar.baz"))
    )
    assertEquals(
      456L,
      getNestedInternalRowValue(row, composeNestedFieldPath(schema, "bar.bor"))
    )
    assertEquals(
      "str",
      getNestedInternalRowValue(row, composeNestedFieldPath(schema, "foo"))
    )
    assertEquals(
      row.getStruct(1, 2),
      getNestedInternalRowValue(row, composeNestedFieldPath(schema, "bar"))
    )

    val rowProperNullable = InternalRow("str", null)

    assertEquals(
      null,
      getNestedInternalRowValue(rowProperNullable, composeNestedFieldPath(schema, "bar.baz"))
    )
    assertEquals(
      null,
      getNestedInternalRowValue(rowProperNullable, composeNestedFieldPath(schema, "bar"))
    )

    val rowInvalidNullable = InternalRow(null, InternalRow(123, 456L))

    assertThrows(classOf[IllegalArgumentException]) { () =>
      getNestedInternalRowValue(rowInvalidNullable, composeNestedFieldPath(schema, "foo"))
    }
  }

  private def assertThrows[T <: Throwable](expectedExceptionClass: Class[T])(f: () => Unit): T = {
    try {
      f.apply()
    } catch {
      case t: Throwable if expectedExceptionClass.isAssignableFrom(t.getClass) =>
        // scalastyle:off return
        return t.asInstanceOf[T]
        // scalastyle:on return
      case ot @ _ =>
        fail(s"Expected exception of class $expectedExceptionClass, but ${ot.getClass} has been thrown")
    }

    fail(s"Expected exception of class $expectedExceptionClass, but nothing has been thrown")
  }

}
