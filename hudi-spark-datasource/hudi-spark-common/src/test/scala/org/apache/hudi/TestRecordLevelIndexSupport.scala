/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi

import org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, FromUnixTime, GreaterThan, In, Literal, Not, Or}
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.TimeZone

class TestRecordLevelIndexSupport {
  // dummy record key field
  val recordKeyField = "_row_key"

  @ParameterizedTest
  @ValueSource(strings = Array("_row_key", "_hoodie_record_key"))
  def testFilterQueryWithRecordKey(filterColumnName: String): Unit = {
    // Case 1: EqualTo filters not on simple AttributeReference and non-Literal should return empty result
    val fmt = "yyyy-MM-dd HH:mm:ss"
    val fromUnixTime = FromUnixTime(Literal(0L), Literal(fmt), Some(TimeZone.getDefault.getID))
    var testFilter: Expression = EqualTo(fromUnixTime, Literal("2020-01-01 00:10:20"))
    var result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.empty)
    assertTrue(result.isEmpty)

    // Case 2: EqualTo filters not on Literal and not on simple AttributeReference should return empty result
    testFilter = EqualTo(Literal("2020-01-01 00:10:20"), fromUnixTime)
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.empty)
    assertTrue(result.isEmpty)

    // Case 3: EqualTo filters on simple AttributeReference and non-Literal should return empty result
    testFilter = EqualTo(AttributeReference(filterColumnName, StringType, nullable = true)(), fromUnixTime)
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.empty)
    assertTrue(result.isEmpty)

    // Case 4: EqualTo filters on simple AttributeReference and Literal which should return non-empty result
    testFilter = EqualTo(AttributeReference(filterColumnName, StringType, nullable = true)(), Literal("row1"))
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.apply(recordKeyField))
    assertTrue(result.isDefined)
    assertEquals(result, Option.apply(testFilter, List.apply("row1")))

    // case 5: EqualTo on fields other than record key should return empty result unless it's _hoodie_record_key
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.apply("blah"))
    if (filterColumnName.equals(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName)) {
      assertTrue(result.isDefined)
      assertEquals(result, Option.apply(testFilter, List.apply("row1")))
    } else {
      assertTrue(result.isEmpty)
    }

    // Case 6: In filter on fields other than record key should return empty result unless it's _hoodie_record_key
    testFilter = In(AttributeReference(filterColumnName, StringType, nullable = true)(), List.apply(Literal("xyz"), Literal("abc")))
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.apply("blah"))
    if (filterColumnName.equals(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName)) {
      assertTrue(result.isDefined)
      assertEquals(result, Option.apply(testFilter, List.apply("xyz", "abc")))
    } else {
      assertTrue(result.isEmpty)
    }

    // Case 7: In filter on record key should return non-empty result
    testFilter = In(AttributeReference(filterColumnName, StringType, nullable = true)(), List.apply(Literal("xyz"), Literal("abc")))
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.apply(recordKeyField))
    assertTrue(result.isDefined)
    assertEquals(result, Option.apply(testFilter, List.apply("xyz", "abc")))

    // Case 8: In filter on simple AttributeReference(on record-key) and non-Literal should return empty result
    testFilter = In(AttributeReference(filterColumnName, StringType, nullable = true)(), List.apply(fromUnixTime))
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.apply(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName))
    assertTrue(result.isEmpty)

    // Case 9: Anything other than EqualTo and In predicate is not supported. Hence it returns empty result
    testFilter = Not(In(AttributeReference(filterColumnName, StringType, nullable = true)(), List.apply(Literal("xyz"), Literal("abc"))))
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.apply(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName))
    assertTrue(result.isEmpty)

    testFilter = Not(In(AttributeReference(filterColumnName, StringType, nullable = true)(), List.apply(fromUnixTime)))
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.apply(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName))
    assertTrue(result.isEmpty)

    testFilter = GreaterThan(AttributeReference(filterColumnName, StringType, nullable = true)(), Literal("row1"))
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.apply(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName))
    assertTrue(result.isEmpty)

    // Case 10: Complex AND query with IN query on multiple columns returns the corresponding literals
    val rk1InFilter = In(AttributeReference("rk1", StringType, nullable = true)(), List.apply(Literal("a1"), Literal("a2")))
    val rk2InFilter = In(AttributeReference("rk2", StringType, nullable = true)(), List.apply(Literal("b1"), Literal("b2")))
    val rk3InFilter = In(AttributeReference("rk3", StringType, nullable = true)(), List.apply(Literal("c1"), Literal("c2")))
    testFilter = And(rk3InFilter, And(rk2InFilter, rk1InFilter))
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.apply("rk1"), RecordLevelIndexSupport.getComplexKeyLiteralGenerator())
    assertTrue(result.get._2.contains("rk1:a1"))
    assertTrue(result.get._2.contains("rk1:a2"))

    // Case 11: Complex AND query with EqualTo and In query on multiple columns returns the corresponding literals
    var rk1EqFilter = EqualTo(AttributeReference("rk1", StringType, nullable = true)(), Literal("a1"))
    var rk2EqFilter = EqualTo(AttributeReference("rk2", StringType, nullable = true)(), Literal("b1"))
    testFilter = And(rk3InFilter, And(rk2EqFilter, rk1EqFilter))
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.apply("rk1"), RecordLevelIndexSupport.getComplexKeyLiteralGenerator())
    assertTrue(result.get._2.contains("rk1:a1"))

    // Case 12: Test negative case with complex query using the above testFilter on a different record key
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.apply("rk4"), RecordLevelIndexSupport.getComplexKeyLiteralGenerator())
    assertTrue(result.isEmpty)

    // Case 11: Test unsupported query type with And
    testFilter = And(rk3InFilter, Or(rk2EqFilter, rk1EqFilter))
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.apply("rk1"), RecordLevelIndexSupport.getComplexKeyLiteralGenerator())
    assertTrue(result.isEmpty)

    // Case 11: Test unsupported query type with And. Here the unsupported query OR is at second level from the root query type
    testFilter = And(rk1EqFilter, And(rk1InFilter, Or(rk1EqFilter, rk1EqFilter)))
    result = RecordLevelIndexSupport.filterQueryWithRecordKey(testFilter, Option.apply("rk1"), RecordLevelIndexSupport.getComplexKeyLiteralGenerator())
    assertTrue(result.isEmpty)
  }
}
