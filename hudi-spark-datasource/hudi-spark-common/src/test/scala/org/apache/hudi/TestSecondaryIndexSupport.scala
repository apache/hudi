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

import org.apache.hudi.SecondaryIndexSupport.filterQueriesWithSecondaryKey
import org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, FromUnixTime, GreaterThan, In, Literal, Not}
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.TimeZone

class TestSecondaryIndexSupport {
  // dummy record key field
  val recordKeyField = "_row_key"

  @ParameterizedTest
  @ValueSource(strings = Array("_row_key", "_hoodie_record_key"))
  def testFilterQueryWithSecondaryKey(filterColumnName: String): Unit = {
    // Case 1: EqualTo filters not on simple AttributeReference and non-Literal should return empty result
    val fmt = "yyyy-MM-dd HH:mm:ss"
    val fromUnixTime = FromUnixTime(Literal(0L), Literal(fmt), Some(TimeZone.getDefault.getID))
    var testFilter: Expression = EqualTo(fromUnixTime, Literal("2020-01-01 00:10:20"))
    var result = filterQueriesWithSecondaryKey(Seq(testFilter), Option.empty)._2
    assertTrue(result.isEmpty)

    // Case 2: EqualTo filters not on Literal and not on simple AttributeReference should return empty result
    testFilter = EqualTo(Literal("2020-01-01 00:10:20"), fromUnixTime)
    result = filterQueriesWithSecondaryKey(Seq(testFilter), Option.empty)._2
    assertTrue(result.isEmpty)

    // Case 3: EqualTo filters on simple AttributeReference and non-Literal should return empty result
    testFilter = EqualTo(AttributeReference(filterColumnName, StringType, nullable = true)(), fromUnixTime)
    result = filterQueriesWithSecondaryKey(Seq(testFilter), Option.empty)._2
    assertTrue(result.isEmpty)

    // Case 4: EqualTo filters on simple AttributeReference and Literal which should return non-empty result
    testFilter = EqualTo(AttributeReference(filterColumnName, StringType, nullable = true)(), Literal("row1"))
    result = filterQueriesWithSecondaryKey(Seq(testFilter), Option.apply(recordKeyField))._2
    assertTrue(result.nonEmpty)
    assertEquals(result, List.apply("row1"))

    // case 5: EqualTo on fields other than record key should return empty result
    result = filterQueriesWithSecondaryKey(Seq(testFilter), Option.apply("blah"))._2
    if (filterColumnName.equals(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName)) {
      assertTrue(result.nonEmpty)
      assertEquals(result, List.apply("row1"))
    } else {
      assertTrue(result.isEmpty)
    }

    // Case 6: In filter on fields other than record key should return empty result
    testFilter = In(AttributeReference(filterColumnName, StringType, nullable = true)(), List.apply(Literal("xyz"), Literal("abc")))
    result = filterQueriesWithSecondaryKey(Seq(testFilter), Option.apply("blah"))._2
    if (filterColumnName.equals(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName)) {
      assertTrue(result.nonEmpty)
      assertEquals(result, List.apply("xyz", "abc"))
    } else {
      assertTrue(result.isEmpty)
    }

    // Case 7: In filter on record key should return non-empty result
    testFilter = In(AttributeReference(filterColumnName, StringType, nullable = true)(), List.apply(Literal("xyz"), Literal("abc")))
    result = filterQueriesWithSecondaryKey(Seq(testFilter), Option.apply(recordKeyField))._2
    assertTrue(result.nonEmpty)

    // Case 8: In filter on simple AttributeReference(on record-key) and non-Literal should return empty result
    testFilter = In(AttributeReference(filterColumnName, StringType, nullable = true)(), List.apply(fromUnixTime))
    result = filterQueriesWithSecondaryKey(Seq(testFilter), Option.apply(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName))._2
    assertTrue(result.isEmpty)

    // Case 9: Anything other than EqualTo and In predicate is not supported. Hence it returns empty result
    testFilter = Not(In(AttributeReference(filterColumnName, StringType, nullable = true)(), List.apply(Literal("xyz"), Literal("abc"))))
    result = filterQueriesWithSecondaryKey(Seq(testFilter), Option.apply(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName))._2
    assertTrue(result.isEmpty)

    testFilter = Not(In(AttributeReference(filterColumnName, StringType, nullable = true)(), List.apply(fromUnixTime)))
    result = filterQueriesWithSecondaryKey(Seq(testFilter), Option.apply(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName))._2
    assertTrue(result.isEmpty)

    testFilter = GreaterThan(AttributeReference(filterColumnName, StringType, nullable = true)(), Literal("row1"))
    result = filterQueriesWithSecondaryKey(Seq(testFilter), Option.apply(HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName))._2
    assertTrue(result.isEmpty)
  }
}
