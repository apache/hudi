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

package org.apache.spark.sql.adapter

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}

import org.apache.spark.sql.types.{IntegerType, StringType}
import org.junit.jupiter.api.Assertions.{assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test

/**
 * Tests for the variant-related methods in [[BaseSpark3Adapter]].
 * These methods are stubs that return None/false or throw UnsupportedOperationException
 * since Spark 3.x does not support VariantType.
 */
class TestBaseSpark3AdapterVariantMethods extends SparkAdapterSupport {

  @Test
  def testGetVariantDataTypeReturnsNone(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark3, "Only applies to Spark 3.x")
    assertTrue(sparkAdapter.getVariantDataType.isEmpty,
      "getVariantDataType should return None on Spark 3.x")
  }

  @Test
  def testIsVariantTypeReturnsFalse(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark3, "Only applies to Spark 3.x")
    assertFalse(sparkAdapter.isVariantType(StringType),
      "isVariantType should return false for StringType on Spark 3.x")
    assertFalse(sparkAdapter.isVariantType(IntegerType),
      "isVariantType should return false for IntegerType on Spark 3.x")
  }

  @Test
  def testIsDataTypeEqualForPhysicalSchemaReturnsNone(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark3, "Only applies to Spark 3.x")
    assertTrue(sparkAdapter.isDataTypeEqualForPhysicalSchema(StringType, StringType).isEmpty,
      "isDataTypeEqualForPhysicalSchema should return None on Spark 3.x")
  }

  @Test
  def testCreateVariantValueWriterThrows(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark3, "Only applies to Spark 3.x")
    assertThrows(classOf[UnsupportedOperationException], () => {
      sparkAdapter.createVariantValueWriter(StringType, _ => (), _ => ())
    })
  }

  @Test
  def testConvertVariantFieldToParquetTypeThrows(): Unit = {
    assumeTrue(HoodieSparkUtils.isSpark3, "Only applies to Spark 3.x")
    val schema = HoodieSchema.create(HoodieSchemaType.BYTES)
    assertThrows(classOf[UnsupportedOperationException], () => {
      sparkAdapter.convertVariantFieldToParquetType(
        StringType, "test_field", schema,
        org.apache.parquet.schema.Type.Repetition.REQUIRED)
    })
  }
}
