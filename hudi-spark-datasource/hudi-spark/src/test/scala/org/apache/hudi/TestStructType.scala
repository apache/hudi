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

import org.apache.spark.sql.types.{DataTypes, Metadata, MetadataBuilder, StructField, StructType}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Tests tp validate the behavior of StructType and StructField in Spark.
 */
class TestStructType {
  @Test
  def testStructTypeEqual(): Unit = {
    val st1 = StructType.fromDDL("a LONG, b INT")
    val st2 = StructType.fromDDL("a LONG, b INT")
    assertTrue(st1 == st2, "struct types should be same")
  }

  @Test
  def testStructTypeNotEqualOnOrderingChange(): Unit = {
    val st1 = StructType.fromDDL("a LONG, b INT")
    val st2 = StructType.fromDDL("b INT, a LONG")
    assertTrue(st1 != st2, "struct types should be different")
  }

  @Test
  def testStructTypeNotEqualOnFieldTypeChange(): Unit = {
    val st1 = StructType.fromDDL("a LONG, b INT")
    val st2 = StructType.fromDDL("a INT, b INT")
    assertTrue(st1 != st2, "struct types should be different")
  }

  @Test
  def testStructTypeNotEqualOnFieldNameChange(): Unit = {
    val st1 = StructType.fromDDL("a LONG, b INT")
    val st2 = StructType.fromDDL("a1 LONG, b INT")
    assertTrue(st1 != st2, "struct types should be different")
  }

  @Test
  def testStructFieldsEqual(): Unit = {
    val st1 = StructType.fromDDL("a LONG, b INT")
    val st2 = StructType.fromDDL("a LONG, b INT")
    val f1 = StructField("a", st1, nullable = true, Metadata.empty)
    val f2 = StructField("a", st2, nullable = true, Metadata.empty)
    assertTrue(f1 == f2, "fields should be same")
  }

  @Test
  def testStructFieldsNotEqualOnNullableChange(): Unit = {
    val st1 = StructType.fromDDL("a LONG, b INT")
    val st2 = StructType.fromDDL("a LONG, b INT")
    val f1 = StructField("a", st1, nullable = true, Metadata.empty)
    val f2 = StructField("a", st2, nullable = false, Metadata.empty)
    assertTrue(f1 != f2, "fields should be different")
  }

  @Test
  def testStructFieldsNotEqualOnDataTypeChange(): Unit = {
    val st1 = StructType.fromDDL("a LONG, b INT")
    val st2 = DataTypes.LongType
    val f1 = StructField("a", st1, nullable = true, Metadata.empty)
    val f2 = StructField("a", st2, nullable = false, Metadata.empty)
    assertTrue(f1 != f2, "fields should be different")
  }

  @Test
  def testStructFieldsNotEqualOnNameChange(): Unit = {
    val st1 = StructType.fromDDL("a LONG, b INT")
    val st2 = StructType.fromDDL("a LONG, b INT")
    val f1 = StructField("a1", st1, nullable = true, Metadata.empty)
    val f2 = StructField("a", st2, nullable = false, Metadata.empty)
    assertTrue(f1 != f2, "fields should be different")
  }

  @Test
  def testStructFieldsNotEqualOnMetadataChange(): Unit = {
    val st1 = StructType.fromDDL("a LONG, b INT")
    val st2 = StructType.fromDDL("a LONG, b INT")
    val f1 = StructField("a", st1, nullable = true, Metadata.empty)
    val f2 = StructField("a", st2, nullable = false, new MetadataBuilder().putLong("a", 1).build())
    assertTrue(f1 != f2, "fields should be different")
  }
}
