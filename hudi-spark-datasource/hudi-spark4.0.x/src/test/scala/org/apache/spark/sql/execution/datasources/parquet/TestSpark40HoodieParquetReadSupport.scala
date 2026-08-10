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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.junit.jupiter.api.{Assertions, Test}

class TestSpark40HoodieParquetReadSupport {

  /**
   * Validate that reorderVariantFields does not treat groups as variant when the value/metadata
   * fields fail the type checks in isVariantGroup. Each sub-group exercises a different false
   * branch of the short-circuit && chain.
   */
  @Test
  def testReorderVariantFieldsNonVariantGroupsUnchanged(): Unit = {
    val schema = Types.buildMessage()
      // value is non-primitive
      .addField(Types.requiredGroup()
        .addField(Types.requiredGroup().addField(Types.required(PrimitiveTypeName.INT32).named("x")).named("value"))
        .addField(Types.required(PrimitiveTypeName.BINARY).named("metadata"))
        .named("g1"))
      // value is primitive, metadata is non-primitive
      .addField(Types.requiredGroup()
        .addField(Types.required(PrimitiveTypeName.BINARY).named("value"))
        .addField(Types.requiredGroup().addField(Types.required(PrimitiveTypeName.INT32).named("x")).named("metadata"))
        .named("g2"))
      // both primitive but non-BINARY
      .addField(Types.requiredGroup()
        .addField(Types.required(PrimitiveTypeName.INT32).named("value"))
        .addField(Types.required(PrimitiveTypeName.INT32).named("metadata"))
        .named("g3"))
      // value is BINARY, metadata is non-BINARY primitive
      .addField(Types.requiredGroup()
        .addField(Types.required(PrimitiveTypeName.BINARY).named("value"))
        .addField(Types.required(PrimitiveTypeName.INT32).named("metadata"))
        .named("g4"))
      .named("test")

    val result = Spark40HoodieParquetReadSupport.reorderVariantFields(schema)
    Assertions.assertEquals(schema, result)
  }
}
