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

class TestHoodieParquetReadSupport {
  @Test
  def testSchemaTrimming_noRemainingFields(): Unit = {
    val requiredNestedField = Types.requiredGroup().addField(Types.required(PrimitiveTypeName.INT32).named("nested_a"))
    val dataNestedField = Types.requiredGroup().addField(Types.required(PrimitiveTypeName.INT32).named("nested_b"))
    val requiredArrayField = Types.requiredList().optionalGroupElement().addField(requiredNestedField.named("element")).named("list")
    val dataArrayField = Types.requiredList().optionalGroupElement().addField(dataNestedField.named("element")).named("list")
    val requiredMapField = Types.requiredMap().key(PrimitiveTypeName.BINARY).value(requiredNestedField.named("value")).named("key_value")
    val dataMapField = Types.requiredMap().key(PrimitiveTypeName.BINARY).value(dataNestedField.named("value")).named("key_value")
    val requiredSchema = Types.buildMessage()
        .addField(Types.required(PrimitiveTypeName.BINARY).named("a"))
        .addField(requiredNestedField.named("b"))
        .addField(requiredArrayField)
        .addField(requiredMapField)
        .addField(Types.required(PrimitiveTypeName.BINARY).named("e"))
        .named("required")
    val dataSchema = Types.buildMessage()
        .addField(Types.required(PrimitiveTypeName.BINARY).named("a"))
        .addField(dataNestedField.named("b"))
        .addField(dataArrayField)
        .addField(dataMapField)
        .addField(Types.required(PrimitiveTypeName.BINARY).named("e"))
        .named("data")

    val trimmedSchema = HoodieParquetReadSupport.trimParquetSchema(requiredSchema, dataSchema)

    val expectedSchema = Types.buildMessage()
        .addField(Types.required(PrimitiveTypeName.BINARY).named("a"))
        .addField(Types.required(PrimitiveTypeName.BINARY).named("e"))
        .named("required")

    Assertions.assertEquals(expectedSchema, trimmedSchema)
  }

  @Test
  def testSchemaTrimming_atLeastOneFieldMatches(): Unit = {
    val requiredNestedField = Types.requiredGroup().addField(Types.required(PrimitiveTypeName.INT32).named("nested_a"))
      .addField(Types.required(PrimitiveTypeName.INT32).named("nested_b"))
    val dataNestedField = Types.requiredGroup().addField(Types.required(PrimitiveTypeName.INT32).named("nested_b"))
      .addField(Types.required(PrimitiveTypeName.INT32).named("nested_c"))
    val requiredArrayField = Types.requiredList().optionalGroupElement().addField(requiredNestedField.named("element")).named("list")
    val dataArrayField = Types.requiredList().optionalGroupElement().addField(dataNestedField.named("element")).named("list")
    val requiredMapField = Types.requiredMap().key(PrimitiveTypeName.BINARY).value(requiredNestedField.named("value")).named("key_value")
    val dataMapField = Types.requiredMap().key(PrimitiveTypeName.BINARY).value(dataNestedField.named("value")).named("key_value")
    val requiredSchema = Types.buildMessage()
        .addField(Types.required(PrimitiveTypeName.BINARY).named("a"))
        .addField(requiredNestedField.named("b"))
        .addField(requiredArrayField)
        .addField(requiredMapField)
        .addField(Types.required(PrimitiveTypeName.BINARY).named("e"))
        .named("required")
    val dataSchema = Types.buildMessage()
        .addField(Types.required(PrimitiveTypeName.BINARY).named("a"))
        .addField(dataNestedField.named("b"))
        .addField(dataArrayField)
        .addField(dataMapField)
        .addField(Types.required(PrimitiveTypeName.BINARY).named("e"))
        .named("data")

    val trimmedSchema = HoodieParquetReadSupport.trimParquetSchema(requiredSchema, dataSchema)

    Assertions.assertEquals(requiredSchema, trimmedSchema)
  }
}
