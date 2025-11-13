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

package org.apache.spark.sql.avro

import org.apache.hudi.avro.model.HoodieMetadataColumnStats

import org.apache.spark.sql.avro.SchemaConverters.SchemaType
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestSchemaConverters {

  /**
   * Helper method to strip metadata from DataType structures for comparison.
   * This allows us to compare the core schema structure while ignoring documentation/comments.
   */
  private def stripMetadata(dataType: DataType): DataType = dataType match {
    case StructType(fields) => StructType(fields.map(f =>
      StructField(f.name, stripMetadata(f.dataType), f.nullable, Metadata.empty)))
    case ArrayType(elementType, containsNull) => ArrayType(stripMetadata(elementType), containsNull)
    case MapType(keyType, valueType, valueContainsNull) =>
      MapType(stripMetadata(keyType), stripMetadata(valueType), valueContainsNull)
    case other => other
  }

  @Test
  def testAvroUnionConversion(): Unit = {
    val originalAvroSchema = HoodieMetadataColumnStats.SCHEMA$

    val SchemaType(convertedStructType, _, _) = SchemaConverters.toSqlType(originalAvroSchema)
    val convertedAvroSchema = SchemaConverters.toAvroType(convertedStructType)

    // NOTE: Here we're validating that converting Avro -> Catalyst and Catalyst -> Avro are inverse
    //       transformations for the core data structure. We strip metadata (comments/docs) since
    //       the toAvroType method doesn't preserve documentation from StructField metadata, making
    //       perfect round-trip conversion with docs challenging for complex union schemas.
    val firstConversion = stripMetadata(convertedStructType)
    val secondConversion = stripMetadata(SchemaConverters.toSqlType(convertedAvroSchema).dataType)

    assertEquals(firstConversion, secondConversion)
  }

}
