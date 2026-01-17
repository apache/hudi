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
import org.apache.hudi.common.schema.HoodieSchema

import org.apache.avro.JsonProperties
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestSchemaConverters {

  @Test
  def testAvroUnionConversion(): Unit = {
    val originalSchema = HoodieSchema.fromAvroSchema(HoodieMetadataColumnStats.SCHEMA$)

    val (convertedStructType, _) = HoodieSparkSchemaConverters.toSqlType(originalSchema)
    val convertedSchema = HoodieSparkSchemaConverters.toHoodieType(convertedStructType)

    // NOTE: Here we're validating that converting Avro -> Catalyst and Catalyst -> Avro are inverse
    //       transformations, but since it's not an easy endeavor to match Avro schemas, we match
    //       derived Catalyst schemas instead
    assertEquals(convertedStructType, HoodieSparkSchemaConverters.toSqlType(convertedSchema)._1)
    // validate that the doc string and default null value are set
    originalSchema.getFields.forEach { field =>
      val convertedField = convertedSchema.getField(field.name()).get()
      assertEquals(field.doc(), convertedField.doc())
      if (field.schema().isNullable) {
        assertEquals(JsonProperties.NULL_VALUE, convertedField.defaultVal().get())
      }
    }
  }
}
