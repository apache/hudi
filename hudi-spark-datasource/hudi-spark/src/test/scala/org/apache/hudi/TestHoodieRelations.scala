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

import org.apache.avro.Schema
import org.apache.hudi.AvroConversionUtils.convertAvroSchemaToStructType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import scala.collection.JavaConverters.asScalaBufferConverter

class TestHoodieRelations {

  @Test
  def testPruningSchema(): Unit = {
    val avroSchemaString = "{\"type\":\"record\",\"name\":\"record\"," +
      "\"fields\":[{\"name\":\"_hoodie_commit_time\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
      "{\"name\":\"_hoodie_commit_seqno\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
      "{\"name\":\"_hoodie_record_key\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
      "{\"name\":\"_hoodie_partition_path\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
      "{\"name\":\"_hoodie_file_name\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
      "{\"name\":\"uuid\",\"type\":\"string\"},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}," +
      "{\"name\":\"age\",\"type\":[\"null\",\"int\"],\"default\":null}," +
      "{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null}," +
      "{\"name\":\"partition\",\"type\":[\"null\",\"string\"],\"default\":null}]}"

    val tableAvroSchema = new Schema.Parser().parse(avroSchemaString)
    val tableStructSchema = convertAvroSchemaToStructType(tableAvroSchema)

    val (requiredAvroSchema, requiredStructSchema, _) =
      HoodieBaseRelation.projectSchema(tableAvroSchema, Array("ts"))

    assertEquals(Seq(tableAvroSchema.getField("ts")), requiredAvroSchema.getFields.asScala)
    assertEquals(
      Seq(tableStructSchema.fields.apply(tableStructSchema.fieldIndex("ts"))),
      requiredStructSchema.fields.toSeq
    )
  }


}
