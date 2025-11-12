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

package org.apache.hudi.cdc

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

class InternalRowToJsonStringConverter(schema: StructType) {

  private lazy val mapper: ObjectMapper = {
    val _mapper = new ObjectMapper
    _mapper.setSerializationInclusion(Include.NON_ABSENT)
    _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _mapper.registerModule(DefaultScalaModule)
    _mapper
  }

  def convert(record: InternalRow): UTF8String = {
    val map = scala.collection.mutable.Map.empty[String, Any]
    schema.zipWithIndex.foreach {
      case (field, idx) =>
        if (field.dataType.isInstanceOf[StringType]) {
          map(field.name) = Option(record.getUTF8String(idx)).map(_.toString).orNull
        } else {
          map(field.name) = record.get(idx, field.dataType)
        }
    }
    UTF8String.fromString(mapper.writeValueAsString(map))
  }
}
