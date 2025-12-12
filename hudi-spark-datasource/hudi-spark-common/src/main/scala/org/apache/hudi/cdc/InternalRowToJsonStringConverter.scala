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
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StringType, StructType}
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
    // Use LinkedHashMap to preserve field order
    val map = scala.collection.mutable.LinkedHashMap.empty[String, Any]
    schema.zipWithIndex.foreach {
      case (field, idx) =>
        map(field.name) = convertField(record.get(idx, field.dataType), field.dataType)
    }
    UTF8String.fromString(mapper.writeValueAsString(map))
  }

  private def convertField(value: Any, dataType: DataType): Any = {
    if (value == null) {
      null
    } else {
      dataType match {
        case StringType => value.toString
        case ArrayType(elementType, _) =>
          value match {
            case arrayData: ArrayData =>
              val convertedArray = scala.collection.mutable.ArrayBuffer[Any]()
              for (i <- 0 until arrayData.numElements()) {
                val element = arrayData.get(i, elementType)
                convertedArray += convertField(element, elementType)
              }
              convertedArray.toArray
            case arr: Array[_] =>
              arr.map(item => convertField(item, elementType))
            case _ => value // fallback
          }
        case MapType(keyType, valueType, _) =>
          value match {
            case mapData: MapData =>
              val convertedMap = scala.collection.mutable.LinkedHashMap[Any, Any]()
              for (i <- 0 until mapData.numElements()) {
                val key = mapData.keyArray().get(i, keyType)
                val value = mapData.valueArray().get(i, valueType)
                convertedMap(convertField(key, keyType)) = convertField(value, valueType)
              }
              convertedMap.toMap
            case map: Map[_, _] =>
              map.map { case (k, v) => (convertField(k, keyType), convertField(v, valueType)) }
            case _ => value // fallback
          }
        case structType: StructType =>
          value match {
            case internalRow: InternalRow =>
              val structMap = scala.collection.mutable.LinkedHashMap[String, Any]()
              structType.zipWithIndex.foreach { case (field, idx) =>
                val fieldValue = internalRow.get(idx, field.dataType)
                structMap(field.name) = convertField(fieldValue, field.dataType)
              }
              structMap.toMap
            case _ => value // fallback
          }
        case _ =>
          // For primitive types and other unsupported types, return as is
          value
      }
    }
  }
}
