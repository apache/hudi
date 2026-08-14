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
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.databind.util.RawValue
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
    // The variant branch below embeds its input verbatim once readTree accepts it, and readTree on
    // its own is happy to parse a valid prefix and leave the rest unconsumed.
    _mapper.configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true)
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
        case dt if dt.typeName == InternalRowToJsonStringConverter.VARIANT_TYPE_NAME =>
          // VariantVal.toString renders the variant as JSON; embed that rendering verbatim so the
          // image carries the variant's structure. Falling through to the default would serialize
          // the VariantVal bean, i.e. its raw value/metadata bytes as base64.
          // Matched on the type name rather than SparkAdapter.isVariantType: this guard is
          // evaluated for every non-string/array/map/struct field, and resolving the adapter
          // needs a version module that is not on hudi-spark-common's own test classpath.
          val variantJson = value.toString
          try {
            // readTree is a validation gate only, and its result is discarded: embedding the parsed
            // node instead would re-serialize the tree through the generator, and Jackson's write-side
            // nesting cap (StreamWriteConstraints, also 1000) is enforced by writeValueAsString in
            // convert, outside this block. A variant deep enough to clear the read limit but not the
            // write limit once the image's own object levels are added would fail the query there.
            // RawValue goes out through JsonGenerator.writeRawValue, which keeps no nesting context.
            val parsed = mapper.readTree(variantJson)
            // readTree accepts blank input as a MissingNode instead of throwing, and RawValue would
            // then emit nothing at all, leaving a malformed image. Trailing-token input is refused by
            // FAIL_ON_TRAILING_TOKENS above, which readTree does not check on its own.
            if (parsed == null || parsed.isMissingNode) variantJson else new RawValue(variantJson)
          } catch {
            // A variant can hold a field name, string or nesting depth past Jackson's default
            // StreamReadConstraints (50k chars, 20M chars, 1000 levels) while staying well inside
            // the variant size limit, and all three arrive here as StreamConstraintsException. A
            // CDC image is diagnostic data rather than the table's data, so keep the rendering as
            // a plain string instead of failing the query over it.
            // NOTE: value.toString is deliberately outside this block. It throws MALFORMED_VARIANT
            // on corrupt bytes, which is a data-integrity problem an operator has to see, not a
            // rendering quirk to paper over -- and there would be no rendering left to fall back to.
            case _: JsonProcessingException => variantJson
          }
        case _ =>
          // For primitive types and other unsupported types, return as is
          value
      }
    }
  }
}

object InternalRowToJsonStringConverter {

  /**
   * Type name of Spark's VariantType. Matched by name so this module, which also compiles
   * against Spark 3 where the type does not exist, needs neither the symbol nor a SparkAdapter.
   */
  private val VARIANT_TYPE_NAME = "variant"
}
