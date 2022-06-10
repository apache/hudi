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

package org.apache.hudi

import com.fasterxml.jackson.core.{JsonParseException, JsonToken}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.io._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericArray, GenericData, GenericRecord}

import scala.collection.JavaConverters._

/**
 * This utility will convert topics of many gzip files of JSON lines into Avro.
 * It doesn't work for generic json. It only works for those specific json with specific assumptions below.
 *
 * 1. For complex types, the JSON string can only has "record" and "array"
 *
 * 2. Below are supported types
 *    {"name": "ts"   , "type": "double", "default": 0 }, // "type" node JsonToken is STRING. Value is string "double".
 *    {"name": "host" , "type": "string", "default": ""}, // "type" token is STRING. value is "string"
 *    {"name": "level", "type": ["string","null"], "default": ""}, // "type" token is START_ARRAY.
 *    {"name": "msg" ,
 *     "type": { // This "type" node JsonToken is START_OBJECT. Value is JsonNode.
 *        "type": "record",
 *  , {
 *     "name" : "minionIds",
 *     "type" : [ "null", {
 *        "type" : "array",
 *        "items" : "int" // "array" can be primitive types or "record". No mixed types in one array.
 *       } ],             // Don't support array of array.
 *     "default" : null
 *    },
 *
 * 3. If the field is optional and the JSON value is missing, null is put in.
 *
 * 4. There are 3 rules of array type.
 * 4.1 In json string, if the key exists and it's an empty array, serialize empty array into GenericArray.
 * 4.2 In json string, if the key exists and it's a null value, serialize null into GenericArray.
 * 4.3 If the key is missing in json string, serialize null into GenericArray.
 *
 * 5. "items" : "array" of api.created_trips (from schema inference) became below.
 *    {
 *       "name" : "pickup_points_to_begintrip",
 *       "type" : [ "null", {
 *         "type" : "array",
 *         "items" : "int"
 *       } ],
 *       "default" : null
 *    }
 *
 * 6. Later, it was found out that api.client_transactions has enum type. It's confirmed it's the only topic with enum.
 *    So, I enhanced this converter and now it can handle enum inside record. It's enum of strings.
 *    No other cases are supported.
 * 6.1 In SparkSQL, the enum shows as binary type and return null. Don't know why yet.
 * 6.2 I used String but not GenericData.EnumSymbol because "The Hadoop Definite Guide" has a table showing that
 *     Java String is the mapping type for Avro Enum.
 *
 */

object JsonAvroConverter {
  val mapper = new ObjectMapper()

  def getSchemaRootFromFile(reader: Reader) =
    mapper.readTree(reader)

  // Check if it's "record" or "array" type when START_OBJECT is met.
  def getStartObjectAvroType(jsonNode: JsonNode): String = {
    if (jsonNode.asToken() != JsonToken.START_OBJECT || !jsonNode.has("type"))
      throw new RuntimeException("Unsupported Avro schema configuration")

    val avroType: JsonNode = jsonNode.get("type")

    if (avroType.asToken() != JsonToken.VALUE_STRING ||
         (avroType.asText() != "record" && avroType.asText() != "array" && avroType.asText() != "enum"))
      throw new RuntimeException("Unsupported Avro schema configuration. Type %s. Json %s"
        .format(avroType.asToken().toString, jsonNode.toString)) // must return "record or "array"

    avroType.asText()
  }

  /**
   *
   * @param inputNode
   * @return (is optional field, type of the node)
   */
  def getAvroType(inputNode: JsonNode): (Boolean, String) = {
    val typeNode: JsonNode =
      if (inputNode.has("items")) {
        // inputNode is "array"
        inputNode.get("items")
      } else {
        // "record" or primitive types (or "enum"?)
        inputNode.get("type")
      }

    typeNode.asToken() match {
      case JsonToken.VALUE_STRING =>
        (false, typeNode.asText()) // The type is "record", "string", "double", "boolean", etc
      case JsonToken.START_OBJECT =>
        (false, getStartObjectAvroType(typeNode))
      case JsonToken.START_ARRAY =>
        // In our schema, it's type of optional field. e.g. "type": ["string","null"]
        // Actual array type is "type" : "array"
        val types: Seq[String] =
          typeNode.elements.asScala.map { node: JsonNode =>
            if (node.asToken() != JsonToken.VALUE_STRING &&
              node.asToken() != JsonToken.START_OBJECT)
              throw new RuntimeException("Found unsupported type %s in option field".format(node.asToken()))

            if (node.asToken() == JsonToken.VALUE_STRING)
              node.asText()
            else
              getStartObjectAvroType(node)
          }.toSeq

        if (types.size != 2 || !types.contains("null"))
          throw new RuntimeException("Optional type must have a null and a type")

        if (types(0) != "null")
          (true, types(0))
        else
          (true, types(1))
      case _ =>
        throw new RuntimeException("Unknown Avro type %s of node %s".format(typeNode.asToken(), inputNode.toString))
    }
  }

  def getStartObjectSchemaNode(schemaNode: JsonNode, isOptional: Boolean): JsonNode = {
    def getChildNode(schemaNode: JsonNode): JsonNode =
      if (schemaNode.has("items")) {
        // It's "array"
        schemaNode.get("items")
      } else {
        // It's "record" (or "enum"?)
        schemaNode.get("type")
      }

    if (!isOptional)
      getChildNode(schemaNode)
    else {
      getChildNode(schemaNode).elements().asScala.filter { node =>
        node.asToken() != JsonToken.VALUE_STRING
      }.toSeq(0)
    }
  }

  def visitPrimitiveTypeValue(primitiveType: String, node: JsonNode, fieldName: String): Any = {
    if (node.asToken() == JsonToken.VALUE_NULL) {
      // e.g. "inviter_id":null
      // Note: Above example is not the same as "inviter_id":"null"
      return null
    }

    var text = node.asText()
    if (primitiveType.equals("string")) {
      return text // return as is.
    }

    // Prepare for parsing into other types.
    if (text != null) {
      text = text.trim
    }
    
    try {
      primitiveType match {
        case "int" => {
          java.lang.Integer.parseInt(text)
        }
        case "long" => {
          java.lang.Long.parseLong(text)
        }
        case "double" => {
          java.lang.Double.parseDouble(text)
        }
        case "boolean" => {
          text = text.toLowerCase()
          if (text.equalsIgnoreCase("true")) {
            true
          } else if (text.equalsIgnoreCase("false")) {
            false
          } else {
            throw new InvalidJsonException("Record value \"%s\" is not boolean. Field \"%s\""
              .format(node.asText, fieldName))
          }
        }
        case _ => {
          throw new RuntimeException("Unknown type %s. Getting primitive. Field \"%s\""
            .format(primitiveType, fieldName))
        }
      }
    } catch {
      // For int, long and double types. The exception of boolean type is thrown above.
      case e: NumberFormatException =>
        throw new InvalidJsonException("Record value \"%s\" is not %s. Field \"%s\""
          .format(node.asText, primitiveType, fieldName))
    }
  }

  def visitArray(schemaRoot: JsonNode, arrayNode: JsonNode, fieldName: String): GenericArray[Any] = {
    // Below can be confusing:
    // The syntax of "array" may be
    // "type" : "array",
    // "items" : {
    //   "type" : "record", ...
    // or
    // "type" : "array",
    // "items" : {
    //   "type" : "double", ...
    // There is no "items" : [ "null", { ... } ] or  "items" : [ "null", "double" ].
    // But, by definition, the values inside are optional, i.e. the array can be empty.
    // So, isOptional below will always equal false because it only checks the syntax.
    // But, the semantic is always true.
    val (isOptional, fieldType: String) = getAvroType(schemaRoot)
    val arrayElements: Seq[Any] =
      if (arrayNode.elements().hasNext) {
        if (fieldType == "record") {
          val elementSchemaRoot = getStartObjectSchemaNode(schemaRoot, isOptional)
          arrayNode.elements().asScala.map { node =>
            visitRecord(elementSchemaRoot, node)
          }.toSeq
        } else {
          // Mixed types can't exist inside array.
          arrayNode.elements().asScala.map { node =>
            visitPrimitiveTypeValue(fieldType, node, fieldName)
          }.toSeq
        }
      } else {
        // As discussed above, the values inside array are always optional
        Seq.empty[Any] // Rule 4.1 of array. See the comment at the beginning.
      }

    val avroSchema = new Schema.Parser().parse(schemaRoot.toString)
    // TODO: Is [Any] correct below? I think it is fine because Avro will write correctly based on the avroSchema.
    val result: GenericData.Array[Any] = new GenericData.Array[Any](arrayElements.size, avroSchema)
    arrayElements.foreach { element =>
      result.add(element)
    }
    result
  }

  def visitEnum(schemaRoot: JsonNode, jsonRoot: JsonNode): String = {
    val isValid: Boolean = schemaRoot.get("symbols").elements().asScala.exists { node: JsonNode =>
      node.asText() == jsonRoot.asText()
    }

    if (isValid) {
      val avroSchema = new Schema.Parser().parse(schemaRoot.toString)
      jsonRoot.asText()
//      new GenericData.EnumSymbol(avroSchema, jsonRoot.asText())
    } else
      throw new InvalidJsonException("Invalid enum value. Schema %s. Json %s. "
        .format(schemaRoot.toString, jsonRoot.toString))

  }

  def visitRecord(schemaRoot: JsonNode, jsonString: String): GenericRecord =
    try {
      visitRecord(schemaRoot, mapper.readTree(jsonString))
    } catch {
      case e: JsonParseException =>
        throw new InvalidJsonException("JsonParseException happened when processing record %s".format(jsonString))
      case e: Exception =>
        throw new InvalidJsonException("Exception happened when processing record %s".format(jsonString))
    }

  def visitRecord(schemaRoot: JsonNode, jsonRoot: JsonNode): GenericRecord = {
    // schemaRoot should have "type": "record". It should also have "name": "sortsol_record" and "fields": [ ...]
    val avroSchema = new Schema.Parser().parse(schemaRoot.toString)
    val record: GenericRecord = new GenericData.Record(avroSchema)
    schemaRoot.get("fields").elements().asScala.foreach { schemaFieldNode: JsonNode =>
      val fieldName: String = schemaFieldNode.get("name").asText()
      val (isOptional: Boolean, fieldType: String) = getAvroType(schemaFieldNode)
      val value: Any =
        if (jsonRoot.has(fieldName)) {
          val currentJsonFieldNode = jsonRoot.get(fieldName)
          if (currentJsonFieldNode == null) {
            null // Field value is null. And, for array, Rule 4.2 and see the comment at the beginning.
          } else {
            if (fieldType == "array") {
              visitArray(getStartObjectSchemaNode(schemaFieldNode, isOptional), currentJsonFieldNode, fieldName)
            } else if (fieldType == "record") {
              visitRecord(getStartObjectSchemaNode(schemaFieldNode, isOptional), currentJsonFieldNode)
            } else if (fieldType == "enum") {
              visitEnum(getStartObjectSchemaNode(schemaFieldNode, isOptional), currentJsonFieldNode)
//              "CREDIT_DEPOSIT" // Tried this already. It was still null in SparkSQL.
            } else {
              visitPrimitiveTypeValue(fieldType, currentJsonFieldNode, fieldName)
            }
          }
        } else if (isOptional)
          null // In case of array, Rule 4.3 is also handled here. See the comment at the beginning.
        else
          throw new InvalidJsonException("Record mandatory field %s missing".format(fieldName))

      record.put(fieldName, value)
    }

    record
  }
}

case class InvalidJsonException(message: String) extends Exception(message: String)