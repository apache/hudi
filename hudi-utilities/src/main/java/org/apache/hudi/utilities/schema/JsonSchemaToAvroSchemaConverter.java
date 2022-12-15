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

package org.apache.hudi.utilities.schema;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonSchemaToAvroSchemaConverter {
  public static final String ONE_OF = "oneOf";
  public static final String TYPE = "type";
  public static final String NAME = "name";
  public static final String PROPERTIES = "properties";
  public static final String FIELDS = "fields";
  public static final String RECORD = "record";
  public static final String MARQETA_JCARD_NAMESPACE = "com.marqeta.jcard";

  /**
   * Handles the oneOf special datatype. Returns an array of [ null, object ]
   * @param jsonArray
   * @param name
   * @return
   * @throws Exception
   */
  private static JsonArray handleOneOf(JsonArray jsonArray, String name) throws Exception {
    JsonArray types = new JsonArray();
    for (JsonElement oneOfs: jsonArray) {
      String oneOfType = oneOfs.getAsJsonObject().get(TYPE).getAsString();
      // if  type is not null it's a json object
      if (oneOfType.equalsIgnoreCase("null")) {
        // handle null
        types.add("null");
      } else {
        Object resp = jsonTypeToAvroType((JsonObject) oneOfs);
        if (resp instanceof String) {
          // handle simple datatype
          types.add((String) resp);
        } else {
          // handle object
          JsonObject avroObject = new JsonObject();
          avroObject.addProperty(NAME, name);
          avroObject.addProperty(TYPE, RECORD);
          avroObject.add(FIELDS, (JsonArray) resp);
          types.add(avroObject);
        }
      }
    }
    return types;
  }

  /**
   * Maps the json data types to avro data types. Does not handle array as of now
   * @param property json datatype
   * @return Object avro datatype
   * @throws Exception
   */
  private static Object jsonTypeToAvroType(JsonObject property) throws Exception {
    String jsonType = property.get("type").getAsString();

    switch (jsonType) {
      case "integer":
        return "long";
      case "string":
        return "string";
      case "number":
        return "double";
      case "boolean":
        return "boolean";
      case "object":
        return jsonPropertiesToAvro(property.get(PROPERTIES).getAsJsonObject());
      // TODO: handle json array
      default:
        throw new Exception("Unhandled datatype found: " + jsonType);
    }

  }

  /**
   * Converts the properties object from the json schema to avro.
   * @param properties
   * @return avro fields array
   */
  public static JsonArray jsonPropertiesToAvro(JsonObject properties) {
    Set<Map.Entry<String, JsonElement>> propSet = properties.entrySet();
    JsonArray avroFields = new JsonArray();
    try {
      for (Map.Entry<String, JsonElement> entry : propSet) {
        String propName = entry.getKey();
        JsonObject propValue = entry.getValue().getAsJsonObject();
        JsonObject avroObject = new JsonObject();
        avroObject.addProperty(NAME, propName);
        if (propValue.has(ONE_OF)) {
          // for oneOf we will have an array of type
          avroObject.add(TYPE, (handleOneOf(propValue.get(ONE_OF).getAsJsonArray(), propName)));
        } else {
          Object resp = jsonTypeToAvroType(propValue);
          if (resp instanceof String) {
            // simple datatype
            avroObject.addProperty(TYPE, (String) jsonTypeToAvroType(propValue));
          } else {
            // record
            JsonObject avroRecord = new JsonObject();
            avroRecord.addProperty(NAME, propName);
            avroRecord.addProperty(TYPE, RECORD);
            avroRecord.add(FIELDS, (JsonElement) resp);
            avroObject.add(TYPE, avroRecord);
          }
        }
        avroFields.add(avroObject);
      }
    } catch (Exception e) {
      System.out.println("exception: " + e.getCause().toString());
    }
    return avroFields;

  }

  /**
   * Converts a jsonSchema read from the confluent schema registry to avro schema
   * @param jsonSchema event schema in JSON format
   * @return avroschema event schema in AVRO format
   */
  public static String convertJsonSchemaToAvroSchema(String jsonSchema) throws IOException {
    JsonParser jsonParser = new JsonParser();
    JsonElement jsonTree = jsonParser.parse(jsonSchema);
    JsonObject avroSchema = new JsonObject();

    if (jsonTree.isJsonObject()) {
      JsonObject jo = jsonTree.getAsJsonObject();
      String name = getTableName(jo.get("title").getAsString());

      JsonObject properties = jo.get(PROPERTIES).getAsJsonObject();
      avroSchema.addProperty(NAME, name);
      avroSchema.addProperty(TYPE, RECORD);
      avroSchema.addProperty("namespace", MARQETA_JCARD_NAMESPACE);
      avroSchema.add(FIELDS, jsonPropertiesToAvro(properties));
      return avroSchema.toString();
    }
    throw new IOException("Invalid json");
  }

  /**
   * Extract the table name from the topic name which is used as the avro schema.
   * @param title topic name
   * @return table name
   */
  private static String getTableName(String title) {
    Pattern pattern = Pattern.compile("cdc_marqeta_jcard_(.*).Envelope");
    Matcher matcher = pattern.matcher(title);
    return matcher.find() ? matcher.group(1) : null;
  }
}
