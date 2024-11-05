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

package org.apache.hudi.internal.schema.utils;

import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utils of serialization and deserialization.
 */
public class SerDeHelper {
  private SerDeHelper() {

  }

  public static final String LATEST_SCHEMA = "latest_schema";
  public static final String SCHEMAS = "schemas";
  private static final String MAX_COLUMN_ID = "max_column_id";
  private static final String VERSION_ID = "version_id";
  private static final String TYPE = "type";
  private static final String RECORD = "record";
  private static final String ARRAY = "array";
  private static final String MAP = "map";
  private static final String FIELDS = "fields";
  private static final String ELEMENT = "element";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String DOC = "doc";
  private static final String NAME = "name";
  private static final String ID = "id";
  private static final String ELEMENT_ID = "element_id";
  private static final String KEY_ID = "key_id";
  private static final String VALUE_ID = "value_id";
  private static final String OPTIONAL = "optional";
  private static final String ELEMENT_OPTIONAL = "element_optional";
  private static final String VALUE_OPTIONAL = "value_optional";

  private static final Pattern FIXED = Pattern.compile("fixed\\[(\\d+)\\]");
  private static final Pattern DECIMAL = Pattern.compile("decimal\\((\\d+),\\s+(\\d+)\\)");

  /**
   * Convert history internalSchemas to json.
   * this is used when save history schemas into hudi.
   *
   * @param internalSchemas history internal schemas
   * @return a string
   */
  public static String toJson(List<InternalSchema> internalSchemas) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = (new JsonFactory()).createGenerator(writer);
      generator.writeStartObject();
      generator.writeArrayFieldStart(SCHEMAS);
      for (InternalSchema schema : internalSchemas) {
        toJson(schema, generator);
      }
      generator.writeEndArray();
      generator.writeEndObject();
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert internalSchemas to json.
   *
   * @param internalSchema a internal schema
   * @return a string
   */
  public static String toJson(InternalSchema internalSchema) {
    if (internalSchema == null || internalSchema.isEmptySchema()) {
      return "";
    }
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = (new JsonFactory()).createGenerator(writer);
      toJson(internalSchema, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void toJson(InternalSchema internalSchema, JsonGenerator generator) throws IOException {
    toJson(internalSchema.getRecord(), internalSchema.getMaxColumnId(), internalSchema.schemaId(), generator);
  }

  private static void toJson(Types.RecordType record, Integer maxColumnId, Long versionId, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    if (maxColumnId != null) {
      generator.writeNumberField(MAX_COLUMN_ID, maxColumnId);
    }
    if (versionId != null) {
      generator.writeNumberField(VERSION_ID, versionId);
    }
    generator.writeStringField(TYPE, RECORD);
    generator.writeArrayFieldStart(FIELDS);
    for (Types.Field field : record.fields()) {
      generator.writeStartObject();
      generator.writeNumberField(ID, field.fieldId());
      generator.writeStringField(NAME, field.name());
      generator.writeBooleanField(OPTIONAL, field.isOptional());
      generator.writeFieldName(TYPE);
      toJson(field.type(), generator);
      if (field.doc() != null) {
        generator.writeStringField(DOC, field.doc());
      }
      generator.writeEndObject();
    }
    generator.writeEndArray();
    generator.writeEndObject();
  }

  private static void toJson(Type type, JsonGenerator generator) throws IOException {
    switch (type.typeId()) {
      case RECORD:
        toJson((Types.RecordType) type, null, null, generator);
        break;
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        generator.writeStartObject();
        generator.writeStringField(TYPE, ARRAY);
        generator.writeNumberField(ELEMENT_ID, array.elementId());
        generator.writeFieldName(ELEMENT);
        toJson(array.elementType(), generator);
        generator.writeBooleanField(ELEMENT_OPTIONAL, array.isElementOptional());
        generator.writeEndObject();
        break;
      case MAP:
        Types.MapType map = (Types.MapType) type;
        generator.writeStartObject();
        generator.writeStringField(TYPE, MAP);
        generator.writeNumberField(KEY_ID, map.keyId());
        generator.writeFieldName(KEY);
        toJson(map.keyType(), generator);
        generator.writeNumberField(VALUE_ID, map.valueId());
        generator.writeFieldName(VALUE);
        toJson(map.valueType(), generator);
        generator.writeBooleanField(VALUE_OPTIONAL, map.isValueOptional());
        generator.writeEndObject();
        break;
      default:
        if (!type.isNestedType()) {
          generator.writeString(type.toString());
        } else {
          throw new HoodieIOException(String.format("cannot write unknown types: %s", type));
        }
    }
  }

  private static Type parseTypeFromJson(JsonNode jsonNode) {
    if (jsonNode.isTextual()) {
      String type = jsonNode.asText().toLowerCase(Locale.ROOT);
      // deal with fixed and decimal
      Matcher fixed = FIXED.matcher(type);
      if (fixed.matches()) {
        return Types.FixedType.getFixed(Integer.parseInt(fixed.group(1)));
      }
      Matcher decimal = DECIMAL.matcher(type);
      if (decimal.matches()) {
        return Types.DecimalType.get(
            Integer.parseInt(decimal.group(1)),
            Integer.parseInt(decimal.group(2)));
      }
      // deal with other type
      switch (Type.fromValue(type)) {
        case BOOLEAN:
          return Types.BooleanType.get();
        case INT:
          return Types.IntType.get();
        case LONG:
          return Types.LongType.get();
        case FLOAT:
          return Types.FloatType.get();
        case DOUBLE:
          return Types.DoubleType.get();
        case DATE:
          return Types.DateType.get();
        case TIME:
          return Types.TimeType.get();
        case TIMESTAMP:
          return Types.TimestampType.get();
        case STRING:
          return Types.StringType.get();
        case UUID:
          return Types.UUIDType.get();
        case BINARY:
          return Types.BinaryType.get();
        default:
          throw new IllegalArgumentException("cannot parser types from jsonNode");
      }
    } else if (jsonNode.isObject()) {
      String typeStr = jsonNode.get(TYPE).asText();
      if (RECORD.equals(typeStr)) {
        JsonNode fieldNodes = jsonNode.get(FIELDS);
        Iterator<JsonNode> iter = fieldNodes.elements();
        List<Types.Field> fields = new ArrayList<>();
        while (iter.hasNext()) {
          JsonNode field = iter.next();
          // extract
          int id = field.get(ID).asInt();
          String name = field.get(NAME).asText();
          Type type = parseTypeFromJson(field.get(TYPE));
          String doc = field.has(DOC) ? field.get(DOC).asText() : null;
          boolean optional = field.get(OPTIONAL).asBoolean();
          // build fields
          fields.add(Types.Field.get(id, optional, name, type, doc));
        }
        return Types.RecordType.get(fields);
      } else if (ARRAY.equals(typeStr)) {
        int elementId = jsonNode.get(ELEMENT_ID).asInt();
        Type elementType = parseTypeFromJson(jsonNode.get(ELEMENT));
        boolean optional = jsonNode.get(ELEMENT_OPTIONAL).asBoolean();
        return Types.ArrayType.get(elementId, optional, elementType);
      } else if (MAP.equals(typeStr)) {
        int keyId = jsonNode.get(KEY_ID).asInt();
        Type keyType = parseTypeFromJson(jsonNode.get(KEY));
        int valueId = jsonNode.get(VALUE_ID).asInt();
        Type valueType = parseTypeFromJson(jsonNode.get(VALUE));
        boolean optional = jsonNode.get(VALUE_OPTIONAL).asBoolean();
        return Types.MapType.get(keyId, valueId, keyType, valueType, optional);
      }
    }
    throw new IllegalArgumentException(String.format("cannot parse type from jsonNode: %s", jsonNode));
  }

  /**
   * Convert jsonNode to internalSchema.
   *
   * @param jsonNode a jsonNode.
   * @return a internalSchema.
   */
  public static InternalSchema fromJson(JsonNode jsonNode) {
    Integer maxColumnId = !jsonNode.has(MAX_COLUMN_ID) ? null : jsonNode.get(MAX_COLUMN_ID).asInt();
    Long versionId = !jsonNode.has(VERSION_ID) ? null : jsonNode.get(VERSION_ID).asLong();
    Types.RecordType type = (Types.RecordType) parseTypeFromJson(jsonNode);
    if (versionId == null) {
      return new InternalSchema(type);
    } else {
      if (maxColumnId != null) {
        return new InternalSchema(versionId, maxColumnId, type);
      } else {
        return new InternalSchema(versionId, type);
      }
    }
  }

  /**
   * Convert string to internalSchema.
   *
   * @param json a json string.
   * @return a internalSchema.
   */
  public static Option<InternalSchema> fromJson(String json) {
    if (json == null || json.isEmpty()) {
      return Option.empty();
    }
    try {
      return Option.of(fromJson(JsonUtils.getObjectMapper().readTree(json)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert json string to history internalSchemas.
   * TreeMap is used to hold history internalSchemas.
   *
   * @param json a json string
   * @return a TreeMap
   */
  public static TreeMap<Long, InternalSchema> parseSchemas(String json) {
    TreeMap<Long, InternalSchema> result = new TreeMap<>();
    try {
      JsonNode jsonNode = JsonUtils.getObjectMapper().readTree(json);
      if (!jsonNode.has(SCHEMAS)) {
        throw new IllegalArgumentException(String.format("cannot parser schemas from current json string, missing key name: %s", SCHEMAS));
      }
      JsonNode schemas = jsonNode.get(SCHEMAS);
      Iterator<JsonNode> iter = schemas.elements();
      while (iter.hasNext()) {
        JsonNode schema = iter.next();
        InternalSchema current = fromJson(schema);
        result.put(current.schemaId(), current);
      }
    } catch (IOException e) {
      throw new HoodieException(e);
    }
    return result;
  }

  /**
   * Add the new schema to the historical schemas.
   * use string operations to reduce overhead.
   *
   * @param newSchema a new internalSchema
   * @param oldSchemas historical schemas string.
   * @return a string.
   */
  public static String inheritSchemas(InternalSchema newSchema, String oldSchemas) {
    if (newSchema == null) {
      return "";
    }
    if (oldSchemas == null || oldSchemas.isEmpty()) {
      return toJson(Arrays.asList(newSchema));
    }
    String checkedString = "{\"schemas\":[";
    if (!oldSchemas.startsWith("{\"schemas\":")) {
      return "";
    }
    String oldSchemasSuffix = oldSchemas.substring(checkedString.length());
    return checkedString + toJson(newSchema) + "," + oldSchemasSuffix;
  }
}

