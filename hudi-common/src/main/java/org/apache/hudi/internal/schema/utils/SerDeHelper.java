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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.JsonProperties;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SerDeHelper {
  private SerDeHelper() {

  }

  public static final String LATESTSCHEMA = "latestSchema";
  public static final String SCHEMAS = "schemas";
  private static final String MAX_COLUMN_ID = "max_column_id";
  private static final String VERSION_ID = "version-id";
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
  private static final String ELEMENT_ID = "element-id";
  private static final String KEY_ID = "key-id";
  private static final String VALUE_ID = "value-id";
  private static final String OPTIONAL = "optional";
  private static final String ELEMENT_OPTIONAL = "element_optional";
  private static final String VALUE_OPTIONAL = "value_optional";

  private static final Pattern FIXED = Pattern.compile("fixed\\[(\\d+)\\]");
  private static final Pattern DECIMAL = Pattern.compile("decimal\\((\\d+),\\s+(\\d+)\\)");

  /**
   * convert a java object to JsonNode.
   * refer to avro schema convert.
   *
   * @param datum a java object.
   * @return convert result.
   */
  public static JsonNode toJsonNode(Object datum) {
    if (datum == null) {
      return null;
    }
    try {
      TokenBuffer generator = new TokenBuffer(new ObjectMapper(), false);
      toJson(datum, generator);
      return new ObjectMapper().readTree(generator.asParser());
    } catch (IOException e) {
      throw new HoodieSchemaException(e);
    }
  }

  private static void toJson(Object datum, JsonGenerator generator) throws IOException {
    if (datum == JsonProperties.NULL_VALUE) { // null
      generator.writeNull();
    } else if (datum instanceof Map) { // record, map
      generator.writeStartObject();
      for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) datum).entrySet()) {
        generator.writeFieldName(entry.getKey().toString());
        toJson(entry.getValue(), generator);
      }
      generator.writeEndObject();
    } else if (datum instanceof Collection) { // array
      generator.writeStartArray();
      for (Object element : (Collection) datum) {
        toJson(element, generator);
      }
      generator.writeEndArray();
    } else if (datum instanceof byte[]) { // bytes, fixed
      generator.writeString(new String((byte[]) datum, StandardCharsets.ISO_8859_1));
    } else if (datum instanceof CharSequence || datum instanceof Enum) { // string, enum
      generator.writeString(datum.toString());
    } else if (datum instanceof Double) { // double
      generator.writeNumber((Double) datum);
    } else if (datum instanceof Float) { // float
      generator.writeNumber((Float) datum);
    } else if (datum instanceof Long) { // long
      generator.writeNumber((Long) datum);
    } else if (datum instanceof Integer) { // int
      generator.writeNumber((Integer) datum);
    } else if (datum instanceof Boolean) { // boolean
      generator.writeBoolean((Boolean) datum);
    } else {
      throw new HoodieSchemaException("Unknown datum class: " + datum.getClass());
    }
  }

  /**
   * check whether current JsonNode meet the requirements of the internal hudi type.
   *
   * @param type a internal hudi type.
   * @param defaultValue jsonNode.
   * @return check result.
   */
  public boolean isValidDefaultValue(Type type, JsonNode defaultValue) {
    if (defaultValue == null) {
      return true;
    }
    switch (type.typeId()) {
      case STRING:
      case BINARY:
      case FIXED:
        return defaultValue.isTextual();
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return defaultValue.isNumber();
      case BOOLEAN:
        return defaultValue.isBoolean();
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType)type;
        if (!defaultValue.isArray()) {
          return false;
        }
        for (JsonNode element : defaultValue) {
          if (!isValidDefaultValue(array.elementType(), element)) {
            return false;
          }
        }
        return true;
      case MAP:
        Types.MapType map = (Types.MapType)type;
        if (!defaultValue.isObject()) {
          return false;
        }
        for (JsonNode value : defaultValue) {
          if (!isValidDefaultValue(map.valueType(), value)) {
            return false;
          }
        }
        return true;
      case RECORD:
        Types.RecordType record = (Types.RecordType)type;
        if (!defaultValue.isObject()) {
          return false;
        }
        for (Types.Field f : record.fields()) {
          if (!isValidDefaultValue(f.type(), defaultValue.has(f.name()) ? defaultValue.get(f.name()) : toJsonNode(f.getDefaultValue()))) {
            return false;
          }
        }
        return true;
      default:
        return false;
    }
  }

  /**
   * convert jsonNode to a java object to meet the requirement of internal hudi type.
   * refer to avro schema convert code.
   *
   * @param jsonNode a jsonNode.
   * @param type a internal hudi type..
   * @return convert value.
   */
  public static Object toObject(JsonNode jsonNode, Type type) {
    if (jsonNode == null) {
      return null;
    } else if (jsonNode.isNull()) {
      return null;
    } else if (jsonNode.isBoolean()) {
      return jsonNode.asBoolean();
    } else if (jsonNode.isInt()) {
      if (type == null || type.typeId().equals(Type.TypeID.INT)) {
        return jsonNode.asInt();
      } else if (type.typeId().equals(Type.TypeID.LONG)) {
        return jsonNode.asLong();
      }
    } else if (jsonNode.isLong()) {
      return jsonNode.asLong();
    } else if (jsonNode.isDouble() || jsonNode.isFloat()) {
      if (type == null || type.typeId().equals(Type.TypeID.DOUBLE)) {
        return jsonNode.asDouble();
      } else if (type.typeId().equals(Type.TypeID.FLOAT)) {
        return (float) jsonNode.asDouble();
      }
    } else if (jsonNode.isTextual()) {
      if (type == null || type.typeId().equals(Type.TypeID.STRING)) {
        return jsonNode.asText();
      } else if (type.typeId().equals(Type.TypeID.BINARY) || type.typeId().equals(Type.TypeID.FIXED)) {
        return jsonNode.textValue().getBytes(StandardCharsets.ISO_8859_1);
      }
    } else if (jsonNode.isArray()) {
      List<Object> l = new ArrayList<>();
      for (JsonNode node : jsonNode) {
        l.add(toObject(node, type == null ? null : ((Types.ArrayType)type).elementType()));
      }
      return l;
    } else if (jsonNode.isObject()) {
      Map<Object, Object> m = new LinkedHashMap<>();
      for (Iterator<String> it = jsonNode.fieldNames(); it.hasNext();) {
        String key = it.next();
        final Type s;
        if (type != null && type.typeId().equals(Type.TypeID.MAP)) {
          s = ((Types.MapType)type).valueType();
        } else if (type != null && type.typeId().equals(Type.TypeID.RECORD)) {
          s = ((Types.RecordType)type).field(key).type();
        } else {
          s = null;
        }
        Object value = toObject(jsonNode.get(key), s);
        m.put(key, value);
      }
      return m;
    }
    return null;
  }

  /**
   * convert history internalSchemas to json.
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
   * convert internalSchemas to json.
   *
   * @param internalSchema a internal schema
   * @return a string
   */
  public static String toJson(InternalSchema internalSchema) {
    if (internalSchema == null) {
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
    toJson(internalSchema.getRecord(), internalSchema.getMax_column_id(), internalSchema.schemaId(), generator);
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
          throw new IllegalArgumentIOException(String.format("cannot write unknown types: %s", type));
        }
    }
  }

  private static Type parserTypeFromJson(JsonNode jsonNode) {
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
          Type type = parserTypeFromJson(field.get(TYPE));
          String doc = field.has(DOC) ? field.get(DOC).asText() : null;
          boolean optional = field.get(OPTIONAL).asBoolean();
          // build fields
          fields.add(Types.Field.get(id, optional, name, type, doc));
        }
        return Types.RecordType.get(fields);
      } else if (ARRAY.equals(typeStr)) {
        int elementId = jsonNode.get(ELEMENT_ID).asInt();
        Type elementType = parserTypeFromJson(jsonNode.get(ELEMENT));
        boolean optional = jsonNode.get(ELEMENT_OPTIONAL).asBoolean();
        return Types.ArrayType.get(elementId, optional, elementType);
      } else if (MAP.equals(typeStr)) {
        int keyId = jsonNode.get(KEY_ID).asInt();
        Type keyType = parserTypeFromJson(jsonNode.get(KEY));
        int valueId = jsonNode.get(VALUE_ID).asInt();
        Type valueType = parserTypeFromJson(jsonNode.get(VALUE));
        boolean optional = jsonNode.get(VALUE_OPTIONAL).asBoolean();
        return Types.MapType.get(keyId, valueId, keyType, valueType, optional);
      }
    }
    throw new IllegalArgumentException(String.format("cannot parse type from jsonNode: %s", jsonNode));
  }

  /**
   * convert jsonNode to internalSchema.
   *
   * @param jsonNode a jsonNode.
   * @return a internalSchema.
   */
  public static InternalSchema fromJson(JsonNode jsonNode) {
    Integer maxColumnId = !jsonNode.has(MAX_COLUMN_ID) ? null : jsonNode.get(MAX_COLUMN_ID).asInt();
    Long versionId = !jsonNode.has(VERSION_ID) ? null : jsonNode.get(VERSION_ID).asLong();
    Types.RecordType type = (Types.RecordType)parserTypeFromJson(jsonNode);
    if (versionId == null) {
      return new InternalSchema(type.fields());
    } else {
      if (maxColumnId != null) {
        return new InternalSchema(versionId, maxColumnId, type.fields());
      } else {
        return new InternalSchema(versionId, type.fields());
      }
    }
  }

  /**
   * convert string to internalSchema.
   *
   * @param json a json string.
   * @return a internalSchema.
   */
  public static Option<InternalSchema> fromJson(String json) {
    if (json == null || json.isEmpty()) {
      return Option.empty();
    }
    try {
      return Option.of(fromJson((new ObjectMapper(new JsonFactory())).readValue(json, JsonNode.class)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * convert json string to history internalSchemas.
   * TreeMap is used to hold history internalSchemas.
   *
   * @param json a json string
   * @return a TreeMap
   */
  public static TreeMap<Long, InternalSchema> parseSchemas(String json) {
    TreeMap<Long, InternalSchema> result = new TreeMap<>();
    try {
      JsonNode jsonNode = (new ObjectMapper(new JsonFactory())).readValue(json, JsonNode.class);
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
      throw new RuntimeException(e);
    }
    return result;
  }

  /**
   * search target internalSchema by version number.
   *
   * @param versionId the internalSchema version to be search.
   * @param internalSchemas internalSchemas to be searched.
   * @return a internalSchema.
   */
  public static InternalSchema searchSchema(long versionId, List<InternalSchema> internalSchemas) {
    TreeMap<Long, InternalSchema> treeMap = new TreeMap<>();
    internalSchemas.forEach(s -> treeMap.put(s.schemaId(), s));
    return searchSchema(versionId, treeMap);
  }

  /**
   * search target internalSchema by version number.
   *
   * @param versionId the internalSchema version to be search.
   * @param treeMap internalSchemas collections to be searched.
   * @return a internalSchema.
   */
  public static InternalSchema searchSchema(long versionId, TreeMap<Long, InternalSchema> treeMap) {
    if (treeMap.containsKey(versionId)) {
      return treeMap.get(versionId);
    } else {
      SortedMap<Long, InternalSchema> headMap = treeMap.headMap(versionId);
      if (!headMap.isEmpty()) {
        return headMap.get(headMap.lastKey());
      }
    }
    return null;
  }

  /**
   * add the new schema to the historical schemas.
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

