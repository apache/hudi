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

package org.apache.hudi.common.schema.evolution;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pure-{@link HoodieSchema} JSON serializer/deserializer for evolution schemas.
 *
 * <p>The on-disk format is a hard backward-compatibility boundary: the
 * {@code latest_schema} blob in commit metadata and the {@code .hoodie/.schema/}
 * history files have a fixed JSON layout that predates this class, and old
 * tables must remain readable. This class emits that JSON layout byte-for-byte
 * and parses every shape the original could parse, including:
 *
 * <ul>
 *   <li>Top-level record with {@code max_column_id} / {@code version_id} /
 *       {@code type:"record"} / {@code fields:[...]}.</li>
 *   <li>Inline records (only {@code type:"record"} + {@code fields:[...]} —
 *       no version metadata).</li>
 *   <li>Arrays with {@code element_id}, {@code element}, {@code element_optional}.</li>
 *   <li>Maps with {@code key_id}, {@code key}, {@code value_id}, {@code value},
 *       {@code value_optional}.</li>
 *   <li>Primitives serialized as a string token: {@code "int"}, {@code "long"},
 *       {@code "boolean"}, {@code "string"}, {@code "binary"}, {@code "date"},
 *       {@code "uuid"}, {@code "time"} (micros), {@code "time-millis"},
 *       {@code "timestamp"} (micros), {@code "timestamp-millis"},
 *       {@code "local-timestamp-millis"}, {@code "local-timestamp-micros"},
 *       {@code "fixed[N]"}, {@code "decimal_bytes(P,S)"}, {@code "decimal_fixed(P,S)[N]"},
 *       and the legacy {@code "decimal(P, S)"} form (parsed only — emission
 *       always picks one of the two explicit decimal forms).</li>
 * </ul>
 *
 * <p>Field ids are emitted as the {@code id} key on each field object and
 * stamped on parse onto the resulting HoodieSchemaField as the
 * {@code field-id} Avro custom property; element / key / value ids likewise
 * land as {@code element-id} / {@code key-id} / {@code value-id} on the
 * corresponding nested HoodieSchema's underlying Avro schema.
 */
public final class HoodieSchemaSerDe {

  // Wire format keys reused by callers reading commit metadata. Any change here
  // breaks compatibility with on-disk tables; do not adjust.
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

  // Match patterns are checked in the order: DECIMAL_FIXED first (otherwise its
  // suffix fixed[N] would partially match FIXED), then FIXED, then DECIMAL_BYTES,
  // and finally the legacy plain DECIMAL form. The DECIMAL pattern requires
  // whitespace after the comma — that matches what Types.DecimalType.toString()
  // historically emitted ("decimal(p, s)" with a space).
  private static final Pattern FIXED_PATTERN = Pattern.compile("fixed\\[(\\d+)\\]");
  private static final Pattern DECIMAL_PATTERN = Pattern.compile("decimal\\((\\d+),\\s+(\\d+)\\)");
  private static final Pattern DECIMAL_BYTES_PATTERN = Pattern.compile("decimal_bytes\\((\\d+),\\s*(\\d+)\\)");
  private static final Pattern DECIMAL_FIXED_PATTERN = Pattern.compile("decimal_fixed\\((\\d+),\\s*(\\d+)\\)\\[(\\d+)\\]");

  private HoodieSchemaSerDe() {
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /**
   * Serializes a single schema to the {@code latest_schema}-style JSON blob.
   * Returns an empty string for null or empty-sentinel schemas — matching the
   * historical semantics for the {@code latest_schema} commit metadata key.
   */
  public static String toJson(HoodieSchema schema) {
    if (schema == null || schema.isEmptySchema()) {
      return "";
    }
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator gen = new JsonFactory().createGenerator(writer);
      writeRecordTopLevel(schema, gen);
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Serializes a list of versioned schemas to the {@code .hoodie/.schema/}
   * history blob format: a single object whose {@code schemas} array contains
   * each schema as a top-level record (carrying its own {@code version_id}
   * and {@code max_column_id}).
   */
  public static String toJsonHistory(List<HoodieSchema> schemas) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator gen = new JsonFactory().createGenerator(writer);
      gen.writeStartObject();
      gen.writeArrayFieldStart(SCHEMAS);
      for (HoodieSchema s : schemas) {
        writeRecordTopLevel(s, gen);
      }
      gen.writeEndArray();
      gen.writeEndObject();
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Parses a single-schema JSON blob (typically the {@code latest_schema}
   * value from commit metadata). Returns empty for null or empty input —
   * the history-blob entry point is {@link #parseHistorySchemas(String)}.
   */
  public static Option<HoodieSchema> fromJson(String json) {
    return fromJson(json, "hoodieSchema");
  }

  /**
   * Variant of {@link #fromJson(String)} that fixes the record name on the
   * resulting top-level HoodieSchema. Useful for callers that need the
   * record name to match an existing schema namespace.
   */
  public static Option<HoodieSchema> fromJson(String json, String recordName) {
    if (json == null || json.isEmpty()) {
      return Option.empty();
    }
    try {
      JsonNode node = JsonUtils.getObjectMapper().readTree(json);
      return Option.of(parseRecordTopLevel(node, recordName));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Parses a history blob and returns its schemas keyed by {@code version_id}.
   * The TreeMap ordering lets callers iterate from oldest to newest.
   */
  public static TreeMap<Long, HoodieSchema> parseHistorySchemas(String json) {
    TreeMap<Long, HoodieSchema> result = new TreeMap<>();
    try {
      JsonNode node = JsonUtils.getObjectMapper().readTree(json);
      if (!node.has(SCHEMAS)) {
        throw new IllegalArgumentException(
            String.format("cannot parser schemas from current json string, missing key name: %s", SCHEMAS));
      }
      Iterator<JsonNode> iter = node.get(SCHEMAS).elements();
      while (iter.hasNext()) {
        HoodieSchema s = parseRecordTopLevel(iter.next(), "hoodieSchema");
        result.put(s.schemaId(), s);
      }
    } catch (IOException e) {
      throw new HoodieException(e);
    }
    return result;
  }

  /**
   * Appends a freshly-evolved schema to an existing serialized history blob.
   * Performs string surgery on the {@code "{\"schemas\":["} prefix to avoid
   * re-serializing the entire history. Returns an empty string when
   * {@code newSchema} is null, and the new schema as a single-element history
   * when {@code oldSchemas} is null/empty. Returns an empty string when
   * {@code oldSchemas} doesn't carry the expected prefix — matching legacy
   * permissive behavior.
   */
  public static String inheritHistory(HoodieSchema newSchema, String oldSchemas) {
    if (newSchema == null) {
      return "";
    }
    if (oldSchemas == null || oldSchemas.isEmpty()) {
      return toJsonHistory(Collections.singletonList(newSchema));
    }
    String checkedString = "{\"schemas\":[";
    if (!oldSchemas.startsWith(checkedString)) {
      return "";
    }
    String oldSchemasSuffix = oldSchemas.substring(checkedString.length());
    return checkedString + toJson(newSchema) + "," + oldSchemasSuffix;
  }

  /**
   * Resolves the schema-history entry that applies to a given version id —
   * exact match if present, otherwise the largest entry strictly less than
   * {@code versionId}, otherwise {@code null}. Returns {@code null} (not an
   * empty-schema sentinel) on miss so callers can choose their own empty-schema
   * construction.
   */
  public static HoodieSchema searchSchema(long versionId, TreeMap<Long, HoodieSchema> history) {
    if (history.containsKey(versionId)) {
      return history.get(versionId);
    }
    SortedMap<Long, HoodieSchema> headMap = history.headMap(versionId);
    return headMap.isEmpty() ? null : headMap.get(headMap.lastKey());
  }

  // -------------------------------------------------------------------------
  // Serialization
  // -------------------------------------------------------------------------

  private static void writeRecordTopLevel(HoodieSchema schema, JsonGenerator gen) throws IOException {
    HoodieSchema record = schema.isNullable() ? schema.getNonNullType() : schema;
    gen.writeStartObject();
    int maxColumnId = schema.maxColumnId();
    if (maxColumnId >= 0) {
      gen.writeNumberField(MAX_COLUMN_ID, maxColumnId);
    }
    long versionId = schema.schemaId();
    if (versionId >= 0) {
      gen.writeNumberField(VERSION_ID, versionId);
    }
    gen.writeStringField(TYPE, RECORD);
    gen.writeArrayFieldStart(FIELDS);
    for (HoodieSchemaField field : record.getFields()) {
      writeField(field, gen);
    }
    gen.writeEndArray();
    gen.writeEndObject();
  }

  private static void writeField(HoodieSchemaField field, JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeNumberField(ID, field.fieldId());
    gen.writeStringField(NAME, field.name());
    boolean optional = field.schema().isNullable();
    gen.writeBooleanField(OPTIONAL, optional);
    gen.writeFieldName(TYPE);
    HoodieSchema fieldType = optional ? field.schema().getNonNullType() : field.schema();
    writeType(fieldType, gen);
    String doc = field.doc().orElse(null);
    if (doc != null) {
      gen.writeStringField(DOC, doc);
    }
    gen.writeEndObject();
  }

  private static void writeType(HoodieSchema schema, JsonGenerator gen) throws IOException {
    HoodieSchemaType t = schema.getType();
    switch (t) {
      case RECORD:
        gen.writeStartObject();
        gen.writeStringField(TYPE, RECORD);
        gen.writeArrayFieldStart(FIELDS);
        for (HoodieSchemaField f : schema.getFields()) {
          writeField(f, gen);
        }
        gen.writeEndArray();
        gen.writeEndObject();
        return;
      case ARRAY: {
        gen.writeStartObject();
        gen.writeStringField(TYPE, ARRAY);
        int elementId = readIntProp(schema.getAvroSchema().getObjectProp(HoodieSchema.ELEMENT_ID_PROP));
        gen.writeNumberField(ELEMENT_ID, elementId);
        gen.writeFieldName(ELEMENT);
        HoodieSchema elementType = schema.getElementType();
        boolean elemOptional = elementType.isNullable();
        writeType(elemOptional ? elementType.getNonNullType() : elementType, gen);
        gen.writeBooleanField(ELEMENT_OPTIONAL, elemOptional);
        gen.writeEndObject();
        return;
      }
      case MAP: {
        gen.writeStartObject();
        gen.writeStringField(TYPE, MAP);
        int keyId = readIntProp(schema.getAvroSchema().getObjectProp(HoodieSchema.KEY_ID_PROP));
        gen.writeNumberField(KEY_ID, keyId);
        gen.writeFieldName(KEY);
        // Map keys in Avro/Hudi are always strings — emit "string" rather than
        // attempting to walk a synthetic key schema. Matches legacy behavior.
        gen.writeString("string");
        int valueId = readIntProp(schema.getAvroSchema().getObjectProp(HoodieSchema.VALUE_ID_PROP));
        gen.writeNumberField(VALUE_ID, valueId);
        gen.writeFieldName(VALUE);
        HoodieSchema valueType = schema.getValueType();
        boolean valOptional = valueType.isNullable();
        writeType(valOptional ? valueType.getNonNullType() : valueType, gen);
        gen.writeBooleanField(VALUE_OPTIONAL, valOptional);
        gen.writeEndObject();
        return;
      }
      default:
        gen.writeString(primitiveTypeString(schema));
    }
  }

  private static int readIntProp(Object raw) {
    return raw instanceof Number ? ((Number) raw).intValue() : -1;
  }

  /**
   * Maps a primitive HoodieSchema to the legacy on-disk type-string token.
   * Logical types take precedence (so date/uuid/time/timestamp variants emit
   * with the Avro logical name) and decimal is split between the bytes-backed
   * and fixed-backed forms based on the underlying Avro storage type. The
   * {@code -micros} suffix is stripped for the bare {@code timestamp} and
   * {@code time} tokens because the legacy
   * {@code Types.TimestampType.toString()} / {@code Types.TimeType.toString()}
   * both collapse to those, and any drift here would be visible on disk.
   */
  private static String primitiveTypeString(HoodieSchema schema) {
    Schema avro = schema.toAvroSchema();
    LogicalType logical = avro.getLogicalType();
    if (logical != null) {
      if (logical instanceof LogicalTypes.Decimal) {
        LogicalTypes.Decimal dec = (LogicalTypes.Decimal) logical;
        if (avro.getType() == Schema.Type.FIXED) {
          return String.format("decimal_fixed(%d,%d)[%d]",
              dec.getPrecision(), dec.getScale(), avro.getFixedSize());
        }
        return String.format("decimal_bytes(%d,%d)", dec.getPrecision(), dec.getScale());
      }
      String name = logical.getName();
      if ("timestamp-micros".equals(name)) {
        return "timestamp";
      }
      if ("time-micros".equals(name)) {
        return "time";
      }
      return name;
    }
    switch (avro.getType()) {
      case BOOLEAN:
        return "boolean";
      case INT:
        return "int";
      case LONG:
        return "long";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      case STRING:
        return "string";
      case BYTES:
        return "binary";
      case FIXED:
        return String.format("fixed[%d]", avro.getFixedSize());
      default:
        throw new HoodieIOException("cannot serialize primitive schema: " + avro);
    }
  }

  // -------------------------------------------------------------------------
  // Parsing
  // -------------------------------------------------------------------------

  private static HoodieSchema parseRecordTopLevel(JsonNode node, String recordName) {
    long versionId = node.has(VERSION_ID) ? node.get(VERSION_ID).asLong() : -1;
    int maxColumnId = node.has(MAX_COLUMN_ID) ? node.get(MAX_COLUMN_ID).asInt() : -1;
    HoodieSchema record = parseRecord(node, recordName);
    if (versionId >= 0) {
      record.setSchemaId(versionId);
    }
    if (maxColumnId >= 0) {
      record.setMaxColumnId(maxColumnId);
    }
    record.invalidateIdIndex();
    return record;
  }

  private static HoodieSchema parseRecord(JsonNode node, String recordName) {
    JsonNode fieldsNode = node.get(FIELDS);
    List<HoodieSchemaField> fields = new ArrayList<>();
    Iterator<JsonNode> iter = fieldsNode.elements();
    while (iter.hasNext()) {
      JsonNode fieldNode = iter.next();
      int fieldId = fieldNode.get(ID).asInt();
      String name = fieldNode.get(NAME).asText();
      boolean optional = fieldNode.get(OPTIONAL).asBoolean();
      String doc = fieldNode.has(DOC) ? fieldNode.get(DOC).asText() : null;
      HoodieSchema typeSchema = parseType(fieldNode.get(TYPE), name);
      HoodieSchema fieldSchema = optional ? HoodieSchema.createNullable(typeSchema) : typeSchema;
      HoodieSchemaField field = HoodieSchemaField.of(name, fieldSchema, doc, null);
      // The id property must live on the Avro Field — that's what
      // HoodieSchemaField#fieldId() reads on the way out.
      field.addProp(HoodieSchema.FIELD_ID_PROP, fieldId);
      fields.add(field);
    }
    return HoodieSchema.createRecord(recordName, null, null, false, fields);
  }

  private static HoodieSchema parseType(JsonNode node, String contextName) {
    if (node.isTextual()) {
      return parseTypeString(node.asText().toLowerCase(Locale.ROOT));
    }
    if (node.isObject()) {
      String typeStr = node.get(TYPE).asText();
      if (RECORD.equals(typeStr)) {
        return parseRecord(node, contextName);
      }
      if (ARRAY.equals(typeStr)) {
        int elementId = node.get(ELEMENT_ID).asInt();
        boolean elementOptional = node.get(ELEMENT_OPTIONAL).asBoolean();
        HoodieSchema elementType = parseType(node.get(ELEMENT), contextName + "_element");
        HoodieSchema effectiveElement = elementOptional ? HoodieSchema.createNullable(elementType) : elementType;
        HoodieSchema arr = HoodieSchema.createArray(effectiveElement);
        arr.getAvroSchema().addProp(HoodieSchema.ELEMENT_ID_PROP, elementId);
        return arr;
      }
      if (MAP.equals(typeStr)) {
        int keyId = node.get(KEY_ID).asInt();
        int valueId = node.get(VALUE_ID).asInt();
        boolean valueOptional = node.get(VALUE_OPTIONAL).asBoolean();
        // Map keys are always string in Avro/Hudi; we ignore whatever's in the
        // KEY field of the JSON and synthesize a string key. Matches legacy.
        HoodieSchema valueType = parseType(node.get(VALUE), contextName + "_value");
        HoodieSchema effectiveValue = valueOptional ? HoodieSchema.createNullable(valueType) : valueType;
        HoodieSchema map = HoodieSchema.createMap(effectiveValue);
        map.getAvroSchema().addProp(HoodieSchema.KEY_ID_PROP, keyId);
        map.getAvroSchema().addProp(HoodieSchema.VALUE_ID_PROP, valueId);
        return map;
      }
    }
    throw new IllegalArgumentException("cannot parse type from jsonNode: " + node);
  }

  private static HoodieSchema parseTypeString(String typeStr) {
    Matcher decimalFixed = DECIMAL_FIXED_PATTERN.matcher(typeStr);
    if (decimalFixed.matches()) {
      return HoodieSchema.createDecimal("decimal", null, null,
          Integer.parseInt(decimalFixed.group(1)),
          Integer.parseInt(decimalFixed.group(2)),
          Integer.parseInt(decimalFixed.group(3)));
    }
    Matcher fixed = FIXED_PATTERN.matcher(typeStr);
    if (fixed.matches()) {
      return HoodieSchema.createFixed("fixed", null, null, Integer.parseInt(fixed.group(1)));
    }
    Matcher decimalBytes = DECIMAL_BYTES_PATTERN.matcher(typeStr);
    if (decimalBytes.matches()) {
      return HoodieSchema.createDecimal(
          Integer.parseInt(decimalBytes.group(1)),
          Integer.parseInt(decimalBytes.group(2)));
    }
    Matcher decimal = DECIMAL_PATTERN.matcher(typeStr);
    if (decimal.matches()) {
      // Plain "decimal(p, s)" — legacy parsed this as Types.DecimalType, which
      // is bytes-backed in Avro. Map to the bytes-backed HoodieSchema decimal.
      return HoodieSchema.createDecimal(
          Integer.parseInt(decimal.group(1)),
          Integer.parseInt(decimal.group(2)));
    }
    switch (typeStr) {
      case "boolean":
        return HoodieSchema.create(HoodieSchemaType.BOOLEAN);
      case "int":
        return HoodieSchema.create(HoodieSchemaType.INT);
      case "long":
        return HoodieSchema.create(HoodieSchemaType.LONG);
      case "float":
        return HoodieSchema.create(HoodieSchemaType.FLOAT);
      case "double":
        return HoodieSchema.create(HoodieSchemaType.DOUBLE);
      case "string":
        return HoodieSchema.create(HoodieSchemaType.STRING);
      case "binary":
        return HoodieSchema.create(HoodieSchemaType.BYTES);
      case "date":
        return HoodieSchema.createDate();
      case "uuid":
        return HoodieSchema.createUUID();
      case "time":
        return HoodieSchema.createTimeMicros();
      case "time-millis":
      case "time_millis":
        return HoodieSchema.createTimeMillis();
      case "timestamp":
        return HoodieSchema.createTimestampMicros();
      case "timestamp-millis":
      case "timestamp_millis":
        return HoodieSchema.createTimestampMillis();
      case "local-timestamp-millis":
      case "local_timestamp_millis":
        return HoodieSchema.createLocalTimestampMillis();
      case "local-timestamp-micros":
      case "local_timestamp_micros":
        return HoodieSchema.createLocalTimestampMicros();
      default:
        throw new IllegalArgumentException("unknown primitive type: " + typeStr);
    }
  }
}
