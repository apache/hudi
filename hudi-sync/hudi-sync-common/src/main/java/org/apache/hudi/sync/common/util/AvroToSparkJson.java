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

package org.apache.hudi.sync.common.util;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class to convert Avro schemas directly to Spark SQL schema JSON format.
 * This bypasses Parquet conversion and eliminates struct-wrapping issues with complex types.
 *
 * Based on Spark's SchemaConverters logic but implemented without Spark dependencies.
 */
public class AvroToSparkJson {

  private AvroToSparkJson() {
    // Utility class
  }

  /**
   * Convert an Avro schema to Spark SQL schema JSON format.
   *
   * @param avroSchema The Avro schema to convert
   * @return JSON string representing the Spark schema
   */
  public static String convertToSparkSchemaJson(Schema avroSchema) {
    if (avroSchema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException("Top-level schema must be a RECORD type, got: " + avroSchema.getType());
    }

    SparkDataType sparkType = convertAvroType(avroSchema);
    return sparkType.toJson();
  }

  private static SparkDataType convertAvroType(Schema avroSchema) {
    switch (avroSchema.getType()) {
      case NULL:
        return new PrimitiveSparkType("null");
      case BOOLEAN:
        return new PrimitiveSparkType("boolean");
      case INT:
        if (avroSchema.getLogicalType() instanceof LogicalTypes.Date) {
          return new PrimitiveSparkType("date");
        }
        return new PrimitiveSparkType("integer");
      case LONG:
        if (avroSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis
            || avroSchema.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
          return new PrimitiveSparkType("timestamp");
        }
        return new PrimitiveSparkType("long");
      case FLOAT:
        return new PrimitiveSparkType("float");
      case DOUBLE:
        return new PrimitiveSparkType("double");
      case BYTES:
      case FIXED:
        if (avroSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
          LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) avroSchema.getLogicalType();
          return new DecimalSparkType(decimal.getPrecision(), decimal.getScale());
        }
        return new PrimitiveSparkType("binary");
      case STRING:
      case ENUM:
        return new PrimitiveSparkType("string");
      case ARRAY:
        SparkDataType elementType = convertAvroType(avroSchema.getElementType());
        boolean containsNull = isNullable(avroSchema.getElementType());
        return new ArraySparkType(elementType, containsNull);
      case MAP:
        SparkDataType valueType = convertAvroType(avroSchema.getValueType());
        boolean valueContainsNull = isNullable(avroSchema.getValueType());
        return new MapSparkType(new PrimitiveSparkType("string"), valueType, valueContainsNull);
      case RECORD:
        List<StructFieldType> fields = avroSchema.getFields().stream()
            .map(field -> {
              SparkDataType fieldType = convertAvroType(field.schema());
              boolean nullable = isNullable(field.schema());
              String comment = field.doc();
              return new StructFieldType(field.name(), fieldType, nullable, comment);
            })
            .collect(Collectors.toList());
        return new StructSparkType(fields);
      case UNION:
        return handleUnion(avroSchema);
      default:
        throw new IllegalArgumentException("Unsupported Avro type: " + avroSchema.getType());
    }
  }

  private static SparkDataType handleUnion(Schema unionSchema) {
    List<Schema> types = unionSchema.getTypes();

    // Handle nullable unions (common pattern: [null, actual_type])
    if (types.size() == 2 && types.stream().anyMatch(s -> s.getType() == Schema.Type.NULL)) {
      Schema nonNullType = types.stream()
          .filter(s -> s.getType() != Schema.Type.NULL)
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Invalid union with only null type"));
      return convertAvroType(nonNullType);
    }

    // For complex unions, we could implement more sophisticated logic
    // For now, treat as string (similar to how some systems handle this)
    return new PrimitiveSparkType("string");
  }

  private static boolean isNullable(Schema schema) {
    if (schema.getType() == Schema.Type.NULL) {
      return true;
    }
    if (schema.getType() == Schema.Type.UNION) {
      return schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
    }
    return false;
  }

  /**
   * Abstract base class for Spark data types
   */
  private abstract static class SparkDataType {
    public abstract String toJson();
  }

  /**
   * Primitive Spark types (string, int, boolean, etc.)
   */
  private static class PrimitiveSparkType extends SparkDataType {
    private final String typeName;

    public PrimitiveSparkType(String typeName) {
      this.typeName = typeName;
    }

    @Override
    public String toJson() {
      return "\"" + typeName + "\"";
    }
  }

  /**
   * Decimal Spark type with precision and scale
   */
  private static class DecimalSparkType extends SparkDataType {
    private final int precision;
    private final int scale;

    public DecimalSparkType(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public String toJson() {
      return "\"decimal(" + precision + "," + scale + ")\"";
    }
  }

  /**
   * Array Spark type
   */
  private static class ArraySparkType extends SparkDataType {
    private final SparkDataType elementType;
    private final boolean containsNull;

    public ArraySparkType(SparkDataType elementType, boolean containsNull) {
      this.elementType = elementType;
      this.containsNull = containsNull;
    }

    @Override
    public String toJson() {
      return "{\"type\":\"array\",\"elementType\":" + elementType.toJson()
             + ",\"containsNull\":" + containsNull + "}";
    }
  }

  /**
   * Map Spark type
   */
  private static class MapSparkType extends SparkDataType {
    private final SparkDataType keyType;
    private final SparkDataType valueType;
    private final boolean valueContainsNull;

    public MapSparkType(SparkDataType keyType, SparkDataType valueType, boolean valueContainsNull) {
      this.keyType = keyType;
      this.valueType = valueType;
      this.valueContainsNull = valueContainsNull;
    }

    @Override
    public String toJson() {
      return "{\"type\":\"map\",\"keyType\":" + keyType.toJson()
             + ",\"valueType\":" + valueType.toJson()
             + ",\"valueContainsNull\":" + valueContainsNull + "}";
    }
  }

  /**
   * Struct Spark type
   */
  private static class StructSparkType extends SparkDataType {
    private final List<StructFieldType> fields;

    public StructSparkType(List<StructFieldType> fields) {
      this.fields = fields;
    }

    @Override
    public String toJson() {
      String fieldsJson = fields.stream()
          .map(StructFieldType::toJson)
          .collect(Collectors.joining(","));
      return "{\"type\":\"struct\",\"fields\":[" + fieldsJson + "]}";
    }
  }

  /**
   * Struct field type with metadata support
   */
  private static class StructFieldType {
    private final String name;
    private final SparkDataType dataType;
    private final boolean nullable;
    private final String comment;

    public StructFieldType(String name, SparkDataType dataType, boolean nullable, String comment) {
      this.name = name;
      this.dataType = dataType;
      this.nullable = nullable;
      this.comment = comment;
    }

    public String toJson() {
      StringBuilder metadata = new StringBuilder("{");
      if (comment != null && !comment.trim().isEmpty()) {
        // Escape quotes in comments
        String escapedComment = comment.replace("\"", "\\\"");
        metadata.append("\"comment\":\"").append(escapedComment).append("\"");
      }
      metadata.append("}");

      return "{\"name\":\"" + name + "\""
             + ",\"type\":" + dataType.toJson()
             + ",\"nullable\":" + nullable
             + ",\"metadata\":" + metadata.toString() + "}";
    }
  }
}