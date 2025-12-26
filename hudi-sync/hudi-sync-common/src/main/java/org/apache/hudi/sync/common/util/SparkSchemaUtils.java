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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;

/**
 * Convert the Hoodie schema to spark schema' json string.
 * This code is refer to org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter
 * in spark project.
 */
public class SparkSchemaUtils {
  private static final String METADATA_DOC_PREFIX = "Hudi metadata field: ";

  public static String convertToSparkSchemaJson(HoodieSchema schema) {
    String fieldsJsonString = schema.getFields().stream().map(field ->
            "{\"name\":\"" + field.name() + "\",\"type\":" + convertFieldType(field.getNonNullSchema())
                + ",\"nullable\":" + field.isNullable() + ",\"metadata\":" + toMetadataJson(field) + "}")
        .reduce((a, b) -> a + "," + b).orElse("");
    return "{\"type\":\"struct\",\"fields\":[" + fieldsJsonString + "]}";
  }

  private static String toMetadataJson(HoodieSchemaField field) {
    if (field.doc().isPresent() && !field.doc().get().isEmpty()) {
      String doc = field.doc().get();
      if (!doc.startsWith(METADATA_DOC_PREFIX)) {
        return "{\"comment\":\"" + escapeJson(doc) + "\"}";
      }
    }
    return "{}";
  }

  private static String escapeJson(String value) {
    StringBuilder escaped = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '\\':
          escaped.append("\\\\");
          break;
        case '"':
          escaped.append("\\\"");
          break;
        case '\b':
          escaped.append("\\b");
          break;
        case '\f':
          escaped.append("\\f");
          break;
        case '\n':
          escaped.append("\\n");
          break;
        case '\r':
          escaped.append("\\r");
          break;
        case '\t':
          escaped.append("\\t");
          break;
        default:
          escaped.append(c);
      }
    }
    return escaped.toString();
  }

  private static String convertFieldType(HoodieSchema originalFieldSchema) {
    HoodieSchema fieldSchema = originalFieldSchema.getNonNullType();
    switch (fieldSchema.getType()) {
      case BOOLEAN: return "\"boolean\"";
      case FLOAT: return "\"float\"";
      case DOUBLE: return "\"double\"";
      case INT:
        return "\"integer\"";
      case LONG:
        return "\"long\"";
      case STRING:
      case ENUM:
      case UUID:
        return "\"string\"";
      case BYTES:
      case FIXED:
        return "\"binary\"";
      case DATE:
        return "\"date\"";
      case TIMESTAMP:
        HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) fieldSchema;
        if (timestampSchema.isUtcAdjusted()) {
          return "\"timestamp\"";
        } else {
          return "\"timestamp_ntz\"";
        }
      case TIME:
        HoodieSchema.Time timeSchema = (HoodieSchema.Time) fieldSchema;
        if (timeSchema.getPrecision() == HoodieSchema.TimePrecision.MILLIS) {
          return "\"integer\"";
        } else {
          return "\"long\"";
        }
      case DECIMAL:
        HoodieSchema.Decimal decimal = (HoodieSchema.Decimal) fieldSchema;
        return "\"decimal(" + decimal.getPrecision() + "," + decimal.getScale() + ")\"";
      case ARRAY:
        return arrayType(fieldSchema.getElementType());
      case MAP:
        HoodieSchema keyType = fieldSchema.getKeyType();
        HoodieSchema valueType = fieldSchema.getValueType();
        boolean valueOptional = valueType.isNullable();
        return "{\"type\":\"map\", \"keyType\":" + convertFieldType(keyType)
            + ",\"valueType\":" + convertFieldType(valueType)
            + ",\"valueContainsNull\":" + valueOptional + "}";
      case RECORD:
        return convertToSparkSchemaJson(fieldSchema);
      default:
        throw new UnsupportedOperationException("Cannot convert " + fieldSchema.getType() + " to spark sql type");
    }
  }

  private static String arrayType(HoodieSchema elementType) {
    return "{\"type\":\"array\", \"elementType\":" + convertFieldType(elementType) + ",\"containsNull\":" + elementType.isNullable() + "}";
  }
}
