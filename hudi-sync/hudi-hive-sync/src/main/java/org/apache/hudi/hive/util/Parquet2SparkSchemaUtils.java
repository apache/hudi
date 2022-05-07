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

package org.apache.hudi.hive.util;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

/**
 * Convert the parquet schema to spark schema' json string.
 * This code is refer to org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter
 * in spark project.
 */
public class Parquet2SparkSchemaUtils {

  public static String convertToSparkSchemaJson(GroupType parquetSchema) {
    String fieldsJsonString = parquetSchema.getFields().stream().map(field -> {
      switch (field.getRepetition()) {
        case OPTIONAL:
          return "{\"name\":\"" + field.getName() + "\",\"type\":" + convertFieldType(field)
                  + ",\"nullable\":true,\"metadata\":{}}";
        case REQUIRED:
          return "{\"name\":\"" + field.getName() + "\",\"type\":" + convertFieldType(field)
                  + ",\"nullable\":false,\"metadata\":{}}";
        case REPEATED:
          String arrayType = arrayType(field, false);
          return "{\"name\":\"" + field.getName() + "\",\"type\":" + arrayType
                  + ",\"nullable\":false,\"metadata\":{}}";
        default:
          throw new UnsupportedOperationException("Unsupport convert " + field + " to spark sql type");
      }
    }).reduce((a, b) -> a + "," + b).orElse("");
    return "{\"type\":\"struct\",\"fields\":[" + fieldsJsonString + "]}";
  }

  private static String convertFieldType(Type field) {
    if (field instanceof PrimitiveType) {
      return "\"" + convertPrimitiveType((PrimitiveType) field) + "\"";
    } else {
      assert field instanceof GroupType;
      return convertGroupField((GroupType) field);
    }
  }

  private static String convertPrimitiveType(PrimitiveType field) {
    PrimitiveType.PrimitiveTypeName typeName = field.getPrimitiveTypeName();
    OriginalType originalType = field.getOriginalType();

    switch (typeName) {
      case BOOLEAN: return "boolean";
      case FLOAT: return "float";
      case DOUBLE: return "double";
      case INT32:
        if (originalType == null) {
          return "integer";
        }
        switch (originalType) {
          case INT_8: return "byte";
          case INT_16: return "short";
          case INT_32: return "integer";
          case DATE: return "date";
          case DECIMAL:
            return "decimal(" + field.getDecimalMetadata().getPrecision() + ","
                    + field.getDecimalMetadata().getScale() + ")";
          default: throw new UnsupportedOperationException("Unsupport convert " + typeName + " to spark sql type");
        }
      case INT64:
        if (originalType == null) {
          return "long";
        }
        switch (originalType)  {
          case INT_64: return "long";
          case DECIMAL:
            return "decimal(" + field.getDecimalMetadata().getPrecision() + ","
                    + field.getDecimalMetadata().getScale() + ")";
          case TIMESTAMP_MICROS:
          case TIMESTAMP_MILLIS:
            return "timestamp";
          default:
            throw new UnsupportedOperationException("Unsupport convert " + typeName + " to spark sql type");
        }
      case INT96: return "timestamp";

      case BINARY:
        if (originalType == null) {
          return "binary";
        }
        switch (originalType) {
          case UTF8:
          case  ENUM:
          case  JSON:
            return "string";
          case BSON: return "binary";
          case DECIMAL:
            return "decimal(" + field.getDecimalMetadata().getPrecision() + ","
                    + field.getDecimalMetadata().getScale() + ")";
          default:
            throw new UnsupportedOperationException("Unsupport convert " + typeName + " to spark sql type");
        }

      case FIXED_LEN_BYTE_ARRAY:
        switch (originalType) {
          case DECIMAL:
            return "decimal(" + field.getDecimalMetadata().getPrecision() + ","
                  + field.getDecimalMetadata().getScale() + ")";
          default:
            throw new UnsupportedOperationException("Unsupport convert " + typeName + " to spark sql type");
        }
      default:
        throw new UnsupportedOperationException("Unsupport convert " + typeName + " to spark sql type");
    }
  }

  private static String convertGroupField(GroupType field) {
    if (field.getOriginalType() == null) {
      return convertToSparkSchemaJson(field);
    }
    switch (field.getOriginalType()) {
      case LIST:
        ValidationUtils.checkArgument(field.getFieldCount() == 1, "Illegal List type: " + field);
        Type repeatedType = field.getType(0);
        if (isElementType(repeatedType, field.getName())) {
          return arrayType(repeatedType, false);
        } else {
          Type elementType = repeatedType.asGroupType().getType(0);
          boolean optional = elementType.isRepetition(OPTIONAL);
          return arrayType(elementType, optional);
        }
      case MAP:
      case MAP_KEY_VALUE:
        GroupType keyValueType = field.getType(0).asGroupType();
        Type keyType = keyValueType.getType(0);
        Type valueType = keyValueType.getType(1);
        boolean valueOptional = valueType.isRepetition(OPTIONAL);
        return "{\"type\":\"map\", \"keyType\":" + convertFieldType(keyType)
                + ",\"valueType\":" + convertFieldType(valueType)
                + ",\"valueContainsNull\":" + valueOptional + "}";
      default:
        throw new UnsupportedOperationException("Unsupport convert " + field + " to spark sql type");
    }
  }

  private static String arrayType(Type elementType, boolean containsNull) {
    return "{\"type\":\"array\", \"elementType\":" + convertFieldType(elementType) + ",\"containsNull\":" + containsNull + "}";
  }

  private static boolean isElementType(Type repeatedType, String parentName) {
    return repeatedType.isPrimitive() || repeatedType.asGroupType().getFieldCount() > 1
      || repeatedType.getName().equals("array") || repeatedType.getName().equals(parentName + "_tuple");
  }
}
