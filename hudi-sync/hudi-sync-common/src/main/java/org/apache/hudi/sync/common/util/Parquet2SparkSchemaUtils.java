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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

/**
 * Convert the parquet schema to spark schema' json string.
 * This code is refer to org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter
 * in spark project.
 */
public class Parquet2SparkSchemaUtils {

  /**
   * Get Spark Sql related table properties. This is used for spark datasource table.
   * @param schema  The schema to write to the table.
   * @return A new parameters added the spark's table properties.
   */
  public static Map<String, String> getSparkTableProperties(List<String> partitionNames, String sparkVersion,
                                                        int schemaLengthThreshold, MessageType schema)  {
    // Convert the schema and partition info used by spark sql to hive table properties.
    // The following code refers to the spark code in
    // https://github.com/apache/spark/blob/master/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveExternalCatalog.scala
    GroupType originGroupType = schema.asGroupType();
    List<Type> partitionCols = new ArrayList<>();
    List<Type> dataCols = new ArrayList<>();
    Map<String, Type> column2Field = new HashMap<>();

    for (Type field : originGroupType.getFields()) {
      column2Field.put(field.getName(), field);
    }
    // Get partition columns and data columns.
    for (String partitionName : partitionNames) {
      // Default the unknown partition fields to be String.
      // Keep the same logical with HiveSchemaUtil#getPartitionKeyType.
      partitionCols.add(column2Field.getOrDefault(partitionName,
          new PrimitiveType(Type.Repetition.REQUIRED, BINARY, partitionName, UTF8)));
    }

    for (Type field : originGroupType.getFields()) {
      if (!partitionNames.contains(field.getName())) {
        dataCols.add(field);
      }
    }

    List<Type> reOrderedFields = new ArrayList<>();
    reOrderedFields.addAll(dataCols);
    reOrderedFields.addAll(partitionCols);
    GroupType reOrderedType = new GroupType(originGroupType.getRepetition(), originGroupType.getName(), reOrderedFields);

    Map<String, String> sparkProperties = new HashMap<>();
    sparkProperties.put("spark.sql.sources.provider", "hudi");
    if (!StringUtils.isNullOrEmpty(sparkVersion)) {
      sparkProperties.put("spark.sql.create.version", sparkVersion);
    }
    // Split the schema string to multi-parts according the schemaLengthThreshold size.
    String schemaString = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(reOrderedType);
    int numSchemaPart = (schemaString.length() + schemaLengthThreshold - 1) / schemaLengthThreshold;
    sparkProperties.put("spark.sql.sources.schema.numParts", String.valueOf(numSchemaPart));
    // Add each part of schema string to sparkProperties
    for (int i = 0; i < numSchemaPart; i++) {
      int start = i * schemaLengthThreshold;
      int end = Math.min(start + schemaLengthThreshold, schemaString.length());
      sparkProperties.put("spark.sql.sources.schema.part." + i, schemaString.substring(start, end));
    }
    // Add partition columns
    if (!partitionNames.isEmpty()) {
      sparkProperties.put("spark.sql.sources.schema.numPartCols", String.valueOf(partitionNames.size()));
      for (int i = 0; i < partitionNames.size(); i++) {
        sparkProperties.put("spark.sql.sources.schema.partCol." + i, partitionNames.get(i));
      }
    }
    return sparkProperties;
  }

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
