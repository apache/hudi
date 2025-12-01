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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.SchemaParseException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * Avro schemas have restrictions (https://avro.apache.org/docs/current/spec.html#names) on field names
 * that other schema formats do not have. This class provides utilities to help with sanitizing
 */
public class SanitizationUtils {

  private static final ObjectMapper OM = new ObjectMapper();

  @Deprecated
  public static class Config {
    @Deprecated
    public static final ConfigProperty<Boolean> SANITIZE_SCHEMA_FIELD_NAMES =
        HoodieStreamerConfig.SANITIZE_SCHEMA_FIELD_NAMES;

    @Deprecated
    public static final ConfigProperty<String> SCHEMA_FIELD_NAME_INVALID_CHAR_MASK =
        HoodieStreamerConfig.SCHEMA_FIELD_NAME_INVALID_CHAR_MASK;
  }

  private static final String AVRO_FIELD_NAME_KEY = "name";

  public static boolean shouldSanitize(TypedProperties props) {
    return getBooleanWithAltKeys(props, HoodieStreamerConfig.SANITIZE_SCHEMA_FIELD_NAMES);
  }

  public static String getInvalidCharMask(TypedProperties props) {
    return getStringWithAltKeys(props, HoodieStreamerConfig.SCHEMA_FIELD_NAME_INVALID_CHAR_MASK, true);
  }

  private static DataType sanitizeDataTypeForAvro(DataType dataType, String invalidCharMask) {
    if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;
      DataType sanitizedDataType = sanitizeDataTypeForAvro(arrayType.elementType(), invalidCharMask);
      return new ArrayType(sanitizedDataType, arrayType.containsNull());
    } else if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      DataType sanitizedKeyDataType = sanitizeDataTypeForAvro(mapType.keyType(), invalidCharMask);
      DataType sanitizedValueDataType = sanitizeDataTypeForAvro(mapType.valueType(), invalidCharMask);
      return new MapType(sanitizedKeyDataType, sanitizedValueDataType, mapType.valueContainsNull());
    } else if (dataType instanceof StructType) {
      return sanitizeStructTypeForAvro((StructType) dataType, invalidCharMask);
    }
    return dataType;
  }

  // TODO(HUDI-5256): Refactor this to use InternalSchema when it is ready.
  private static StructType sanitizeStructTypeForAvro(StructType structType, String invalidCharMask) {
    StructType sanitizedStructType = new StructType();
    StructField[] structFields = structType.fields();
    for (StructField s : structFields) {
      DataType currFieldDataTypeSanitized = sanitizeDataTypeForAvro(s.dataType(), invalidCharMask);
      StructField structFieldCopy = new StructField(HoodieAvroUtils.sanitizeName(s.name(), invalidCharMask),
          currFieldDataTypeSanitized, s.nullable(), s.metadata());
      sanitizedStructType = sanitizedStructType.add(structFieldCopy);
    }
    return sanitizedStructType;
  }

  public static Dataset<Row> sanitizeColumnNamesForAvro(Dataset<Row> inputDataset, String invalidCharMask) {
    StructField[] inputFields = inputDataset.schema().fields();
    Dataset<Row> targetDataset = inputDataset;
    for (StructField sf : inputFields) {
      DataType sanitizedFieldDataType = sanitizeDataTypeForAvro(sf.dataType(), invalidCharMask);
      if (!sanitizedFieldDataType.equals(sf.dataType())) {
        // Sanitizing column names for nested types can be thought of as going from one schema to another
        // which are structurally similar except for actual column names itself. So casting is safe and sufficient.
        targetDataset = targetDataset.withColumn(sf.name(), targetDataset.col(sf.name()).cast(sanitizedFieldDataType));
      }
      String possibleRename = HoodieAvroUtils.sanitizeName(sf.name(), invalidCharMask);
      if (!sf.name().equals(possibleRename)) {
        targetDataset = targetDataset.withColumnRenamed(sf.name(), possibleRename);
      }
    }
    return targetDataset;
  }

  public static Dataset<Row> sanitizeColumnNamesForAvro(Dataset<Row> inputDataset, TypedProperties props) {
    return shouldSanitize(props) ? sanitizeColumnNamesForAvro(inputDataset, getInvalidCharMask(props))
        : inputDataset;
  }

  /*
   * We first rely on Avro to parse and then try to rename only for those failed.
   * This way we can improve our parsing capabilities without breaking existing functionality.
   * For example, we don't yet support multiple named schemas defined in a file.
   */
  public static HoodieSchema parseAvroSchema(String schemaStr, boolean shouldSanitize, String invalidCharMask) {
    try {
      return HoodieSchema.parse(schemaStr);
    } catch (HoodieAvroSchemaException | SchemaParseException spe) {
      // if sanitizing is not enabled rethrow the exception.
      if (!shouldSanitize) {
        throw spe;
      }
      // Rename avro fields and try parsing once again.
      Option<HoodieSchema> parseResult = parseSanitizedAvroSchemaNoThrow(schemaStr, invalidCharMask);
      if (!parseResult.isPresent()) {
        // throw original exception.
        throw spe;
      }
      return parseResult.get();
    }
  }

  /**
   * Parse list for sanitizing
   * @param src - deserialized schema
   * @param invalidCharMask - mask to replace invalid characters with
   */
  private static List<Object> transformList(List<Object> src, String invalidCharMask) {
    return src.stream().map(obj -> {
      if (obj instanceof List) {
        return transformList((List<Object>) obj, invalidCharMask);
      } else if (obj instanceof Map) {
        return transformMap((Map<String, Object>) obj, invalidCharMask);
      } else {
        return obj;
      }
    }).collect(Collectors.toList());
  }

  /**
   * Parse map for sanitizing. If we have a string in the map, and it is an avro field name key, then we sanitize the name.
   * Otherwise, we keep recursively going through the schema.
   * @param src - deserialized schema
   * @param invalidCharMask - mask to replace invalid characters with
   */
  private static Map<String, Object> transformMap(Map<String, Object> src, String invalidCharMask) {
    return src.entrySet().stream()
        .map(kv -> {
          if (kv.getValue() instanceof List) {
            return Pair.of(kv.getKey(), transformList((List<Object>) kv.getValue(), invalidCharMask));
          } else if (kv.getValue() instanceof Map) {
            return Pair.of(kv.getKey(), transformMap((Map<String, Object>) kv.getValue(), invalidCharMask));
          } else if (kv.getValue() instanceof String) {
            String currentStrValue = (String) kv.getValue();
            if (kv.getKey().equals(AVRO_FIELD_NAME_KEY)) {
              return Pair.of(kv.getKey(), HoodieAvroUtils.sanitizeName(currentStrValue, invalidCharMask));
            }
            return Pair.of(kv.getKey(), currentStrValue);
          } else {
            return Pair.of(kv.getKey(), kv.getValue());
          }
        }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
  }

  /**
   * Sanitizes illegal field names in the schema using recursive calls to transformMap and transformList
   */
  private static Option<HoodieSchema> parseSanitizedAvroSchemaNoThrow(String schemaStr, String invalidCharMask) {
    try {
      OM.enable(JsonParser.Feature.ALLOW_COMMENTS);
      Map<String, Object> objMap = OM.readValue(schemaStr, Map.class);
      Map<String, Object> modifiedMap = transformMap(objMap, invalidCharMask);
      return Option.of(HoodieSchema.parse(OM.writeValueAsString(modifiedMap)));
    } catch (Exception ex) {
      return Option.empty();
    }
  }
}
