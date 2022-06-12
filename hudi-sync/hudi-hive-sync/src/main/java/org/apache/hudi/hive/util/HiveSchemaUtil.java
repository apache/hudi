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

import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.SchemaDifference;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Schema Utilities.
 */
public class HiveSchemaUtil {

  private static final Logger LOG = LogManager.getLogger(HiveSchemaUtil.class);
  public static final String HIVE_ESCAPE_CHARACTER = "`";

  /**
   * Get the schema difference between the storage schema and hive table schema.
   */
  public static SchemaDifference getSchemaDifference(MessageType storageSchema, Map<String, String> tableSchema,
                                                     List<String> partitionKeys) {
    return getSchemaDifference(storageSchema, tableSchema, partitionKeys, false);
  }

  public static SchemaDifference getSchemaDifference(MessageType storageSchema, Map<String, String> tableSchema,
      List<String> partitionKeys, boolean supportTimestamp) {
    Map<String, String> newTableSchema;
    try {
      newTableSchema = convertParquetSchemaToHiveSchema(storageSchema, supportTimestamp);
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to convert parquet schema to hive schema", e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting schema difference for " + tableSchema + "\r\n\r\n" + newTableSchema);
    }

    SchemaDifference.Builder schemaDiffBuilder = SchemaDifference.newBuilder(storageSchema, tableSchema);
    Set<String> tableColumns = new HashSet<>();

    for (Map.Entry<String, String> field : tableSchema.entrySet()) {
      String fieldName = field.getKey().toLowerCase();
      String tickSurroundedFieldName = tickSurround(fieldName);
      if (!isFieldExistsInSchema(newTableSchema, tickSurroundedFieldName) && !partitionKeys.contains(fieldName)) {
        schemaDiffBuilder.deleteTableColumn(fieldName);
      } else {
        // check type
        String tableColumnType = field.getValue();
        if (!isFieldExistsInSchema(newTableSchema, tickSurroundedFieldName)) {
          if (partitionKeys.contains(fieldName)) {
            // Partition key does not have to be part of the storage schema
            continue;
          }
          // We will log this and continue. Hive schema is a superset of all parquet schemas
          LOG.warn("Ignoring table column " + fieldName + " as its not present in the parquet schema");
          continue;
        }
        tableColumnType = tableColumnType.replaceAll("\\s+", "");

        String expectedType = getExpectedType(newTableSchema, tickSurroundedFieldName);
        expectedType = expectedType.replaceAll("\\s+", "");
        expectedType = expectedType.replaceAll("`", "");

        if (!tableColumnType.equalsIgnoreCase(expectedType)) {
          // check for incremental queries, the schema type change is allowed as per evolution
          // rules
          if (!isSchemaTypeUpdateAllowed(tableColumnType, expectedType)) {
            throw new HoodieHiveSyncException("Could not convert field Type from " + tableColumnType + " to "
                + expectedType + " for field " + fieldName);
          }
          schemaDiffBuilder.updateTableColumn(fieldName, getExpectedType(newTableSchema, tickSurroundedFieldName));
        }
      }
      tableColumns.add(tickSurroundedFieldName);
    }

    for (Map.Entry<String, String> entry : newTableSchema.entrySet()) {
      if (!tableColumns.contains(entry.getKey().toLowerCase())) {
        schemaDiffBuilder.addTableColumn(entry.getKey(), entry.getValue());
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Difference between schemas: " + schemaDiffBuilder.build().toString());
    }

    return schemaDiffBuilder.build();
  }

  private static String getExpectedType(Map<String, String> newTableSchema, String fieldName) {
    for (Map.Entry<String, String> entry : newTableSchema.entrySet()) {
      if (entry.getKey().toLowerCase().equals(fieldName)) {
        return entry.getValue();
      }
    }
    return null;
  }

  private static boolean isFieldExistsInSchema(Map<String, String> newTableSchema, String fieldName) {
    for (String entry : newTableSchema.keySet()) {
      if (entry.toLowerCase().equals(fieldName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns equivalent Hive table schema read from a parquet file.
   *
   * @param messageType : Parquet Schema
   * @return : Hive Table schema read from parquet file MAP[String,String]
   */
  public static Map<String, String> convertParquetSchemaToHiveSchema(MessageType messageType, boolean supportTimestamp) throws IOException {
    return convertMapSchemaToHiveSchema(parquetSchemaToMapSchema(messageType, supportTimestamp, true));
  }

  /**
   * Returns equivalent Hive table Field schema read from a parquet file.
   *
   * @param messageType : Parquet Schema
   * @return : Hive Table schema read from parquet file List[FieldSchema] without partitionField
   */
  public static List<FieldSchema> convertParquetSchemaToHiveFieldSchema(MessageType messageType, HiveSyncConfig syncConfig) throws IOException {
    return convertMapSchemaToHiveFieldSchema(parquetSchemaToMapSchema(messageType, syncConfig.hiveSyncConfigParams.supportTimestamp, false), syncConfig);
  }

  /**
   * Returns schema in Map<String,String> form read from a parquet file.
   *
   * @param messageType : parquet Schema
   * @param supportTimestamp
   * @param doFormat : This option controls whether schema will have spaces in the value part of the schema map. This is required because spaces in complex schema trips the HMS create table calls.
   *                 This value will be false for HMS but true for QueryBasedDDLExecutors
   * @return : Intermediate schema in the form of Map<String, String>
   */
  public static LinkedHashMap<String, String> parquetSchemaToMapSchema(MessageType messageType, boolean supportTimestamp, boolean doFormat) throws IOException {
    LinkedHashMap<String, String> schema = new LinkedHashMap<>();
    List<Type> parquetFields = messageType.getFields();
    for (Type parquetType : parquetFields) {
      StringBuilder result = new StringBuilder();
      String key = parquetType.getName();
      if (parquetType.isRepetition(Type.Repetition.REPEATED)) {
        result.append(createHiveArray(parquetType, "", supportTimestamp, doFormat));
      } else {
        result.append(convertField(parquetType, supportTimestamp, doFormat));
      }

      schema.put(key, result.toString());
    }
    return schema;
  }

  public static Map<String, String> convertMapSchemaToHiveSchema(LinkedHashMap<String, String> schema) throws IOException {
    Map<String, String> hiveSchema = new LinkedHashMap<>();
    for (Map.Entry<String,String> entry: schema.entrySet()) {
      hiveSchema.put(hiveCompatibleFieldName(entry.getKey(), false, true), entry.getValue());
    }
    return hiveSchema;
  }

  /**
   * @param schema Intermediate schema in the form of Map<String,String>
   * @param syncConfig
   * @return List of FieldSchema objects derived from schema without the partition fields as the HMS api expects them as different arguments for alter table commands.
   * @throws IOException
   */
  public static List<FieldSchema> convertMapSchemaToHiveFieldSchema(LinkedHashMap<String, String> schema, HiveSyncConfig syncConfig) throws IOException {
    return schema.keySet().stream()
        .map(key -> new FieldSchema(key, schema.get(key).toLowerCase(), ""))
        .filter(field -> !syncConfig.hoodieSyncConfigParams.partitionFields.contains(field.getName()))
        .collect(Collectors.toList());
  }

  /**
   * Convert one field data type of parquet schema into an equivalent Hive schema.
   *
   * @param parquetType : Single parquet field
   * @return : Equivalent sHive schema
   */
  private static String convertField(final Type parquetType, boolean supportTimestamp, boolean doFormat) {
    StringBuilder field = new StringBuilder();
    if (parquetType.isPrimitive()) {
      final PrimitiveType.PrimitiveTypeName parquetPrimitiveTypeName =
          parquetType.asPrimitiveType().getPrimitiveTypeName();
      final OriginalType originalType = parquetType.getOriginalType();
      if (originalType == OriginalType.DECIMAL) {
        final DecimalMetadata decimalMetadata = parquetType.asPrimitiveType().getDecimalMetadata();
        return field.append("DECIMAL(").append(decimalMetadata.getPrecision()).append(doFormat ? " , " : ",")
            .append(decimalMetadata.getScale()).append(")").toString();
      } else if (originalType == OriginalType.DATE) {
        return field.append("DATE").toString();
      } else if (supportTimestamp && originalType == OriginalType.TIMESTAMP_MICROS) {
        return field.append("TIMESTAMP").toString();
      }

      // TODO - fix the method naming here
      return parquetPrimitiveTypeName.convert(new PrimitiveType.PrimitiveTypeNameConverter<String, RuntimeException>() {
        @Override
        public String convertBOOLEAN(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
          return "boolean";
        }

        @Override
        public String convertINT32(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
          return "int";
        }

        @Override
        public String convertINT64(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
          return "bigint";
        }

        @Override
        public String convertINT96(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
          return "timestamp-millis";
        }

        @Override
        public String convertFLOAT(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
          return "float";
        }

        @Override
        public String convertDOUBLE(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
          return "double";
        }

        @Override
        public String convertFIXED_LEN_BYTE_ARRAY(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
          return "binary";
        }

        @Override
        public String convertBINARY(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
          if (originalType == OriginalType.UTF8 || originalType == OriginalType.ENUM) {
            return "string";
          } else {
            return "binary";
          }
        }
      });
    } else {
      GroupType parquetGroupType = parquetType.asGroupType();
      OriginalType originalType = parquetGroupType.getOriginalType();
      if (originalType != null) {
        switch (originalType) {
          case LIST:
            if (parquetGroupType.getFieldCount() != 1) {
              throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
            }
            Type elementType = parquetGroupType.getType(0);
            if (!elementType.isRepetition(Type.Repetition.REPEATED)) {
              throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
            }
            return createHiveArray(elementType, parquetGroupType.getName(), supportTimestamp, doFormat);
          case MAP:
            if (parquetGroupType.getFieldCount() != 1 || parquetGroupType.getType(0).isPrimitive()) {
              throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
            }
            GroupType mapKeyValType = parquetGroupType.getType(0).asGroupType();
            if (!mapKeyValType.isRepetition(Type.Repetition.REPEATED)
                || !mapKeyValType.getOriginalType().equals(OriginalType.MAP_KEY_VALUE)
                || mapKeyValType.getFieldCount() != 2) {
              throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
            }
            Type keyType = mapKeyValType.getType(0);
            if (!keyType.isPrimitive()
                || !keyType.asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.BINARY)
                || !keyType.getOriginalType().equals(OriginalType.UTF8)) {
              throw new UnsupportedOperationException("Map key type must be binary (UTF8): " + keyType);
            }
            Type valueType = mapKeyValType.getType(1);
            return createHiveMap(convertField(keyType, supportTimestamp, doFormat), convertField(valueType, supportTimestamp, doFormat), doFormat);
          case ENUM:
          case UTF8:
            return "string";
          case MAP_KEY_VALUE:
            // MAP_KEY_VALUE was supposed to be used to annotate key and
            // value group levels in a
            // MAP. However, that is always implied by the structure of
            // MAP. Hence, PARQUET-113
            // dropped the requirement for having MAP_KEY_VALUE.
          default:
            throw new UnsupportedOperationException("Cannot convert Parquet type " + parquetType);
        }
      } else {
        // if no original type then it's a record
        return createHiveStruct(parquetGroupType.getFields(), supportTimestamp, doFormat);
      }
    }
  }

  /**
   * Return a 'struct' Hive schema from a list of Parquet fields.
   *
   * @param parquetFields : list of parquet fields
   * @return : Equivalent 'struct' Hive schema
   */
  private static String createHiveStruct(List<Type> parquetFields, boolean supportTimestamp, boolean doFormat) {
    StringBuilder struct = new StringBuilder();
    struct.append(doFormat ? "STRUCT< " : "STRUCT<");
    for (Type field : parquetFields) {
      // TODO: struct field name is only translated to support special char($)
      // We will need to extend it to other collection type
      struct.append(hiveCompatibleFieldName(field.getName(), true, doFormat)).append(doFormat ? " : " : ":");
      struct.append(convertField(field, supportTimestamp, doFormat)).append(doFormat ? ", " : ",");
    }
    struct.delete(struct.length() - (doFormat ? 2 : 1), struct.length()); // Remove the last
    // ", "
    struct.append(">");
    String finalStr = struct.toString();
    // Struct cannot have - in them. userstore_udr_entities has uuid in struct. This breaks the
    // schema.
    // HDrone sync should not fail because of this.
    finalStr = finalStr.replaceAll("-", "_");
    return finalStr;
  }

  private static String hiveCompatibleFieldName(String fieldName, boolean isNested, boolean doFormat) {
    String result = fieldName;
    if (isNested) {
      result = ColumnNameXLator.translateNestedColumn(fieldName);
    }
    return doFormat ? tickSurround(result) : result;
  }

  private static String tickSurround(String result) {
    if (!result.startsWith("`")) {
      result = "`" + result;
    }
    if (!result.endsWith("`")) {
      result = result + "`";
    }
    return result;
  }

  private static String removeSurroundingTick(String result) {
    if (result.startsWith("`") && result.endsWith("`")) {
      result = result.substring(1, result.length() - 1);
    }

    return result;
  }

  /**
   * Create a 'Map' schema from Parquet map field.
   */
  private static String createHiveMap(String keyType, String valueType, boolean doFormat) {
    return (doFormat ? "MAP< " : "MAP<") + keyType + (doFormat ? ", " : ",") + valueType + ">";
  }

  /**
   * Create an Array Hive schema from equivalent parquet list type.
   */
  private static String createHiveArray(Type elementType, String elementName, boolean supportTimestamp, boolean doFormat) {
    StringBuilder array = new StringBuilder();
    array.append(doFormat ? "ARRAY< " : "ARRAY<");
    if (elementType.isPrimitive()) {
      array.append(convertField(elementType, supportTimestamp, doFormat));
    } else {
      final GroupType groupType = elementType.asGroupType();
      final List<Type> groupFields = groupType.getFields();
      if (groupFields.size() > 1 || (groupFields.size() == 1
          && (elementType.getName().equals("array") || elementType.getName().equals(elementName + "_tuple")))) {
        array.append(convertField(elementType, supportTimestamp, doFormat));
      } else {
        array.append(convertField(groupType.getFields().get(0), supportTimestamp, doFormat));
      }
    }
    array.append(">");
    return array.toString();
  }

  public static boolean isSchemaTypeUpdateAllowed(String prevType, String newType) {
    if (prevType == null || prevType.trim().isEmpty() || newType == null || newType.trim().isEmpty()) {
      return false;
    }
    prevType = prevType.toLowerCase();
    newType = newType.toLowerCase();
    if (prevType.equals(newType)) {
      return true;
    } else if (prevType.equalsIgnoreCase("int") && newType.equalsIgnoreCase("bigint")) {
      return true;
    } else if (prevType.equalsIgnoreCase("float") && newType.equalsIgnoreCase("double")) {
      return true;
    } else {
      return prevType.contains("struct") && newType.toLowerCase().contains("struct");
    }
  }

  public static String generateSchemaString(MessageType storageSchema) throws IOException {
    return generateSchemaString(storageSchema, Collections.EMPTY_LIST);
  }

  public static String generateSchemaString(MessageType storageSchema, List<String> colsToSkip) throws IOException {
    return generateSchemaString(storageSchema, colsToSkip, false);
  }

  public static String generateSchemaString(MessageType storageSchema, List<String> colsToSkip, boolean supportTimestamp) throws IOException {
    Map<String, String> hiveSchema = convertParquetSchemaToHiveSchema(storageSchema, supportTimestamp);
    StringBuilder columns = new StringBuilder();
    for (Map.Entry<String, String> hiveSchemaEntry : hiveSchema.entrySet()) {
      if (!colsToSkip.contains(removeSurroundingTick(hiveSchemaEntry.getKey()))) {
        columns.append(hiveSchemaEntry.getKey()).append(" ");
        columns.append(hiveSchemaEntry.getValue()).append(", ");
      }
    }
    // Remove the last ", "
    columns.delete(columns.length() - 2, columns.length());
    return columns.toString();
  }

  public static String generateCreateDDL(String tableName, MessageType storageSchema, HiveSyncConfig config, String inputFormatClass,
                                         String outputFormatClass, String serdeClass, Map<String, String> serdeProperties,
                                         Map<String, String> tableProperties) throws IOException {
    Map<String, String> hiveSchema = convertParquetSchemaToHiveSchema(storageSchema, config.hiveSyncConfigParams.supportTimestamp);
    String columns = generateSchemaString(storageSchema, config.hoodieSyncConfigParams.partitionFields, config.hiveSyncConfigParams.supportTimestamp);

    List<String> partitionFields = new ArrayList<>();
    for (String partitionKey : config.hoodieSyncConfigParams.partitionFields) {
      String partitionKeyWithTicks = tickSurround(partitionKey);
      partitionFields.add(new StringBuilder().append(partitionKeyWithTicks).append(" ")
          .append(getPartitionKeyType(hiveSchema, partitionKeyWithTicks)).toString());
    }

    String partitionsStr = String.join(",", partitionFields);
    StringBuilder sb = new StringBuilder();
    if (config.hiveSyncConfigParams.createManagedTable) {
      sb.append("CREATE TABLE IF NOT EXISTS ");
    } else {
      sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");
    }
    sb.append(HIVE_ESCAPE_CHARACTER).append(config.hoodieSyncConfigParams.databaseName).append(HIVE_ESCAPE_CHARACTER)
            .append(".").append(HIVE_ESCAPE_CHARACTER).append(tableName).append(HIVE_ESCAPE_CHARACTER);
    sb.append("( ").append(columns).append(")");
    if (!config.hoodieSyncConfigParams.partitionFields.isEmpty()) {
      sb.append(" PARTITIONED BY (").append(partitionsStr).append(")");
    }
    if (config.hiveSyncConfigParams.bucketSpec != null) {
      sb.append(' ' + config.hiveSyncConfigParams.bucketSpec + ' ');
    }
    sb.append(" ROW FORMAT SERDE '").append(serdeClass).append("'");
    if (serdeProperties != null && !serdeProperties.isEmpty()) {
      sb.append(" WITH SERDEPROPERTIES (").append(propertyToString(serdeProperties)).append(")");
    }
    sb.append(" STORED AS INPUTFORMAT '").append(inputFormatClass).append("'");
    sb.append(" OUTPUTFORMAT '").append(outputFormatClass).append("' LOCATION '").append(config.hoodieSyncConfigParams.basePath).append("'");

    if (tableProperties != null && !tableProperties.isEmpty()) {
      sb.append(" TBLPROPERTIES(").append(propertyToString(tableProperties)).append(")");
    }
    return sb.toString();
  }

  private static String propertyToString(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String, String> entry: properties.entrySet()) {
      if (!first) {
        sb.append(",");
      }
      sb.append("'").append(entry.getKey()).append("'='").append(entry.getValue()).append("'");
      first = false;
    }
    return sb.toString();
  }

  public static String getPartitionKeyType(Map<String, String> hiveSchema, String partitionKey) {
    if (hiveSchema.containsKey(partitionKey)) {
      return hiveSchema.get(partitionKey);
    }
    // Default the unknown partition fields to be String
    // TODO - all partition fields should be part of the schema. datestr is treated as special.
    // Dont do that
    return "String";
  }
}
