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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.SchemaDifference;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_CREATE_MANAGED_TABLE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_BUCKET_SYNC_SPEC;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;

/**
 * Schema Utilities.
 */
public class HiveSchemaUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HiveSchemaUtil.class);
  public static final String HIVE_ESCAPE_CHARACTER = "`";

  public static final String BOOLEAN_TYPE_NAME = "boolean";
  public static final String INT_TYPE_NAME = "int";
  public static final String BIGINT_TYPE_NAME = "bigint";
  public static final String FLOAT_TYPE_NAME = "float";
  public static final String DOUBLE_TYPE_NAME = "double";
  public static final String STRING_TYPE_NAME = "string";
  public static final String BINARY_TYPE_NAME = "binary";
  public static final String DATE_TYPE_NAME = "DATE";

  /**
   * Get the schema difference between the storage schema and hive table schema.
   */
  public static SchemaDifference getSchemaDifference(HoodieSchema storageSchema, Map<String, String> tableSchema,
                                                     List<String> partitionKeys) {
    return getSchemaDifference(storageSchema, tableSchema, partitionKeys, false);
  }

  public static SchemaDifference getSchemaDifference(HoodieSchema storageSchema, Map<String, String> tableSchema,
      List<String> partitionKeys, boolean supportTimestamp) {
    Map<String, String> newTableSchema;
    try {
      newTableSchema = convertSchemaToHiveSchema(storageSchema, supportTimestamp);
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to convert schema to hive schema", e);
    }
    LOG.debug("Getting schema difference for {} \r\n\r\n{}", tableSchema, newTableSchema);

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
          // We will log this and continue. Hive schema is a superset of all schemas
          LOG.info("Ignoring table column {} as its not present in the table schema", fieldName);
          continue;
        }
        tableColumnType = tableColumnType.replaceAll("\\s+", "");

        String expectedType = getExpectedType(newTableSchema, tickSurroundedFieldName);
        expectedType = expectedType.replaceAll("\\s+", "");
        expectedType = expectedType.replaceAll("`", "");

        if (!tableColumnType.equalsIgnoreCase(expectedType)) {
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
    SchemaDifference result = schemaDiffBuilder.build();
    LOG.debug("Difference between schemas: {}", result);
    return result;
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
   * Returns equivalent Hive table schema for the provided table schema.
   *
   * @param schema Table Schema
   * @return Hive Table schema MAP[String,String]
   */
  public static Map<String, String> convertSchemaToHiveSchema(HoodieSchema schema, boolean supportTimestamp) throws IOException {
    return convertMapSchemaToHiveSchema(hoodieSchemaToMapSchema(schema, supportTimestamp, true));
  }

  /**
   * Returns equivalent Hive table Field schema for the provided table schema.
   *
   * @param schema Table Schema
   * @return Hive Table schema without partitionField
   */
  public static List<FieldSchema> convertSchemaToHiveFieldSchema(HoodieSchema schema, HiveSyncConfig syncConfig) {
    return convertMapSchemaToHiveFieldSchema(hoodieSchemaToMapSchema(schema, syncConfig.getBoolean(HIVE_SUPPORT_TIMESTAMP_TYPE), false), syncConfig);
  }

  /**
   * Returns schema in Map<String,String> form translated from the table's schema.
   *
   * @param schema the current schema
   * @param supportTimestamp
   * @param doFormat This option controls whether schema will have spaces in the value part of the schema map. This is required because spaces in complex schema trips the HMS create table calls.
   *                 This value will be false for HMS but true for QueryBasedDDLExecutors
   * @return Intermediate schema in the form of Map<String, String>
   */
  public static LinkedHashMap<String, String> hoodieSchemaToMapSchema(HoodieSchema schema, boolean supportTimestamp, boolean doFormat) {
    LinkedHashMap<String, String> hiveSchema = new LinkedHashMap<>();
    List<HoodieSchemaField> fields = schema.getFields();
    for (HoodieSchemaField field : fields) {
      hiveSchema.put(field.name(), convertField(field.schema(), supportTimestamp, doFormat));
    }
    return hiveSchema;
  }

  public static Map<String, String> convertMapSchemaToHiveSchema(LinkedHashMap<String, String> schema) {
    Map<String, String> hiveSchema = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry: schema.entrySet()) {
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
  public static List<FieldSchema> convertMapSchemaToHiveFieldSchema(LinkedHashMap<String, String> schema, HiveSyncConfig syncConfig) {
    return schema.keySet().stream()
        .map(key -> new FieldSchema(key, schema.get(key).toLowerCase(), ""))
        .filter(field -> !syncConfig.getSplitStrings(META_SYNC_PARTITION_FIELDS).contains(field.getName()))
        .collect(Collectors.toList());
  }

  /**
   * Convert one field data type of Hoodie schema into an equivalent Hive schema.
   *
   * @param fieldSchema the current field's schema
   * @param supportTimestamp
   * @param doFormat This option controls whether schema will have spaces in the value part of the schema map. This is required because spaces in complex schema trips the HMS create table calls.
   * @return : Equivalent Hive schema
   */
  private static String convertField(final HoodieSchema fieldSchema, boolean supportTimestamp, boolean doFormat) {
    HoodieSchema nonNullType = fieldSchema.getNonNullType();
    switch (nonNullType.getType()) {
      case INT:
        return INT_TYPE_NAME;
      case LONG:
        return BIGINT_TYPE_NAME;
      case FLOAT:
        return FLOAT_TYPE_NAME;
      case DOUBLE:
        return DOUBLE_TYPE_NAME;
      case BOOLEAN:
        return BOOLEAN_TYPE_NAME;
      case STRING:
      case ENUM:
        return STRING_TYPE_NAME;
      case BYTES:
      case UUID:
      case FIXED:
        return BINARY_TYPE_NAME;
      case DATE:
        return DATE_TYPE_NAME;
      case TIMESTAMP:
        if (!supportTimestamp) {
          return BIGINT_TYPE_NAME;
        } else {
          return "TIMESTAMP";
        }
      case TIME:
        HoodieSchema.Time timeSchema = (HoodieSchema.Time) nonNullType;
        return timeSchema.getPrecision() == HoodieSchema.TimePrecision.MILLIS ? INT_TYPE_NAME : BIGINT_TYPE_NAME;
      case DECIMAL:
        HoodieSchema.Decimal decimalSchema = (HoodieSchema.Decimal) nonNullType;
        return "DECIMAL(" + decimalSchema.getPrecision() + (doFormat ? " , " : ",") + decimalSchema.getScale() + ")";
      case ARRAY:
        HoodieSchema elementType = nonNullType.getElementType();
        return createHiveArray(elementType, supportTimestamp, doFormat);
      case MAP:
        HoodieSchema keyType = nonNullType.getKeyType();
        HoodieSchema valueType = nonNullType.getValueType();
        return createHiveMap(convertField(keyType, supportTimestamp, doFormat), convertField(valueType, supportTimestamp, doFormat), doFormat);
      case RECORD:
        return createHiveStruct(nonNullType.getFields(), supportTimestamp, doFormat);
      default:
        throw new UnsupportedOperationException("Cannot convert type " + nonNullType.getType());
    }
  }

  /**
   * Return a 'struct' Hive schema from a list of fields.
   *
   * @param fields : list of fields
   * @return : Equivalent 'struct' Hive schema
   */
  private static String createHiveStruct(List<HoodieSchemaField> fields, boolean supportTimestamp, boolean doFormat) {
    StringBuilder struct = new StringBuilder();
    struct.append(doFormat ? "STRUCT< " : "STRUCT<");
    for (HoodieSchemaField field : fields) {
      // TODO: struct field name is only translated to support special char($)
      // We will need to extend it to other collection type
      struct.append(hiveCompatibleFieldName(field.name(), true, doFormat)).append(doFormat ? " : " : ":");
      struct.append(convertField(field.schema(), supportTimestamp, doFormat)).append(doFormat ? ", " : ",");
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
   * Create a 'Map' schema from a map field.
   */
  private static String createHiveMap(String keyType, String valueType, boolean doFormat) {
    return (doFormat ? "MAP< " : "MAP<") + keyType + (doFormat ? ", " : ",") + valueType + ">";
  }

  /**
   * Create an Array Hive schema from equivalent list type.
   */
  private static String createHiveArray(HoodieSchema elementSchema, boolean supportTimestamp, boolean doFormat) {
    return new StringBuilder()
        .append(doFormat ? "ARRAY< " : "ARRAY<")
        .append(convertField(elementSchema, supportTimestamp, doFormat))
        .append(">")
        .toString();
  }

  public static String generateSchemaString(HoodieSchema storageSchema) throws IOException {
    return generateSchemaString(storageSchema, Collections.emptyList());
  }

  public static String generateSchemaString(HoodieSchema storageSchema, List<String> colsToSkip) throws IOException {
    return generateSchemaString(storageSchema, colsToSkip, false);
  }

  public static String generateSchemaString(HoodieSchema storageSchema, List<String> colsToSkip, boolean supportTimestamp) throws IOException {
    Map<String, String> hiveSchema = convertSchemaToHiveSchema(storageSchema, supportTimestamp);
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

  public static String generateCreateDDL(String tableName, HoodieSchema storageSchema, HiveSyncConfig config, String inputFormatClass,
                                         String outputFormatClass, String serdeClass, Map<String, String> serdeProperties,
                                         Map<String, String> tableProperties) throws IOException {
    Map<String, String> hiveSchema = convertSchemaToHiveSchema(storageSchema, config.getBoolean(HIVE_SUPPORT_TIMESTAMP_TYPE));
    String columns = generateSchemaString(storageSchema, config.getSplitStrings(META_SYNC_PARTITION_FIELDS), config.getBoolean(HIVE_SUPPORT_TIMESTAMP_TYPE));

    List<String> partitionFields = new ArrayList<>();
    for (String partitionKey : config.getSplitStrings(META_SYNC_PARTITION_FIELDS)) {
      String partitionKeyWithTicks = tickSurround(partitionKey);
      partitionFields.add(partitionKeyWithTicks + " "
          + getPartitionKeyType(hiveSchema, partitionKeyWithTicks));
    }

    String partitionsStr = String.join(",", partitionFields);
    StringBuilder sb = new StringBuilder();
    if (config.getBoolean(HIVE_CREATE_MANAGED_TABLE)) {
      sb.append("CREATE TABLE IF NOT EXISTS ");
    } else {
      sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");
    }
    sb.append(HIVE_ESCAPE_CHARACTER).append(config.getStringOrDefault(META_SYNC_DATABASE_NAME)).append(HIVE_ESCAPE_CHARACTER)
            .append(".").append(HIVE_ESCAPE_CHARACTER).append(tableName).append(HIVE_ESCAPE_CHARACTER);
    sb.append("( ").append(columns).append(")");
    if (!config.getSplitStrings(META_SYNC_PARTITION_FIELDS).isEmpty()) {
      sb.append(" PARTITIONED BY (").append(partitionsStr).append(")");
    }
    if (config.getString(HIVE_SYNC_BUCKET_SYNC_SPEC) != null) {
      sb.append(' ' + config.getString(HIVE_SYNC_BUCKET_SYNC_SPEC) + ' ');
    }
    sb.append(" ROW FORMAT SERDE '").append(serdeClass).append("'");
    if (serdeProperties != null && !serdeProperties.isEmpty()) {
      sb.append(" WITH SERDEPROPERTIES (").append(propertyToString(serdeProperties)).append(")");
    }
    sb.append(" STORED AS INPUTFORMAT '").append(inputFormatClass).append("'");
    sb.append(" OUTPUTFORMAT '").append(outputFormatClass).append("' LOCATION '").append(config.getAbsoluteBasePath()).append("'");

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
    return STRING_TYPE_NAME;
  }
}
