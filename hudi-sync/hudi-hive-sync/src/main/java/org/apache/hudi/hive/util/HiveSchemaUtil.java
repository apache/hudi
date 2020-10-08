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

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.SchemaDifference;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema Utilities.
 */
public class HiveSchemaUtil {

  private static final Logger LOG = LogManager.getLogger(HiveSchemaUtil.class);
  public static final String HIVE_ESCAPE_CHARACTER = "`";

  /**
   * Get the schema difference between the storage schema and hive table schema.
   */
  public static SchemaDifference getSchemaDifference(Schema storageSchema, Map<String, String> tableSchema,
      List<String> partitionKeys) {
    Map<String, String> newTableSchema;
    try {
      newTableSchema = convertAvroSchemaToHiveSchema(storageSchema);
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to convert parquet schema to hive schema", e);
    }
    LOG.info("Getting schema difference for " + tableSchema + "\r\n\r\n" + newTableSchema);
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
    LOG.info("Difference between schemas: " + schemaDiffBuilder.build().toString());

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
  public static Map<String, String> convertAvroSchemaToHiveSchema(Schema avroSchema) throws IOException {
    Map<String, String> schema = new LinkedHashMap<>();
    List<Schema.Field> schemaFields = avroSchema.getFields();
    for (Schema.Field schemaField : schemaFields) {
      StringBuilder result = new StringBuilder();
      String key = schemaField.name();
      result.append(convertFieldFromAvro(schemaField.schema()));
      schema.put(hiveCompatibleFieldName(key, false), result.toString());
    }
    return schema;
  }

  /**
   * Convert one field data type of parquet schema into an equivalent Hive schema.
   *
   * @param parquetType : Single paruet field
   * @return : Equivalent sHive schema
   */
  private static String convertFieldFromAvro(final Schema schema) {
    StringBuilder field = new StringBuilder();
    Schema.Type type = schema.getType();
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Decimal) {
        return field.append("DECIMAL(").append(((LogicalTypes.Decimal) logicalType).getPrecision()).append(" , ")
            .append(((LogicalTypes.Decimal) logicalType).getScale()).append(")").toString();
      } else if (logicalType instanceof LogicalTypes.Date) {
        return field.append("DATE").toString();
      } else {
        Log.info("not handle the type transform");
      }
    }
    if (type.equals(Schema.Type.BOOLEAN)) {
      return field.append("boolean").toString();
    } else if (type.equals(Schema.Type.INT)) {
      return field.append("int").toString();
    } else if (type.equals(Schema.Type.LONG)) {
      return field.append("bigint").toString();
    } else if (type.equals(Schema.Type.FLOAT)) {
      return field.append("float").toString();
    } else if (type.equals(Schema.Type.DOUBLE)) {
      return field.append("double").toString();
    } else if (type.equals(Schema.Type.BYTES)) {
      return field.append("binary").toString();
    } else if (type.equals(Schema.Type.STRING)) {
      return field.append("string").toString();
    } else if (type.equals(Schema.Type.RECORD)) {
      List<Pair<String, Schema>> noNullSchemaFields = new ArrayList<>();
      for (Schema.Field fieldItem : schema.getFields()) {
        if (fieldItem.schema().getType().equals(Schema.Type.NULL)) {
          continue; // Avro nulls are not encoded, unless they are null unions
        }
        noNullSchemaFields.add(new ImmutablePair<String, Schema>(fieldItem.name(), fieldItem.schema()));
      }
      return createHiveStructFromAvro(noNullSchemaFields);
    } else if (type.equals(Schema.Type.ENUM)) {
      return field.append("string").toString();
    } else if (type.equals(Schema.Type.ARRAY)) {
      Schema elementType = schema.getElementType();
      String schemaName = schema.getName();
      return createHiveArrayFromAvro(schemaName, elementType);
    } else if (type.equals(Schema.Type.MAP)) {
      String keyType = "string";
      String valType = convertFieldFromAvro(schema.getValueType());
      return createHiveMap(keyType, valType);
    } else if (type.equals(Schema.Type.FIXED)) {
      return field.append("binary").toString();
    } else if (type.equals(Schema.Type.UNION)) {
      List<Pair<String, Schema>> noNullSchemaFields = new ArrayList<>();
      int index = 0;
      for (Schema childSchema : schema.getTypes()) {
        if (!childSchema.getType().equals(Schema.Type.NULL)) {
          noNullSchemaFields.add(new ImmutablePair("member" + index++, childSchema));
        }
      }
      // If we only get a null and one other type then its a simple optional field
      // otherwise construct a union container
      switch (noNullSchemaFields.size()) {
        case 0:
          throw new UnsupportedOperationException("Cannot convert Avro union of only nulls");
        case 1:
          return convertFieldFromAvro(noNullSchemaFields.get(0).getRight());
        default: // complex union type
          return createHiveStructFromAvro(noNullSchemaFields);
      }
    } else {
      throw new UnsupportedOperationException("Cannot convert Avro type " + type);
    }
  }

  /**
   * Return a 'struct' Hive schema from a list of Parquet fields.
   *
   * @param parquetFields : list of parquet fields
   * @return : Equivalent 'struct' Hive schema
   */
  private static String createHiveStructFromAvro(List<Pair<String, Schema>> fields) {
    StringBuilder struct = new StringBuilder();
    struct.append("STRUCT< ");
    for (Pair<String, Schema> field : fields) {
      // TODO: struct field name is only translated to support special char($)
      // We will need to extend it to other collection type
      struct.append(hiveCompatibleFieldName(field.getLeft(), true)).append(" : ");
      struct.append(convertFieldFromAvro(field.getRight())).append(", ");
    }
    struct.delete(struct.length() - 2, struct.length()); // Remove the last
    // ", "
    struct.append(">");
    String finalStr = struct.toString();
    // Struct cannot have - in them. userstore_udr_entities has uuid in struct. This breaks the
    // schema.
    // HDrone sync should not fail because of this.
    finalStr = finalStr.replaceAll("-", "_");
    return finalStr;
  }

  private static String hiveCompatibleFieldName(String fieldName, boolean isNested) {
    String result = fieldName;
    if (isNested) {
      result = ColumnNameXLator.translateNestedColumn(fieldName);
    }
    return tickSurround(result);
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
  private static String createHiveMap(String keyType, String valueType) {
    return "MAP< " + keyType + ", " + valueType + ">";
  }

  /**
   * Create an Array Hive schema from equivalent parquet list type.
   */
  private static String createHiveArrayFromAvro(String schemaName, Schema elementType) {
    StringBuilder array = new StringBuilder();
    array.append("ARRAY< ");
    if (elementType.getType().equals(Schema.Type.RECORD) && (elementType.getFields().size() == 1
        && !elementType.getName().equals("array") && !elementType.getName().endsWith("_tuple"))) {
      array.append(convertFieldFromAvro(elementType.getFields().get(0).schema()));
    } else {
      array.append(convertFieldFromAvro(elementType));
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

  public static String generateSchemaString(Schema storageSchema) throws IOException {
    return generateSchemaString(storageSchema, new ArrayList<>());
  }

  public static String generateSchemaString(Schema storageSchema, List<String> colsToSkip) throws IOException {
    Map<String, String> hiveSchema = convertAvroSchemaToHiveSchema(storageSchema);
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

  public static String generateCreateDDL(String tableName, Schema storageSchema, HiveSyncConfig config, String inputFormatClass,
                                         String outputFormatClass, String serdeClass) throws IOException {
    Map<String, String> hiveSchema = convertAvroSchemaToHiveSchema(storageSchema);
    String columns = generateSchemaString(storageSchema, config.partitionFields);

    List<String> partitionFields = new ArrayList<>();
    for (String partitionKey : config.partitionFields) {
      String partitionKeyWithTicks = tickSurround(partitionKey);
      partitionFields.add(new StringBuilder().append(partitionKeyWithTicks).append(" ")
          .append(getPartitionKeyType(hiveSchema, partitionKeyWithTicks)).toString());
    }

    String partitionsStr = String.join(",", partitionFields);
    StringBuilder sb = new StringBuilder("CREATE EXTERNAL TABLE  IF NOT EXISTS ");
    sb.append(HIVE_ESCAPE_CHARACTER).append(config.databaseName).append(HIVE_ESCAPE_CHARACTER)
            .append(".").append(HIVE_ESCAPE_CHARACTER).append(tableName).append(HIVE_ESCAPE_CHARACTER);
    sb.append("( ").append(columns).append(")");
    if (!config.partitionFields.isEmpty()) {
      sb.append(" PARTITIONED BY (").append(partitionsStr).append(")");
    }
    sb.append(" ROW FORMAT SERDE '").append(serdeClass).append("'");
    sb.append(" STORED AS INPUTFORMAT '").append(inputFormatClass).append("'");
    sb.append(" OUTPUTFORMAT '").append(outputFormatClass).append("' LOCATION '").append(config.basePath).append("'");
    return sb.toString();
  }

  private static String getPartitionKeyType(Map<String, String> hiveSchema, String partitionKey) {
    if (hiveSchema.containsKey(partitionKey)) {
      return hiveSchema.get(partitionKey);
    }
    // Default the unknown partition fields to be String
    // TODO - all partition fields should be part of the schema. datestr is treated as special.
    // Dont do that
    return "String";
  }
}
