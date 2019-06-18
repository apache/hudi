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

package com.uber.hoodie.hive.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.HoodieLogFormat.Reader;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.hive.HiveSyncConfig;
import com.uber.hoodie.hive.HoodieHiveSyncException;
import com.uber.hoodie.hive.SchemaDifference;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schema Utilities
 */
public class SchemaUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtil.class);

  /**
   * Get the schema difference between the storage schema and hive table schema
   */
  public static SchemaDifference getSchemaDifference(MessageType storageSchema,
      Map<String, String> tableSchema, List<String> partitionKeys) {
    Map<String, String> newTableSchema;
    try {
      newTableSchema = convertParquetSchemaToHiveSchema(storageSchema);
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to convert parquet schema to hive schema", e);
    }
    LOG.info("Getting schema difference for " + tableSchema + "\r\n\r\n" + newTableSchema);
    SchemaDifference.Builder schemaDiffBuilder = SchemaDifference
        .newBuilder(storageSchema, tableSchema);
    Set<String> tableColumns = Sets.newHashSet();

    for (Map.Entry<String, String> field : tableSchema.entrySet()) {
      String fieldName = field.getKey().toLowerCase();
      String tickSurroundedFieldName = tickSurround(fieldName);
      if (!isFieldExistsInSchema(newTableSchema, tickSurroundedFieldName) && !partitionKeys
          .contains(
              fieldName)) {
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
          LOG.warn(
              "Ignoring table column " + fieldName + " as its not present in the parquet schema");
          continue;
        }
        tableColumnType = tableColumnType.replaceAll("\\s+", "");

        String expectedType = getExpectedType(newTableSchema, tickSurroundedFieldName);
        expectedType = expectedType.replaceAll("\\s+", "");
        expectedType = expectedType.replaceAll("`", "");

        if (!tableColumnType.equalsIgnoreCase(expectedType)) {
          // check for incremental datasets, the schema type change is allowed as per evolution
          // rules
          if (!isSchemaTypeUpdateAllowed(tableColumnType, expectedType)) {
            throw new HoodieHiveSyncException(
                "Could not convert field Type from " + tableColumnType + " to " + expectedType
                    + " for field " + fieldName);
          }
          schemaDiffBuilder.updateTableColumn(fieldName,
              getExpectedType(newTableSchema, tickSurroundedFieldName));
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

  private static boolean isFieldExistsInSchema(Map<String, String> newTableSchema,
      String fieldName) {
    for (String entry : newTableSchema.keySet()) {
      if (entry.toLowerCase().equals(fieldName)) {
        return true;
      }
    }
    return false;
  }


  /**
   * Returns equivalent Hive table schema read from a parquet file
   *
   * @param messageType : Parquet Schema
   * @return : Hive Table schema read from parquet file MAP[String,String]
   */
  public static Map<String, String> convertParquetSchemaToHiveSchema(MessageType messageType)
      throws IOException {
    Map<String, String> schema = Maps.newLinkedHashMap();
    List<Type> parquetFields = messageType.getFields();
    for (Type parquetType : parquetFields) {
      StringBuilder result = new StringBuilder();
      String key = parquetType.getName();
      if (parquetType.isRepetition(Type.Repetition.REPEATED)) {
        result.append(createHiveArray(parquetType, ""));
      } else {
        result.append(convertField(parquetType));
      }

      schema.put(hiveCompatibleFieldName(key, false), result.toString());
    }
    return schema;
  }

  /**
   * Convert one field data type of parquet schema into an equivalent Hive schema
   *
   * @param parquetType : Single paruet field
   * @return : Equivalent sHive schema
   */
  private static String convertField(final Type parquetType) {
    StringBuilder field = new StringBuilder();
    if (parquetType.isPrimitive()) {
      final PrimitiveType.PrimitiveTypeName parquetPrimitiveTypeName = parquetType.asPrimitiveType()
          .getPrimitiveTypeName();
      final OriginalType originalType = parquetType.getOriginalType();
      if (originalType == OriginalType.DECIMAL) {
        final DecimalMetadata decimalMetadata = parquetType.asPrimitiveType().getDecimalMetadata();
        return field.append("DECIMAL(").append(decimalMetadata.getPrecision()).append(" , ")
            .append(decimalMetadata.getScale()).append(")").toString();
      }
      // TODO - fix the method naming here
      return parquetPrimitiveTypeName
          .convert(new PrimitiveType.PrimitiveTypeNameConverter<String, RuntimeException>() {
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
            public String convertFIXED_LEN_BYTE_ARRAY(
                PrimitiveType.PrimitiveTypeName primitiveTypeName) {
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
            return createHiveArray(elementType, parquetGroupType.getName());
          case MAP:
            if (parquetGroupType.getFieldCount() != 1 || parquetGroupType.getType(0)
                .isPrimitive()) {
              throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
            }
            GroupType mapKeyValType = parquetGroupType.getType(0).asGroupType();
            if (!mapKeyValType.isRepetition(Type.Repetition.REPEATED)
                || !mapKeyValType.getOriginalType().equals(OriginalType.MAP_KEY_VALUE)
                || mapKeyValType.getFieldCount() != 2) {
              throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
            }
            Type keyType = mapKeyValType.getType(0);
            if (!keyType.isPrimitive() || !keyType.asPrimitiveType().getPrimitiveTypeName()
                .equals(PrimitiveType.PrimitiveTypeName.BINARY)
                || !keyType.getOriginalType().equals(OriginalType.UTF8)) {
              throw new UnsupportedOperationException(
                  "Map key type must be binary (UTF8): " + keyType);
            }
            Type valueType = mapKeyValType.getType(1);
            return createHiveMap(convertField(keyType), convertField(valueType));
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
        return createHiveStruct(parquetGroupType.getFields());
      }
    }
  }

  /**
   * Return a 'struct' Hive schema from a list of Parquet fields
   *
   * @param parquetFields : list of parquet fields
   * @return : Equivalent 'struct' Hive schema
   */
  private static String createHiveStruct(List<Type> parquetFields) {
    StringBuilder struct = new StringBuilder();
    struct.append("STRUCT< ");
    for (Type field : parquetFields) {
      //TODO: struct field name is only translated to support special char($)
      //We will need to extend it to other collection type
      struct.append(hiveCompatibleFieldName(field.getName(), true)).append(" : ");
      struct.append(convertField(field)).append(", ");
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
   * Create a 'Map' schema from Parquet map field
   */
  private static String createHiveMap(String keyType, String valueType) {
    return "MAP< " + keyType + ", " + valueType + ">";
  }

  /**
   * Create an Array Hive schema from equivalent parquet list type
   */
  private static String createHiveArray(Type elementType, String elementName) {
    StringBuilder array = new StringBuilder();
    array.append("ARRAY< ");
    if (elementType.isPrimitive()) {
      array.append(convertField(elementType));
    } else {
      final GroupType groupType = elementType.asGroupType();
      final List<Type> groupFields = groupType.getFields();
      if (groupFields.size() > 1 || (groupFields.size() == 1 && (
          elementType.getName().equals("array") || elementType.getName()
              .equals(elementName + "_tuple")))) {
        array.append(convertField(elementType));
      } else {
        array.append(convertField(groupType.getFields().get(0)));
      }
    }
    array.append(">");
    return array.toString();
  }

  public static boolean isSchemaTypeUpdateAllowed(String prevType, String newType) {
    if (prevType == null || prevType.trim().isEmpty() || newType == null || newType.trim()
        .isEmpty()) {
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
    } else if (prevType.contains("struct") && newType.toLowerCase().contains("struct")) {
      return true;
    }
    return false;
  }

  public static String generateSchemaString(MessageType storageSchema) throws IOException {
    return generateSchemaString(storageSchema, new ArrayList<>());
  }

  public static String generateSchemaString(MessageType storageSchema, List<String> colsToSkip) throws IOException {
    Map<String, String> hiveSchema = convertParquetSchemaToHiveSchema(storageSchema);
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

  public static String generateCreateDDL(MessageType storageSchema, HiveSyncConfig config,
      String inputFormatClass, String outputFormatClass, String serdeClass) throws IOException {
    Map<String, String> hiveSchema = convertParquetSchemaToHiveSchema(storageSchema);
    String columns = generateSchemaString(storageSchema, config.partitionFields);

    List<String> partitionFields = new ArrayList<>();
    for (String partitionKey : config.partitionFields) {
      String partitionKeyWithTicks = tickSurround(partitionKey);
      partitionFields.add(new StringBuilder().append(partitionKey).append(" ")
          .append(getPartitionKeyType(hiveSchema, partitionKeyWithTicks)).toString());
    }

    String partitionsStr = partitionFields.stream().collect(Collectors.joining(","));
    StringBuilder sb = new StringBuilder("CREATE EXTERNAL TABLE  IF NOT EXISTS ");
    sb = sb.append(config.databaseName).append(".").append(config.tableName);
    sb = sb.append("( ").append(columns).append(")");
    if (!config.partitionFields.isEmpty()) {
      sb = sb.append(" PARTITIONED BY (").append(partitionsStr).append(")");
    }
    sb = sb.append(" ROW FORMAT SERDE '").append(serdeClass).append("'");
    sb = sb.append(" STORED AS INPUTFORMAT '").append(inputFormatClass).append("'");
    sb = sb.append(" OUTPUTFORMAT '").append(outputFormatClass).append("' LOCATION '")
        .append(config.basePath).append("'");
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

  /**
   * Read the schema from the log file on path
   */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public static MessageType readSchemaFromLogFile(FileSystem fs, Path path) throws IOException {
    Reader reader = HoodieLogFormat.newReader(fs, new HoodieLogFile(path), null);
    HoodieAvroDataBlock lastBlock = null;
    while (reader.hasNext()) {
      HoodieLogBlock block = reader.next();
      if (block instanceof HoodieAvroDataBlock) {
        lastBlock = (HoodieAvroDataBlock) block;
      }
    }
    reader.close();
    if (lastBlock != null) {
      return new AvroSchemaConverter().convert(lastBlock.getSchema());
    }
    return null;
  }
}
