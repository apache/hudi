/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.gcp.bigquery;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieException;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Extracts the BigQuery schema from a Hudi table.
 */
public class BigQuerySchemaResolver {
  private static final BigQuerySchemaResolver INSTANCE = new BigQuerySchemaResolver(TableSchemaResolver::new);

  private final Function<HoodieTableMetaClient, TableSchemaResolver> tableSchemaResolverSupplier;

  @VisibleForTesting
  BigQuerySchemaResolver(Function<HoodieTableMetaClient, TableSchemaResolver> tableSchemaResolverSupplier) {
    this.tableSchemaResolverSupplier = tableSchemaResolverSupplier;
  }

  public static BigQuerySchemaResolver getInstance() {
    return INSTANCE;
  }

  /**
   * Get the BigQuery schema for the table. If the BigQuery table is configured with partitioning, the caller must pass in the partition fields so that they are not returned in the schema.
   * If the partition fields are in the schema, it will cause an error when querying the table since BigQuery will treat it as a duplicate column.
   * @param metaClient Meta client for the Hudi table
   * @param partitionFields The fields that are used for partitioning in BigQuery
   * @return The BigQuery schema for the table
   */
  Schema getTableSchema(HoodieTableMetaClient metaClient, List<String> partitionFields) {
    try {
      Schema schema = convertSchema(tableSchemaResolverSupplier.apply(metaClient).getTableAvroSchema());
      if (partitionFields.isEmpty()) {
        return schema;
      } else {
        return Schema.of(schema.getFields().stream().filter(field -> !partitionFields.contains(field.getName())).collect(Collectors.toList()));
      }
    } catch (Exception e) {
      throw new HoodieBigQuerySyncException("Failed to get table schema", e);
    }
  }

  /**
   * Converts a BigQuery schema to the string representation used in the BigQuery SQL command to create the manifest based table.
   * @param schema The BigQuery schema
   * @return The string representation of the schema
   */
  public static String schemaToSqlString(Schema schema) {
    return fieldsToSqlString(schema.getFields());
  }

  private static String fieldsToSqlString(List<Field> fields) {
    return fields.stream().map(field -> {
      String mode = field.getMode() == Field.Mode.REQUIRED ? " NOT NULL" : "";
      String type;
      if (field.getType().getStandardType() == StandardSQLTypeName.STRUCT) {
        type = String.format("STRUCT<%s>", fieldsToSqlString(field.getSubFields()));
      } else {
        type = field.getType().getStandardType().name();
      }
      String name = field.getName();
      if (field.getMode() == Field.Mode.REPEATED) {
        return String.format("`%s` ARRAY<%s>", name, type);
      } else {
        return String.format("`%s` %s%s", name, type, mode);
      }
    }).collect(Collectors.joining(", "));
  }

  @VisibleForTesting
  Schema convertSchema(org.apache.avro.Schema schema) {
    return Schema.of(getFields(schema));
  }

  private Field getField(org.apache.avro.Schema fieldSchema, String name, boolean nullable) {
    final Field.Mode fieldMode = nullable ? Field.Mode.NULLABLE : Field.Mode.REQUIRED;
    StandardSQLTypeName standardSQLTypeName;
    switch (fieldSchema.getType()) {
      case INT:
      case LONG:
        LogicalType logicalType = fieldSchema.getLogicalType();
        if (logicalType == null) {
          standardSQLTypeName = StandardSQLTypeName.INT64;
        } else if (logicalType.equals(LogicalTypes.date())) {
          standardSQLTypeName = StandardSQLTypeName.DATE;
        } else if (logicalType.equals(LogicalTypes.timeMillis()) || logicalType.equals(LogicalTypes.timeMicros())) {
          standardSQLTypeName = StandardSQLTypeName.TIME;
        } else if (logicalType.equals(LogicalTypes.timestampMillis()) || logicalType.equals(LogicalTypes.timestampMicros())) {
          standardSQLTypeName = StandardSQLTypeName.TIMESTAMP;
          // Due to older avro support, we need to use strings for local timestamp logical types
        } else if (logicalType.getName().equals("local-timestamp-millis") || logicalType.getName().equals("local-timestamp-micros")) {
          standardSQLTypeName = StandardSQLTypeName.INT64;
        } else {
          throw new IllegalArgumentException("Unexpected logical type in schema: " + logicalType);
        }
        break;
      case ENUM:
      case STRING:
        standardSQLTypeName = StandardSQLTypeName.STRING;
        break;
      case BOOLEAN:
        standardSQLTypeName = StandardSQLTypeName.BOOL;
        break;
      case DOUBLE:
      case FLOAT:
        standardSQLTypeName = StandardSQLTypeName.FLOAT64;
        break;
      case BYTES:
      case FIXED:
        LogicalType bytesLogicalType = fieldSchema.getLogicalType();
        if (bytesLogicalType == null) {
          standardSQLTypeName = StandardSQLTypeName.BYTES;
        } else if (bytesLogicalType instanceof LogicalTypes.Decimal) {
          standardSQLTypeName = StandardSQLTypeName.NUMERIC;
        } else {
          throw new IllegalArgumentException("Unexpected logical type in schema: " + bytesLogicalType);
        }
        break;
      case RECORD:
        return Field.newBuilder(name, StandardSQLTypeName.STRUCT,
            FieldList.of(getFields(fieldSchema))).setMode(fieldMode).build();
      case ARRAY:
        Field arrayField = getField(fieldSchema.getElementType(), "array", true);
        return Field.newBuilder(name, arrayField.getType(), arrayField.getSubFields()).setMode(Field.Mode.REPEATED).build();
      case MAP:
        Field keyField = Field.newBuilder("key", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build();
        Field valueField = getField(fieldSchema.getValueType(), "value", false);
        Field keyValueField = Field.newBuilder("key_value", StandardSQLTypeName.STRUCT, keyField, valueField).setMode(Field.Mode.REPEATED).build();
        return Field.newBuilder(name, StandardSQLTypeName.STRUCT, keyValueField).setMode(Field.Mode.NULLABLE).build();
      case UNION:
        List<org.apache.avro.Schema> subTypes = fieldSchema.getTypes();
        validateUnion(subTypes);
        org.apache.avro.Schema fieldSchemaFromUnion = subTypes.get(0).getType() == org.apache.avro.Schema.Type.NULL ? subTypes.get(1) : subTypes.get(0);
        nullable = true;
        return getField(fieldSchemaFromUnion, name, nullable);
      default:
        throw new RuntimeException("Unexpected field type: " + fieldSchema.getType());
    }
    return Field.newBuilder(name, standardSQLTypeName).setMode(fieldMode).build();
  }

  private List<Field> getFields(org.apache.avro.Schema schema) {
    return schema.getFields().stream().map(field -> {
      final org.apache.avro.Schema fieldSchema;
      final boolean nullable;
      if (field.schema().getType() == org.apache.avro.Schema.Type.UNION) {
        List<org.apache.avro.Schema> subTypes = field.schema().getTypes();
        validateUnion(subTypes);
        fieldSchema = subTypes.get(0).getType() == org.apache.avro.Schema.Type.NULL ? subTypes.get(1) : subTypes.get(0);
        nullable = true;
      } else {
        fieldSchema = field.schema();
        nullable = false;
      }
      return getField(fieldSchema, field.name(), nullable);
    }).collect(Collectors.toList());
  }

  private void validateUnion(List<org.apache.avro.Schema> subTypes) {
    if (subTypes.size() != 2 || (subTypes.get(0).getType() != org.apache.avro.Schema.Type.NULL
        && subTypes.get(1).getType() != org.apache.avro.Schema.Type.NULL)) {
      throw new HoodieException("Only unions of a single type and null are currently supported");
    }
  }
}
