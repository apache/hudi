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

package org.apache.hudi.avro;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.TableChanges;
import org.apache.hudi.internal.schema.utils.SchemaChangeUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.HoodieAvroUtils.createNewSchemaField;
import static org.apache.hudi.common.util.CollectionUtils.reduce;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.internal.schema.convert.InternalSchemaConverter.convert;

/**
 * Utils for Avro Schema.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class AvroSchemaUtils {

  /**
   * Create a new schema but maintain all meta info from the old schema
   *
   * @param schema schema to get the meta info from
   * @param fields list of fields in order that will be in the new schema
   *
   * @return schema with fields from fields, and metadata from schema
   */
  public static Schema createNewSchemaFromFieldsWithReference(Schema schema, List<Schema.Field> fields) {
    if (schema == null) {
      throw new IllegalArgumentException("Schema must not be null");
    }
    Schema newSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
    Map<String, Object> schemaProps = Collections.emptyMap();
    try {
      schemaProps = schema.getObjectProps();
    } catch (Exception e) {
      log.warn("Error while getting object properties from schema: {}", schema, e);
    }
    for (Map.Entry<String, Object> prop : schemaProps.entrySet()) {
      newSchema.addProp(prop.getKey(), prop.getValue());
    }
    newSchema.setFields(fields);
    return newSchema;
  }

  /**
   * Prunes a data schema to match the structure of a required schema while preserving
   * original metadata where possible.
   *
   * <p>This method recursively traverses both schemas and creates a new schema that:
   * <ul>
   *   <li>Contains only fields present in the required schema</li>
   *   <li>Preserves field metadata (type, documentation, default values) from the data schema</li>
   *   <li>Optionally includes fields from the required schema that are marked for exclusion</li>
   * </ul>
   *
   * @param dataSchema the source schema containing the original data structure and metadata
   * @param requiredSchema the target schema that defines the desired structure and field requirements
   * @param mandatoryFields a set of top level field names that should be included from the required schema
   *                     even if they don't exist in the data schema. This allows for fields like cdc operation
   *                     don't exist in the data schema. We keep the types matching the required schema because
   *                     timestamp partition cols can be read as a different type than the data schema
   *
   * @return a new pruned schema that matches the required schema structure while preserving
   *         data schema metadata where possible
   */
  public static Schema pruneDataSchema(Schema dataSchema, Schema requiredSchema, Set<String> mandatoryFields) {
    Schema prunedDataSchema = pruneDataSchemaInternal(getNonNullTypeFromUnion(dataSchema), getNonNullTypeFromUnion(requiredSchema), mandatoryFields);
    if (dataSchema.isNullable() && !prunedDataSchema.isNullable()) {
      return createNullableSchema(prunedDataSchema);
    }
    return prunedDataSchema;
  }

  private static Schema pruneDataSchemaInternal(Schema dataSchema, Schema requiredSchema, Set<String> mandatoryFields) {
    switch (requiredSchema.getType()) {
      case RECORD:
        if (dataSchema.getType() != Schema.Type.RECORD) {
          throw new IllegalArgumentException("Data schema is not a record");
        }
        List<Schema.Field> newFields = new ArrayList<>();
        for (Schema.Field requiredSchemaField : requiredSchema.getFields()) {
          if (mandatoryFields.contains(requiredSchemaField.name())) {
            newFields.add(createNewSchemaField(requiredSchemaField));
          } else {
            Schema.Field dataSchemaField = dataSchema.getField(requiredSchemaField.name());
            if (dataSchemaField != null) {
              Schema.Field newField = createNewSchemaField(
                  dataSchemaField.name(),
                  pruneDataSchema(dataSchemaField.schema(), requiredSchemaField.schema(), Collections.emptySet()),
                  dataSchemaField.doc(),
                  dataSchemaField.defaultVal()
              );
              newFields.add(newField);
            }
          }
        }
        Schema newRecord = Schema.createRecord(dataSchema.getName(), dataSchema.getDoc(), dataSchema.getNamespace(), dataSchema.isError());
        copyProperties(dataSchema, newRecord);
        newRecord.setFields(newFields);
        return newRecord;

      case ARRAY:
        if (dataSchema.getType() != Schema.Type.ARRAY) {
          throw new IllegalArgumentException("Data schema is not an array");
        }
        return Schema.createArray(pruneDataSchema(dataSchema.getElementType(), requiredSchema.getElementType(), Collections.emptySet()));

      case MAP:
        if (dataSchema.getType() != Schema.Type.MAP) {
          throw new IllegalArgumentException("Data schema is not a map");
        }
        return Schema.createMap(pruneDataSchema(dataSchema.getValueType(), requiredSchema.getValueType(), Collections.emptySet()));

      case UNION:
        throw new IllegalArgumentException("Data schema is a union");

      default:
        return dataSchema;
    }
  }

  /**
   * Returns true in case provided {@link Schema} is nullable (ie accepting null values),
   * returns false otherwise
   */
  public static boolean isNullable(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return false;
    }

    List<Schema> innerTypes = schema.getTypes();
    return innerTypes.size() > 1 && innerTypes.stream().anyMatch(it -> it.getType() == Schema.Type.NULL);
  }

  /**
   * Resolves typical Avro's nullable schema definition: {@code Union(Schema.Type.NULL, <NonNullType>)},
   * decomposing union and returning the target non-null type
   */
  public static Schema getNonNullTypeFromUnion(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return schema;
    }

    List<Schema> innerTypes = schema.getTypes();

    if (innerTypes.size() != 2) {
      throw new HoodieAvroSchemaException(
          String.format("Unsupported Avro UNION type %s: Only UNION of a null type and a non-null type is supported", schema));
    }
    Schema firstInnerType = innerTypes.get(0);
    Schema secondInnerType = innerTypes.get(1);
    if ((firstInnerType.getType() != Schema.Type.NULL && secondInnerType.getType() != Schema.Type.NULL)
        || (firstInnerType.getType() == Schema.Type.NULL && secondInnerType.getType() == Schema.Type.NULL)) {
      throw new HoodieAvroSchemaException(
          String.format("Unsupported Avro UNION type %s: Only UNION of a null type and a non-null type is supported", schema));
    }
    return firstInnerType.getType() == Schema.Type.NULL ? secondInnerType : firstInnerType;
  }

  /**
   * Creates schema following Avro's typical nullable schema definition: {@code Union(Schema.Type.NULL, <NonNullType>)},
   * wrapping around provided target non-null type
   */
  public static Schema createNullableSchema(Schema.Type avroType) {
    return createNullableSchema(Schema.create(avroType));
  }

  public static Schema createNullableSchema(Schema schema) {
    checkState(schema.getType() != Schema.Type.NULL);
    return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
  }

  public static String createSchemaErrorString(String errorMessage, Schema writerSchema, Schema tableSchema) {
    return String.format("%s\nwriterSchema: %s\ntableSchema: %s", errorMessage, writerSchema, tableSchema);
  }

  /**
   * Create a new schema by force changing all the fields as nullable.
   *
   * @param schema original schema
   * @return a new schema with all the fields updated as nullable.
   */
  public static Schema asNullable(Schema schema) {
    List<String> filterCols = schema.getFields().stream()
            .filter(f -> !f.schema().isNullable()).map(Schema.Field::name).collect(Collectors.toList());
    if (filterCols.isEmpty()) {
      return schema;
    }
    InternalSchema internalSchema = convert(HoodieSchema.fromAvroSchema(schema));
    TableChanges.ColumnUpdateChange schemaChange = TableChanges.ColumnUpdateChange.get(internalSchema);
    schemaChange = reduce(filterCols, schemaChange,
            (change, field) -> change.updateColumnNullability(field, true));
    return convert(SchemaChangeUtils.applyTableChanges2Schema(internalSchema, schemaChange), schema.getFullName()).toAvroSchema();
  }

  /**
   * Helper to copy properties and logical types from source schema to target schema.
   */
  private static Schema copyProperties(Schema source, Schema target) {
    for (Map.Entry<String, Object> prop : source.getObjectProps().entrySet()) {
      target.addProp(prop.getKey(), prop.getValue());
    }
    if (source.getLogicalType() != null) {
      source.getLogicalType().addToSchema(target);
    }
    return target;
  }
}
