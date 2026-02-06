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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
}
