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

package org.apache.hudi.common.schema;

import org.apache.hudi.common.avro.AvroSchemaUtils;
import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.internal.HoodieSchemaException;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * HoodieSchema-typed structural transforms of table schemas and of the well-known Hudi record shapes.
 *
 * <p>What lives here:</p>
 * <ul>
 *   <li>metadata fields: {@link #addMetadataFields(HoodieSchema, boolean)},
 *       {@link #removeMetadataFields(HoodieSchema)}, {@link #createHoodieWriteSchema(String, boolean)},
 *       {@link #isMetadataField(String)}</li>
 *   <li>record-key and delete-log schemas: {@link #getRecordKeySchema()},
 *       {@link #getRecordKeyPartitionPathSchema()}, {@link #createDeleteLogSchema(HoodieSchema, List)}</li>
 *   <li>projection and pruning: {@link #generateProjectionSchema(HoodieSchema, List)},
 *       {@link #projectSchema(HoodieSchema, List)}, {@link #pruneDataSchema(HoodieSchema, HoodieSchema, Set)},
 *       {@link #removeFields(HoodieSchema, Set)}</li>
 *   <li>appending and merging fields: {@link #appendFieldsToSchema(HoodieSchema, List)},
 *       {@link #appendFieldsToSchemaDedupNested(HoodieSchema, List)},
 *       {@link #mergeSchemas(HoodieSchema, HoodieSchema)},
 *       {@link #createNewSchemaFromFieldsWithReference(HoodieSchema, List)}</li>
 *   <li>field copies and defaults: {@link #createNewSchemaField(HoodieSchemaField)} (the other
 *       {@code createNewSchemaField} overloads are validated aliases of {@code HoodieSchemaField.of}),
 *       {@link #toJavaDefaultValue(HoodieSchemaField)}</li>
 *   <li>nullability: {@link #asNullable(HoodieSchema)}</li>
 *   <li>naming: {@link #sanitizeName(String)}, {@link #getRecordQualifiedName(String)}</li>
 *   <li>lookups and predicates that need more than {@link HoodieSchema} offers on its own:
 *       {@link #findNestedField(HoodieSchema, String)}, {@link #findMissingFields(HoodieSchema, HoodieSchema)},
 *       {@link #resolveUnionSchema(HoodieSchema, String)}, {@link #hasDecimalField(HoodieSchema)}</li>
 * </ul>
 *
 * <p>Not here:</p>
 * <ul>
 *   <li>value and record operations: {@link org.apache.hudi.common.avro.HoodieAvroUtils}</li>
 *   <li>reader/writer compatibility checks: {@link HoodieSchemaCompatibility}</li>
 *   <li>questions about a single schema: instance methods on {@link HoodieSchema}.
 *       {@link #getFieldSchema(HoodieSchema, String)} and {@link #getNestedField(HoodieSchema, String)} are
 *       argument-checking facades over {@link HoodieSchema#getField(String)} and
 *       {@link HoodieSchema#getNestedField(String)}, not lookups of their own</li>
 *   <li>the field-id InternalSchema (schema-on-read) domain: {@code org.apache.hudi.common.schema.internal}</li>
 * </ul>
 *
 * <p>A few methods here still delegate to Avro-typed implementations
 * ({@link #asNullable(HoodieSchema)}, {@link #createNullableSchema(HoodieSchema)},
 * {@link #projectSchema(HoodieSchema, List)} and the 5-arg
 * {@link #createNewSchemaField(String, HoodieSchema, String, Object, HoodieFieldOrder)}). Those delegations
 * are being retired under #16639; new methods must be implemented on HoodieSchema directly.</p>
 *
 * @since 1.2.0
 */
public final class HoodieSchemaUtils {

  // As per https://avro.apache.org/docs/current/spec.html#names
  private static final Pattern INVALID_AVRO_CHARS_IN_NAMES_PATTERN = Pattern.compile("[^A-Za-z0-9_]");
  private static final Pattern INVALID_AVRO_FIRST_CHAR_IN_NAMES_PATTERN = Pattern.compile("[^A-Za-z_]");
  private static final String MASK_FOR_INVALID_CHARS_IN_NAMES = "__";

  public static final HoodieSchema METADATA_FIELD_SCHEMA = HoodieSchema.createNullable(HoodieSchemaType.STRING);
  public static final HoodieSchema RECORD_KEY_SCHEMA = initRecordKeySchema();

  // Private constructor to prevent instantiation
  private HoodieSchemaUtils() {
    throw new UnsupportedOperationException("Utility class cannot be instantiated");
  }

  /**
   * Returns the schema for the specified field.
   *
   * @param schema    record schema that contains the field
   * @param fieldName field name to resolve
   * @return schema of the resolved field
   * @throws HoodieSchemaException if the field does not exist in the schema
   */
  public static HoodieSchema getFieldSchema(HoodieSchema schema, String fieldName) {
    return schema.getNonNullType().getField(fieldName).map(HoodieSchemaField::schema)
        .orElseThrow(() -> new HoodieSchemaException("Field " + fieldName + " doesn't exist in schema: " + schema));
  }

  /**
   * Creates a write schema for Hudi operations, adding necessary metadata fields.
   *
   * @param schema             the base schema string (JSON format)
   * @param withOperationField whether to include operation metadata field
   * @return HoodieSchema configured for write operations
   * @throws IllegalArgumentException if schema is null or empty
   */
  public static HoodieSchema createHoodieWriteSchema(String schema, boolean withOperationField) {
    ValidationUtils.checkArgument(schema != null && !schema.trim().isEmpty(),
        "Schema string cannot be null or empty");

    return addMetadataFields(HoodieSchema.parse(schema), withOperationField);
  }

  /**
   * Adds Hudi metadata fields to the given schema with the withOperationField flag set as false.
   *
   * @param schema             the input schema
   * @return new HoodieSchema with metadata fields added
   * @throws IllegalArgumentException if schema is null
   */
  public static HoodieSchema addMetadataFields(HoodieSchema schema) {
    return addMetadataFields(schema, false);
  }

  /**
   * Prepends the Hudi metadata columns ({@code _hoodie_commit_time}, {@code _hoodie_commit_seqno},
   * {@code _hoodie_record_key}, {@code _hoodie_partition_path}, {@code _hoodie_file_name}) to the given
   * schema; {@code withOperationField} additionally adds {@code _hoodie_operation}. Metadata columns
   * already present on the input schema are dropped rather than duplicated.
   *
   * @param schema             the input schema
   * @param withOperationField whether to include operation metadata field
   * @return new HoodieSchema with metadata fields added
   * @throws IllegalArgumentException if schema is null
   */
  public static HoodieSchema addMetadataFields(HoodieSchema schema, boolean withOperationField) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");

    if (schema.getType() == HoodieSchemaType.NULL) {
      return schema;
    }
    int newFieldsSize = HoodieRecord.HOODIE_META_COLUMNS.size() + (withOperationField ? 1 : 0);
    List<HoodieSchemaField> parentFields = new ArrayList<>(schema.getFields().size() + newFieldsSize);

    HoodieSchemaField commitTimeField =
        HoodieSchemaField.of(HoodieRecord.COMMIT_TIME_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", HoodieSchema.NULL_VALUE);
    HoodieSchemaField commitSeqnoField =
        HoodieSchemaField.of(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", HoodieSchema.NULL_VALUE);
    HoodieSchemaField recordKeyField =
        HoodieSchemaField.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", HoodieSchema.NULL_VALUE);
    HoodieSchemaField partitionPathField =
        HoodieSchemaField.of(HoodieRecord.PARTITION_PATH_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", HoodieSchema.NULL_VALUE);
    HoodieSchemaField fileNameField =
        HoodieSchemaField.of(HoodieRecord.FILENAME_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", HoodieSchema.NULL_VALUE);

    parentFields.add(commitTimeField);
    parentFields.add(commitSeqnoField);
    parentFields.add(recordKeyField);
    parentFields.add(partitionPathField);
    parentFields.add(fileNameField);

    if (withOperationField) {
      final HoodieSchemaField operationField =
          HoodieSchemaField.of(HoodieRecord.OPERATION_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", HoodieSchema.NULL_VALUE);
      parentFields.add(operationField);
    }

    for (HoodieSchemaField field : schema.getFields()) {
      if (!HoodieSchemaUtils.isMetadataField(field.name())) {
        HoodieSchemaField newField = createNewSchemaField(field);
        for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
          newField.addProp(prop.getKey(), prop.getValue());
        }
        parentFields.add(newField);
      }
    }
    return createNewSchemaFromFieldsWithReference(schema, parentFields);
  }

  /**
   * Removes the Hudi metadata columns, including {@code _hoodie_operation}, from the given schema.
   *
   * @param schema the input schema with metadata fields
   * @return new HoodieSchema with metadata fields removed
   * @throws IllegalArgumentException if schema is null
   */
  public static HoodieSchema removeMetadataFields(HoodieSchema schema) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");

    if (schema.getType() == HoodieSchemaType.NULL) {
      return schema;
    }
    return removeFields(schema, HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION);
  }

  /**
   * Merges two schemas, combining fields from both with conflict resolution.
   *
   * <p>This is a plain recursive union of the two field lists: source field order is preserved and
   * target-only fields are appended. There is no type promotion and no nullability reconciliation. For
   * schema-evolution reconciliation use {@code AvroSchemaEvolutionUtils#reconcileSchema} /
   * {@code AvroSchemaEvolutionUtils#reconcileSchemaRequirements}.</p>
   *
   * @param sourceSchema source schema to merge from
   * @param targetSchema target schema to merge into
   * @return new HoodieSchema representing the merged result
   * @throws IllegalArgumentException if either schema is null
   */
  public static HoodieSchema mergeSchemas(HoodieSchema sourceSchema, HoodieSchema targetSchema) {
    ValidationUtils.checkArgument(sourceSchema != null, "Source schema cannot be null");
    ValidationUtils.checkArgument(targetSchema != null, "Target schema cannot be null");

    if (sourceSchema.getType() != HoodieSchemaType.RECORD) {
      return sourceSchema;
    }
    List<HoodieSchemaField> fields = new ArrayList<>();
    for (HoodieSchemaField f : sourceSchema.getFields()) {
      Option<HoodieSchemaField> foundField = targetSchema.getField(f.name());
      fields.add(createNewSchemaField(f.name(), foundField.map(field -> mergeSchemas(f.schema(), field.schema())).orElse(f.schema()),
          f.doc().orElse(null), f.defaultVal().orElse(null)));
    }
    for (HoodieSchemaField f : targetSchema.getFields()) {
      if (sourceSchema.getField(f.name()).isEmpty()) {
        fields.add(createNewSchemaField(f));
      }
    }
    return createNewSchemaFromFieldsWithReference(sourceSchema, fields);
  }

  /**
   * Creates a nullable version of the given schema (union of null and the schema).
   *
   * <p>{@link HoodieSchema#createNullable(HoodieSchema)} is the idempotent native equivalent and is
   * preferred; this overload round-trips through Avro and is retained only for existing call sites.</p>
   *
   * @param schema the input schema
   * @return new HoodieSchema that allows null values
   * @throws IllegalArgumentException if schema is null
   */
  public static HoodieSchema createNullableSchema(HoodieSchema schema) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");

    // Delegate to AvroSchemaUtils
    Schema nullableAvro = AvroSchemaUtils.createNullableSchema(schema.toAvroSchema());
    return HoodieSchema.fromAvroSchema(nullableAvro);
  }

  /**
   * Create a new schema by force changing all the fields as nullable.
   *
   * @return a new schema with all the fields updated as nullable
   * @throws IllegalArgumentException if schema is null
   * @see AvroSchemaUtils#asNullable(Schema)
   */
  public static HoodieSchema asNullable(HoodieSchema schema) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");

    // Delegate to AvroSchemaUtils
    Schema nullableAvro = AvroSchemaUtils.asNullable(schema.toAvroSchema());
    return HoodieSchema.fromAvroSchema(nullableAvro);
  }

  /**
   * Removes the named top-level fields from a RECORD schema, preserving the record's name, namespace,
   * error flag and custom properties. Returns the input schema unchanged when no field matches.
   *
   * @param schema original schema (must be RECORD type)
   * @param fieldNamesToRemove set of field names to remove
   * @return new HoodieSchema without the specified fields
   * @throws IllegalArgumentException if schema is null or not a RECORD type
   */
  public static HoodieSchema removeFields(HoodieSchema schema, Set<String> fieldNamesToRemove) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");
    ValidationUtils.checkArgument(schema.getType() == HoodieSchemaType.RECORD,
        () -> "Only RECORD schemas can have fields removed, got: " + schema.getType());

    if (fieldNamesToRemove == null || fieldNamesToRemove.isEmpty()) {
      return schema;
    }

    // Filter and copy fields (must create new instances, can't reuse Avro fields)
    List<HoodieSchemaField> filteredFields = schema.getFields().stream()
        .filter(field -> !fieldNamesToRemove.contains(field.name()))
        .map(HoodieSchemaUtils::createNewSchemaField)
        .collect(Collectors.toList());

    if (filteredFields.size() == schema.getFields().size()) {
      return schema; // No fields were removed
    }

    // Create record with isError flag preserved
    HoodieSchema newSchema = HoodieSchema.createRecord(
        schema.getName(),
        schema.getDoc().orElse(null),
        schema.getNamespace().orElse(null),
        schema.isError(),
        filteredFields
    );

    // Copy custom properties
    Map<String, Object> props = schema.getObjectProps();
    for (Map.Entry<String, Object> prop : props.entrySet()) {
      newSchema.addProp(prop.getKey(), prop.getValue());
    }

    return newSchema;
  }

  /**
   * Finds the top-level fields that are present in the table schema but missing from the writer schema.
   * Nested records are not descended into; {@code HoodieSchemaCompatibility#checkValidEvolution} is the
   * one that finds missing fields recursively.
   *
   * @param tableSchema  the complete table schema
   * @param writerSchema the writer schema to check against
   * @return list of HoodieSchemaFields that are missing in writer schema
   * @throws IllegalArgumentException if either schema is null
   * @see HoodieSchemaCompatibility#checkValidEvolution(HoodieSchema, HoodieSchema)
   */
  public static List<HoodieSchemaField> findMissingFields(HoodieSchema tableSchema, HoodieSchema writerSchema) {
    return findMissingFields(tableSchema, writerSchema, Collections.emptySet());
  }

  /**
   * Finds the top-level fields that are present in the table schema but missing from the writer schema,
   * skipping the excluded column names (typically the partition columns). Nested records are not descended
   * into; {@code HoodieSchemaCompatibility#checkValidEvolution} is the one that finds missing fields
   * recursively.
   *
   * @param tableSchema    the complete table schema
   * @param writerSchema   the writer schema to check against
   * @param excludeColumns column names to exclude from missing field check
   * @return list of HoodieSchemaFields that are missing in writer schema
   * @throws IllegalArgumentException if either schema is null
   * @see HoodieSchemaCompatibility#checkValidEvolution(HoodieSchema, HoodieSchema)
   */
  public static List<HoodieSchemaField> findMissingFields(HoodieSchema tableSchema, HoodieSchema writerSchema,
                                                          Set<String> excludeColumns) {
    ValidationUtils.checkArgument(tableSchema != null, "Table schema cannot be null");
    ValidationUtils.checkArgument(writerSchema != null, "Writer schema cannot be null");

    if (tableSchema.getType() != HoodieSchemaType.RECORD || writerSchema.getType() != HoodieSchemaType.RECORD) {
      return Collections.emptyList();
    }

    Set<String> exclusions = excludeColumns != null ? excludeColumns : Collections.emptySet();
    Set<String> writerFieldNames = writerSchema.getFields().stream()
        .map(HoodieSchemaField::name)
        .collect(Collectors.toSet());

    // Find fields in table schema that are not present in writer schema and not excluded
    return tableSchema.getFields().stream()
        .filter(field -> !exclusions.contains(field.name()))
        .filter(field -> !writerFieldNames.contains(field.name()))
        .collect(Collectors.toList());
  }

  /**
   * Alias of {@link HoodieSchemaField#of(String, HoodieSchema, String, Object)} with argument validation.
   * Prefer {@code HoodieSchemaField.of} directly in new code.
   *
   * @param name         field name
   * @param schema       field schema
   * @param doc          field documentation (can be null)
   * @param defaultValue default value (can be null)
   * @return new HoodieSchemaField instance
   * @throws IllegalArgumentException if name or schema is null/empty
   */
  public static HoodieSchemaField createNewSchemaField(String name, HoodieSchema schema,
                                                       String doc, Object defaultValue) {
    ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Field name cannot be null or empty");
    ValidationUtils.checkArgument(schema != null, "Field schema cannot be null");

    return HoodieSchemaField.of(name, schema, doc, defaultValue);
  }

  /**
   * Alias of {@link HoodieSchemaField#of(String, HoodieSchema, String, Object, HoodieFieldOrder)} with
   * argument validation. Prefer {@code HoodieSchemaField.of} directly in new code; this overload still
   * round-trips through the Avro-typed {@code HoodieAvroUtils#createNewSchemaField} and is being retired
   * under #16639.
   *
   * @param name         field name
   * @param schema       field schema
   * @param doc          field documentation (can be null)
   * @param defaultValue default value (can be null)
   * @param order        field order for sorting
   * @return new HoodieSchemaField instance
   * @throws IllegalArgumentException if name, schema, or order is null/empty
   * @since 1.2.0
   */
  public static HoodieSchemaField createNewSchemaField(String name, HoodieSchema schema,
                                                       String doc, Object defaultValue, HoodieFieldOrder order) {
    ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Field name cannot be null or empty");
    ValidationUtils.checkArgument(schema != null, "Field schema cannot be null");
    ValidationUtils.checkArgument(order != null, "Field order cannot be null");

    // Delegate to HoodieAvroUtils
    Schema.Field avroField = HoodieAvroUtils.createNewSchemaField(
        name, schema.toAvroSchema(), doc, defaultValue, order.toAvroOrder());
    return HoodieSchemaField.fromAvroField(avroField);
  }

  /**
   * Copy factory: returns a new field carrying the same name, schema, doc and default value as
   * {@code field}. It exists because the backing Avro field cannot be shared between two records, so a
   * field taken off one schema has to be copied before being placed on another. When building a field from
   * scratch prefer {@link HoodieSchemaField#of(String, HoodieSchema, String, Object)}.
   *
   * @param field the original HoodieSchemaField to create a new field from
   * @return a new HoodieSchemaField with the same properties but properly formatted default value
   */
  public static HoodieSchemaField createNewSchemaField(HoodieSchemaField field) {
    return createNewSchemaField(field.name(), field.schema(), field.doc().orElse(null), field.defaultVal().orElse(null));
  }

  /**
   * Gets a field (including nested fields) from the schema using dot notation.
   * This method is a null-checking facade over {@link HoodieSchema#getNestedField(String)}: it returns the
   * leaf field itself, paired with its canonical dotted path.
   * <p>
   * Not to be confused with {@link #findNestedField(HoodieSchema, String)}, which returns a synthesized
   * lineage sub-schema rather than the leaf, and which does not understand {@code list.element} /
   * {@code key_value} path segments.
   * </p>
   * <p>
   * Supports nested field access using dot notation. For example:
   * <ul>
   *   <li>"name" - retrieves top-level field</li>
   *   <li>"user.profile.displayName" - retrieves nested field</li>
   *   <li>"items.list.element" - retrieves array element schema </li>
   *   <li>"metadata.key_value.key" - retrieves map key schema</li>
   *   <li>"metadata.key_value.value" - retrieves map value schema</li>
   * </ul>
   *
   * @param schema    the schema to search in
   * @param fieldName the field name (may contain dots for nested fields)
   * @return Option containing Pair of canonical field name and the HoodieSchemaField, or Option.empty() if field not found
   * @throws IllegalArgumentException if schema or fieldName is null/empty
   * @since 1.2.0
   */
  public static Option<Pair<String, HoodieSchemaField>> getNestedField(HoodieSchema schema, String fieldName) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");
    ValidationUtils.checkArgument(fieldName != null && !fieldName.isEmpty(), "Field name cannot be null or empty");
    return schema.getNestedField(fieldName);
  }

  /**
   * Generates a projection schema from the original schema, including only the specified fields.
   *
   * <p>Field names are matched case-insensitively and the projected field keeps the schema's original casing:
   * Avro field names are case-sensitive while Hive lowercases column projections before they reach the reader
   * (see {@code HoodieRealtimeRecordReaderUtils#generateProjectionSchema}), so both sides are lowercased with
   * {@code Locale.ROOT} for the lookup. The default locale would map an upper-case I to dotless-i under a Turkish
   * or Azeri locale and break the match with {@code HiveHoodieReaderContext}, which pre-lowercases with
   * {@code Locale.ROOT}. A schema with two fields that differ only in case cannot be projected and fails on the
   * duplicate lowercase key.</p>
   *
   * @param originalSchema the source schema
   * @param fieldNames     the list of field names to include in the projection
   * @return new HoodieSchema containing only the specified fields
   * @throws IllegalArgumentException if schema is null or not a record type
   * @since 1.2.0
   */
  public static HoodieSchema generateProjectionSchema(HoodieSchema originalSchema, List<String> fieldNames) {
    ValidationUtils.checkArgument(originalSchema != null, "Original schema cannot be null");
    ValidationUtils.checkArgument(fieldNames != null, "Field names cannot be null");

    Map<String, HoodieSchemaField> schemaFieldsMap = originalSchema.getFields().stream()
        .map(r -> Pair.of(r.name().toLowerCase(Locale.ROOT), r))
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    List<HoodieSchemaField> projectedFields = new ArrayList<>(fieldNames.size());
    for (String fn : fieldNames) {
      HoodieSchemaField field = schemaFieldsMap.get(fn.toLowerCase(Locale.ROOT));
      if (field == null) {
        throw new HoodieException("Field " + fn + " not found in log schema. Query cannot proceed! "
            + "Derived Schema Fields: " + new ArrayList<>(schemaFieldsMap.keySet()));
      } else {
        projectedFields.add(createNewSchemaField(field));
      }
    }

    return HoodieSchema.createRecord(originalSchema.getName(), originalSchema.getNamespace().orElse(null), originalSchema.getDoc().orElse(null), projectedFields);
  }

  /**
   * Prunes the data schema to only include fields that are required by the required schema,
   * plus any mandatory fields specified.
   *
   * @param dataSchema      the full data schema
   * @param requiredSchema  the schema containing required fields
   * @param mandatoryFields set of field names that must be included regardless
   * @return new HoodieSchema with pruned fields
   * @throws IllegalArgumentException if either schema is null
   * @since 1.2.0
   */
  public static HoodieSchema pruneDataSchema(HoodieSchema dataSchema, HoodieSchema requiredSchema, Set<String> mandatoryFields) {
    ValidationUtils.checkArgument(dataSchema != null, "Data schema cannot be null");
    ValidationUtils.checkArgument(requiredSchema != null, "Required schema cannot be null");

    Set<String> mandatorySet = mandatoryFields != null ? mandatoryFields : Collections.emptySet();

    HoodieSchema prunedDataSchema = pruneDataSchemaInternal(dataSchema.getNonNullType(), requiredSchema.getNonNullType(), mandatorySet);
    if (dataSchema.isNullable() && !prunedDataSchema.isNullable()) {
      return HoodieSchema.createNullable(prunedDataSchema);
    }
    return prunedDataSchema;
  }

  private static HoodieSchema pruneDataSchemaInternal(HoodieSchema dataSchema, HoodieSchema requiredSchema, Set<String> mandatoryFields) {
    switch (requiredSchema.getType()) {
      case RECORD:
        // BLOB and VARIANT are represented as Avro RECORDs but carry a logical type
        // whose validate() contract requires the full canonical field layout
        // ({type,data,reference} and {metadata,value} respectively). Partially pruning
        // their inner fields would drop that contract. Spark's projection still prunes
        // these columns at eval time, so reading the full logical-type record here is
        // correct and cheap.
        if (dataSchema.getType() == HoodieSchemaType.BLOB
            || dataSchema.getType() == HoodieSchemaType.VARIANT) {
          return dataSchema;
        }
        if (dataSchema.getType() != HoodieSchemaType.RECORD) {
          throw new IllegalArgumentException("Data schema is not a record");
        }
        List<HoodieSchemaField> newFields = new ArrayList<>(requiredSchema.getFields().size());
        for (HoodieSchemaField requiredSchemaField : requiredSchema.getFields()) {
          if (mandatoryFields.contains(requiredSchemaField.name())) {
            newFields.add(createNewSchemaField(requiredSchemaField));
          } else {
            dataSchema.getField(requiredSchemaField.name()).ifPresent(dataSchemaField -> {
              HoodieSchema prunedFieldSchema = pruneDataSchema(dataSchemaField.schema(), requiredSchemaField.schema(), Collections.emptySet());
              HoodieSchemaField newField = createNewSchemaField(
                  dataSchemaField.name(),
                  prunedFieldSchema,
                  dataSchemaField.doc().orElse(null),
                  dataSchemaField.defaultVal().orElse(null)
              );
              newFields.add(newField);
            });
          }
        }
        HoodieSchema newRecord = HoodieSchema.createRecord(dataSchema.getName(), dataSchema.getNamespace().orElse(null), dataSchema.getDoc().orElse(null), newFields);
        copyProperties(dataSchema, newRecord);
        return newRecord;

      case ARRAY:
        if (dataSchema.getType() != HoodieSchemaType.ARRAY) {
          throw new IllegalArgumentException("Data schema is not an array");
        }
        return HoodieSchema.createArray(pruneDataSchema(dataSchema.getElementType(), requiredSchema.getElementType(), Collections.emptySet()));

      case MAP:
        if (dataSchema.getType() != HoodieSchemaType.MAP) {
          throw new IllegalArgumentException("Data schema is not a map");
        }
        return HoodieSchema.createMap(pruneDataSchema(dataSchema.getValueType(), requiredSchema.getValueType(), Collections.emptySet()));

      case UNION:
        throw new IllegalArgumentException("Data schema is a union");

      default:
        return dataSchema;
    }
  }

  /**
   * Helper to copy properties and logical types from source schema to target schema.
   */
  private static HoodieSchema copyProperties(HoodieSchema source, HoodieSchema target) {
    for (Map.Entry<String, Object> prop : source.getObjectProps().entrySet()) {
      target.addProp(prop.getKey(), prop.getValue());
    }
    return target;
  }

  /**
   * Adds newFields to the schema. Will add nested fields without duplicating the field
   * For example if your schema is "a.b.{c,e}" and newfields contains "a.{b.{d,e},x.y}",
   * It will stitch them together to be "a.{b.{c,d,e},x.y}
   *
   * @param schema    the original schema
   * @param newFields list of new fields to add
   * @return the updated schema with new fields added
   */
  public static HoodieSchema appendFieldsToSchemaDedupNested(HoodieSchema schema, List<HoodieSchemaField> newFields) {
    return appendFieldsToSchemaBase(schema, newFields, true);
  }

  /**
   * Appends provided new fields at the end of the given schema
   * <p>
   * NOTE: No deduplication is made, this method simply appends fields at the end of the list
   *       of the source schema as is
   * <p>
   */
  public static HoodieSchema appendFieldsToSchema(HoodieSchema schema, List<HoodieSchemaField> newFields) {
    return appendFieldsToSchemaBase(schema, newFields, false);
  }

  /**
   * Create a new schema but maintain all meta info from the old schema.
   *
   * @param schema schema to get the meta info from
   * @param fields list of fields in order that will be in the new schema
   *
   * @return schema with fields from fields, and metadata from schema
   */
  public static HoodieSchema createNewSchemaFromFieldsWithReference(HoodieSchema schema, List<HoodieSchemaField> fields) {
    if (schema == null) {
      throw new IllegalArgumentException("Schema must not be null");
    }
    Map<String, Object> schemaProps = schema.getObjectProps();
    HoodieSchema newSchema = HoodieSchema.createRecord(schema.getName(), schema.getNamespace().orElse(null), schema.getDoc().orElse(null), fields);
    for (Map.Entry<String, Object> prop : schemaProps.entrySet()) {
      newSchema.addProp(prop.getKey(), prop.getValue());
    }
    return newSchema;
  }

  /**
   * Get gets a field from a record, works on nested fields as well (if you provide the whole name, eg: toplevel.nextlevel.child)
   * <p>
   * Returns a synthesized lineage sub-schema, not the leaf field: {@code b.z.z2} comes back as
   * {@code b:record(z:record(z2))}. That shape is what {@link #appendFieldsToSchemaDedupNested} consumes.
   * Only record nesting is understood here - {@code list.element} and {@code key_value} path segments are
   * not. Use {@link #getNestedField(HoodieSchema, String)} or {@link HoodieSchema#getNestedField(String)}
   * when the leaf field and its canonical path are what is wanted.
   * </p>
   * @return the field, including its lineage.
   * For example, if you have a schema: record(a:int, b:record(x:int, y:long, z:record(z1: int, z2: float, z3: double), c:bool)
   * "fieldName" | output
   * ---------------------------------
   * "a"         | a:int
   * "b"         | b:record(x:int, y:long, z:record(z1: int, z2: int, z3: int)
   * "c"         | c:bool
   * "b.x"       | b:record(x:int)
   * "b.z.z2"    | b:record(z:record(z2:float))
   *
   * this is intended to be used with appendFieldsToSchemaDedupNested
   */
  public static Option<HoodieSchemaField> findNestedField(HoodieSchema schema, String fieldName) {
    return findNestedField(schema, fieldName.split("\\."), 0);
  }

  private static Option<HoodieSchemaField> findNestedField(HoodieSchema schema, String[] fieldParts, int index) {
    if (schema.getType() == HoodieSchemaType.UNION) {
      Option<HoodieSchemaField> notUnion = findNestedField(schema.getNonNullType(), fieldParts, index);
      if (!notUnion.isPresent()) {
        return Option.empty();
      }
      HoodieSchemaField nu = notUnion.get();
      return Option.of(createNewSchemaField(nu));
    }
    if (fieldParts.length <= index) {
      return Option.empty();
    }

    Option<HoodieSchemaField> foundFieldOpt = schema.getField(fieldParts[index]);
    if (foundFieldOpt.isEmpty()) {
      return Option.empty();
    }
    HoodieSchemaField foundField = foundFieldOpt.get();

    if (index == fieldParts.length - 1) {
      return Option.of(createNewSchemaField(foundField));
    }

    HoodieSchema foundSchema = foundField.schema();
    Option<HoodieSchemaField> nestedPart = findNestedField(foundSchema, fieldParts, index + 1);
    if (!nestedPart.isPresent()) {
      return Option.empty();
    }
    boolean isUnion = false;
    if (foundSchema.getType() == HoodieSchemaType.UNION) {
      isUnion = true;
      foundSchema = foundSchema.getNonNullType();
    }
    HoodieSchema newSchema = createNewSchemaFromFieldsWithReference(foundSchema, Collections.singletonList(nestedPart.get()));
    return Option.of(createNewSchemaField(foundField.name(), isUnion ? HoodieSchema.createNullable(newSchema) : newSchema, foundField.doc().orElse(null), foundField.defaultVal().orElse(null)));
  }

  private static HoodieSchema initRecordKeySchema() {
    HoodieSchemaField recordKeyField =
            createNewSchemaField(HoodieRecord.RECORD_KEY_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    return HoodieSchema.createRecord(
            "HoodieRecordKey",
            "",
            "",
            false,
            Collections.singletonList(recordKeyField)
    );
  }

  public static HoodieSchema getRecordKeySchema() {
    return RECORD_KEY_SCHEMA;
  }

  /**
   * Builds the two-column {@code HoodieRecordKey} record holding {@code _hoodie_record_key} and
   * {@code _hoodie_partition_path}, both nullable strings.
   *
   * @return HoodieSchema containing record key and partition path fields
   * @see #getRecordKeySchema()
   */
  public static HoodieSchema getRecordKeyPartitionPathSchema() {
    List<HoodieSchemaField> toBeAddedFields = new ArrayList<>(2);

    HoodieSchemaField recordKeyField =
        createNewSchemaField(HoodieRecord.RECORD_KEY_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    HoodieSchemaField partitionPathField =
        createNewSchemaField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);

    toBeAddedFields.add(recordKeyField);
    toBeAddedFields.add(partitionPathField);
    return HoodieSchema.createRecord("HoodieRecordKey", "", "", false, toBeAddedFields);
  }

  /**
   * Schema of a native delete log record: the record key plus the ordering fields, which are
   * always nullable (see the comment in the body).
   */
  public static HoodieSchema createDeleteLogSchema(HoodieSchema tableSchema, List<String> orderingFieldNames) {
    // Native delete logs store only the record key plus optional ordering values, so ordering fields in
    // the delete-log schema must always be nullable even when the table schema marks them required.
    // A delete record such as HoodieEmptyRecord may carry OrderingValues.getDefault() as an in-memory
    // sentinel rather than a real field value. Persist NULL for that missing value so readers can map it
    // back to the default ordering without confusing it with a real business value such as 0.
    List<HoodieSchemaField> fields = Stream.concat(
        Stream.of(createNewSchemaField(
            HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieSchema.create(HoodieSchemaType.STRING), null, null)),
        orderingFieldNames.stream().map(orderingFieldName -> tableSchema.getField(orderingFieldName)
            .map(field -> createNewSchemaField(
                field.name(), HoodieSchema.createNullable(field.schema()), field.doc().orElse(null), HoodieSchema.NULL_VALUE))
            .orElseThrow(() ->
                new IllegalArgumentException("Ordering field " + orderingFieldName + " not found in table schema"))))
        .collect(Collectors.toList());
    return HoodieSchema.createRecord("hudi_delete_log_record", null, null, fields);
  }

  /**
   * Fetches projected schema given list of fields to project. The field can be nested in format `a.b.c` where a is
   * the top level field, b is at second level and so on. Field names are matched case-sensitively.
   * This is equivalent to {@link HoodieAvroUtils#projectSchema(Schema, List)} but operates on HoodieSchema.
   *
   * <p>The two sibling projection helpers differ:</p>
   * <ul>
   *   <li>{@link #generateProjectionSchema(HoodieSchema, List)} - top-level fields only, matched
   *       case-insensitively</li>
   *   <li>{@link #pruneDataSchema(HoodieSchema, HoodieSchema, Set)} - prunes to the shape of a required
   *       schema instead of to a list of names</li>
   * </ul>
   *
   * @param fileSchema the original schema
   * @param fields     list of fields to project
   * @return projected schema containing only specified fields
   */
  public static HoodieSchema projectSchema(HoodieSchema fileSchema, List<String> fields) {
    return HoodieSchema.fromAvroSchema(HoodieAvroUtils.projectSchema(fileSchema.toAvroSchema(), fields));
  }

  /**
   * Generates fully-qualified name for the Avro's schema based on the Table's name
   *
   * <p>The qualified name follows the pattern: hoodie.{tableName}.{tableName}_record
   * where tableName is sanitized for Avro compatibility.</p>
   *
   * NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
   *       This method should not change for compatibility reasons as older versions
   *       of Avro might be comparing fully-qualified names rather than just the record
   *       names
   *
   * @param tableName the Hudi table name
   * @return the fully-qualified Avro record name (e.g., "hoodie.my_table.my_table_record")
   * @throws IllegalArgumentException if tableName is null or empty
   * @since 1.2.0
   */
  public static String getRecordQualifiedName(String tableName) {
    ValidationUtils.checkArgument(tableName != null && !tableName.trim().isEmpty(),
        "Table name cannot be null or empty");

    String sanitizedTableName = sanitizeName(tableName);
    return "hoodie." + sanitizedTableName + "." + sanitizedTableName + "_record";
  }

  public static boolean hasDecimalField(HoodieSchema schema) {
    switch (schema.getType()) {
      case RECORD:
        for (HoodieSchemaField field : schema.getFields()) {
          if (hasDecimalField(field.schema())) {
            return true;
          }
        }
        return false;
      case ARRAY:
        return hasDecimalField(schema.getElementType());
      case MAP:
        return hasDecimalField(schema.getValueType());
      case UNION:
        return hasDecimalField(schema.getNonNullType());
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }

  /**
   * Resolves a union schema by finding the schema matching the given full name.
   * Handles both simple nullable unions (null + non-null) and complex unions with multiple types.
   *
   * <p>This method supports the following union types:
   * <ul>
   *   <li>Simple nullable unions: {@code ["null", "Type"]} - returns the non-null type</li>
   *   <li>Complex unions: {@code ["null", "TypeA", "TypeB"]} - returns the type matching fieldSchemaFullName</li>
   *   <li>Non-union schemas - returns the schema as-is</li>
   * </ul>
   *
   * @param schema the schema to resolve (may or may not be a union)
   * @param fieldSchemaFullName the full name of the schema to find within the union
   * @return the resolved schema
   * @throws HoodieSchemaException if the union cannot be resolved or no matching type is found
   */
  public static HoodieSchema resolveUnionSchema(HoodieSchema schema, String fieldSchemaFullName) {
    if (schema.getType() != HoodieSchemaType.UNION) {
      return schema;
    }

    List<HoodieSchema> innerTypes = schema.getTypes();
    if (innerTypes.size() == 2 && schema.isNullable()) {
      // this is a basic nullable field so handle it more efficiently
      return schema.getNonNullType();
    }

    HoodieSchema nonNullType = innerTypes.stream()
        .filter(it -> it.getType() != HoodieSchemaType.NULL && Objects.equals(it.getFullName(), fieldSchemaFullName))
        .findFirst()
        .orElse(null);

    if (nonNullType == null) {
      throw new HoodieSchemaException(
          String.format("Unsupported UNION type %s: Only UNION of a null type and a non-null type is supported", schema));
    }

    return nonNullType;
  }

  @VisibleForTesting
  public static String addMetadataColumnTypes(String hiveColumnTypes) {
    return "string,string,string,string,string," + hiveColumnTypes;
  }

  public static boolean isMetadataField(String fieldName) {
    return HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(fieldName);
  }

  /**
   * Converts a HoodieSchemaField's default value to its Java representation.
   *
   * <p>For primitive types (STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, ENUM, BYTES, FIXED, DECIMAL)
   * and logical types (TIME, TIMESTAMP, DATE, UUID), the default value is returned as-is.
   * For complex types (ARRAY, MAP, RECORD), Avro's GenericData utility is used
   * to properly construct the default value.</p>
   *
   * @param field the HoodieSchemaField containing the default value
   * @return the Java representation of the default value, or null if no default value exists
   * @throws IllegalArgumentException if the field's type is not supported
   * @since 1.2.0
   */
  public static Object toJavaDefaultValue(HoodieSchemaField field) {
    ValidationUtils.checkArgument(field != null, "Field cannot be null");

    Option<Object> defaultValOpt = field.defaultVal();
    if (!defaultValOpt.isPresent() || defaultValOpt.get() == HoodieJsonProperties.NULL_VALUE) {
      return null;
    }

    Object defaultVal = defaultValOpt.get();
    HoodieSchemaType type = field.getNonNullSchema().getType();

    switch (type) {
      case STRING:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case ENUM:
      case BYTES:
      case FIXED:
      case DECIMAL:
      case TIME:
      case TIMESTAMP:
      case DATE:
      case UUID:
        return defaultVal;
      case ARRAY:
      case MAP:
      case RECORD:
        // Use Avro's standard GenericData utility for complex types
        // Delegate to the underlying Avro field
        return GenericData.get().getDefaultValue(field.getAvroField());
      default:
        throw new IllegalArgumentException("Unsupported HoodieSchema type: " + type);
    }
  }

  /**
   * Sanitizes Name according to Avro rule for names.
   * Removes characters other than the ones mentioned in <a href="https://avro.apache.org/docs/current/spec.html#names">avro spec</a> .
   *
   * @param name input name
   * @return sanitized name
   */
  public static String sanitizeName(String name) {
    return sanitizeName(name, MASK_FOR_INVALID_CHARS_IN_NAMES);
  }

  /**
   * Sanitizes Name according to Avro rule for names.
   * Removes characters other than the ones mentioned in <a href="https://avro.apache.org/docs/current/spec.html#names">avro spec</a>.
   *
   * @param name            input name
   * @param invalidCharMask replacement for invalid characters.
   * @return sanitized name
   */
  public static String sanitizeName(String name, String invalidCharMask) {
    if (INVALID_AVRO_FIRST_CHAR_IN_NAMES_PATTERN.matcher(name.substring(0, 1)).matches()) {
      name = INVALID_AVRO_FIRST_CHAR_IN_NAMES_PATTERN.matcher(name).replaceFirst(invalidCharMask);
    }
    return INVALID_AVRO_CHARS_IN_NAMES_PATTERN.matcher(name).replaceAll(invalidCharMask);
  }

  public static String createSchemaErrorString(String errorMessage, HoodieSchema writerSchema, HoodieSchema tableSchema) {
    return String.format("%s\nwriterSchema: %s\ntableSchema: %s", errorMessage, writerSchema, tableSchema);
  }

  private static HoodieSchema appendFieldsToSchemaBase(HoodieSchema schema, List<HoodieSchemaField> newFields, boolean dedupNested) {
    List<HoodieSchemaField> fields = schema.getFields().stream()
        .map(HoodieSchemaUtils::createNewSchemaField)
        .collect(Collectors.toList());
    if (dedupNested) {
      for (HoodieSchemaField f : newFields) {
        Option<HoodieSchemaField> field = schema.getField(f.name());
        if (field.isPresent()) {
          HoodieSchemaField foundField = field.get();
          fields.set(foundField.pos(), createNewSchemaField(foundField.name(), mergeSchemas(foundField.schema(), f.schema()), foundField.doc().orElse(null), foundField.defaultVal().orElse(null)));
        } else {
          fields.add(f);
        }
      }
    } else {
      fields.addAll(newFields);
    }

    return createNewSchemaFromFieldsWithReference(schema, fields);
  }
}
