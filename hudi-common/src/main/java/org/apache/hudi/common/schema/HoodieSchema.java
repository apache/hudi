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

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.HoodieSchemaException;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Wrapper class for Avro Schema that provides Hudi-specific schema functionality
 * while maintaining binary compatibility with Avro.
 *
 * <p>This class serves as the primary interface for schema operations within Hudi.
 * It encapsulates an Avro Schema and provides a consistent, type-safe API while
 * maintaining full compatibility with existing Avro-based code.</p>
 *
 * <p>Key features:
 * <ul>
 *   <li>Binary compatibility with Avro Schema</li>
 *   <li>Type-safe field access through HoodieSchemaField</li>
 *   <li>Support for all Avro schema types and operations</li>
 *   <li>Consistent error handling using Hudi exceptions</li>
 *   <li>Integration with Hudi's Option type for null safety</li>
 * </ul></p>
 *
 * <p>Usage examples:
 * <pre>{@code
 * // Create from JSON
 * HoodieSchema schema = HoodieSchema.parse(jsonSchemaString);
 *
 * // Create primitive schemas
 * HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
 *
 * // Access schema properties
 * HoodieSchemaType type = schema.getType();
 * List<HoodieSchemaField> fields = schema.getFields();
 * Option<HoodieSchemaField> field = schema.getField("fieldName");
 *
 * // Convert back to Avro for compatibility
 * Schema avroSchema = schema.getAvroSchema();
 * }</pre></p>
 *
 * @since 1.2.0
 */
public class HoodieSchema implements Serializable {
  private static final long serialVersionUID = 1L;
  /**
   * Constant representing a null JSON value, equivalent to JsonProperties.NULL_VALUE.
   * This provides compatibility with Avro's JsonProperties while maintaining Hudi's API.
   */
  public static final Object NULL_VALUE = JsonProperties.NULL_VALUE;
  public static final HoodieSchema NULL_SCHEMA = HoodieSchema.create(HoodieSchemaType.NULL);

  /**
   * Constant to use when attaching type metadata to external schema systems like Spark's StructType.
   */
  public static final String TYPE_METADATA_FIELD = "hudi_type";

  /**
   * Constants for Parquet-style accessor patterns used in nested MAP and ARRAY navigation.
   * These patterns are specifically used for column stats generation and differ from
   * InternalSchema constants which are used in schema evolution contexts.
   */
  private static final String ARRAY_LIST = "list";
  private static final String ARRAY_ELEMENT = "element";
  private static final String MAP_KEY_VALUE = "key_value";
  private static final String MAP_KEY = "key";
  private static final String MAP_VALUE = "value";

  private static final String ARRAY_LIST_ELEMENT = ARRAY_LIST + "." + ARRAY_ELEMENT;
  private static final String MAP_KEY_VALUE_KEY = MAP_KEY_VALUE + "." + MAP_KEY;
  private static final String MAP_KEY_VALUE_VALUE = MAP_KEY_VALUE + "." + MAP_VALUE;

  public static final String PARQUET_ARRAY_SPARK = ".array";
  public static final String PARQUET_ARRAY_AVRO = "." + ARRAY_LIST_ELEMENT;

  private Schema avroSchema;
  private HoodieSchemaType type;
  private transient List<HoodieSchemaField> fields;
  private transient Map<String, HoodieSchemaField> fieldMap;

  // Register the Variant logical type with Avro
  static {
    LogicalTypes.register(VariantLogicalType.VARIANT_LOGICAL_TYPE_NAME, new VariantLogicalTypeFactory());
    LogicalTypes.register(BlobLogicalType.BLOB_LOGICAL_TYPE_NAME, new BlogLogicalTypeFactory());
  }

  /**
   * Creates a new HoodieSchema wrapping the given Avro schema.
   *
   * @param avroSchema the Avro schema to wrap, cannot be null
   * @throws IllegalArgumentException if avroSchema is null
   */
  private HoodieSchema(Schema avroSchema) {
    this(avroSchema, null);
  }

  /**
   * Creates a new HoodieSchema with the given Avro schema and fields.
   * @param avroSchema the Avro schema to wrap, cannot be null
   * @param fields the list of HoodieSchemaField objects, can be null
   * @throws IllegalArgumentException if avroSchema is null
   */
  private HoodieSchema(Schema avroSchema, List<HoodieSchemaField> fields) {
    ValidationUtils.checkArgument(avroSchema != null, "Avro schema cannot be null");
    this.avroSchema = avroSchema;
    Schema.Type avroType = avroSchema.getType();
    ValidationUtils.checkState(avroType != null, "Avro schema type cannot be null");
    this.type = HoodieSchemaType.fromAvro(avroSchema);
    this.fields = fields != null ? Collections.unmodifiableList(fields) : null;
  }

  /**
   * Factory method to create HoodieSchema from an Avro schema.
   *
   * @param avroSchema the Avro schema to wrap
   * @return new HoodieSchema instance
   */
  public static HoodieSchema fromAvroSchema(Schema avroSchema) {
    if (avroSchema == null) {
      return null;
    }
    HoodieSchema schema;
    LogicalType logicalType = avroSchema.getLogicalType();
    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Decimal) {
        schema = new HoodieSchema.Decimal(avroSchema);
      } else if (logicalType instanceof LogicalTypes.TimeMillis || logicalType instanceof LogicalTypes.TimeMicros) {
        schema = new HoodieSchema.Time(avroSchema);
      } else if (logicalType instanceof LogicalTypes.TimestampMillis || logicalType instanceof LogicalTypes.TimestampMicros
          || logicalType instanceof LogicalTypes.LocalTimestampMillis || logicalType instanceof LogicalTypes.LocalTimestampMicros) {
        schema = new HoodieSchema.Timestamp(avroSchema);
      } else if (logicalType == VariantLogicalType.variant()) {
        schema = new HoodieSchema.Variant(avroSchema);
      } else if (logicalType == BlobLogicalType.blob()) {
        schema = new HoodieSchema.Blob(avroSchema);
      } else {
        schema = new HoodieSchema(avroSchema);
      }
    } else {
      schema = new HoodieSchema(avroSchema);
    }
    validateNoBlobsInArrayOrMap(schema);
    return schema;
  }

  /**
   * Parses a JSON schema string and returns the corresponding HoodieSchema.
   *
   * @param jsonSchema the JSON schema string to parse
   * @return parsed HoodieSchema
   * @throws HoodieAvroSchemaException if the schema string is invalid
   */
  public static HoodieSchema parse(String jsonSchema) {
    return new HoodieSchema.Parser().parse(jsonSchema);
  }

  /**
   * Parses a JSON schema string and returns the corresponding HoodieSchema allowing validateDefaults to be configured
   */
  public static HoodieSchema parse(String jsonSchema, boolean validateDefaults) {
    return new HoodieSchema.Parser(validateDefaults).parse(jsonSchema);
  }

  /**
   * Parses an InputStream and returns the corresponding HoodieSchema.
   *
   * @param inputStream the InputStream to parse
   * @return parsed HoodieSchema
   * @throws HoodieAvroSchemaException if the schema string is invalid
   */
  public static HoodieSchema parse(InputStream inputStream) {
    return new HoodieSchema.Parser().parse(inputStream);
  }

  /**
   * Creates a schema for the specified primitive type.
   *
   * @param type the primitive schema type
   * @return new HoodieSchema for the primitive type
   * @throws IllegalArgumentException if type is not a primitive type
   */
  public static HoodieSchema create(HoodieSchemaType type) {
    ValidationUtils.checkArgument(type != null, "Schema type cannot be null");
    ValidationUtils.checkArgument(type.isPrimitive(), () -> "Only primitive types are supported: " + type);
    ValidationUtils.checkArgument(type != HoodieSchemaType.DECIMAL, () -> "Decimal precision and scale must be specified");
    ValidationUtils.checkArgument(type != HoodieSchemaType.TIME, () -> "Time precision must be specified");
    ValidationUtils.checkArgument(type != HoodieSchemaType.TIMESTAMP, () -> "Timestamp precision must be specified");
    if (type == HoodieSchemaType.DATE) {
      return createDate();
    } else if (type == HoodieSchemaType.UUID) {
      return createUUID();
    }

    Schema.Type avroType = type.toAvroType();
    ValidationUtils.checkState(avroType != null, "Converted Avro type cannot be null");

    Schema avroSchema = Schema.create(avroType);
    return new HoodieSchema(avroSchema);
  }

  /**
   * Creates a nullable schema for the specified primitive type.
   * @param type the primitive schema type
   * @return new HoodieSchema representing a nullable version of the primitive type
   */
  public static HoodieSchema createNullable(HoodieSchemaType type) {
    HoodieSchema nonNullSchema = create(type);
    return createNullable(nonNullSchema);
  }

  /**
   * Creates a nullable schema (union of null and the specified schema).
   *
   * @param schema the schema to make nullable
   * @return new HoodieSchema representing a nullable version of the input schema
   */
  public static HoodieSchema createNullable(HoodieSchema schema) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");

    Schema inputAvroSchema = schema.avroSchema;
    ValidationUtils.checkState(inputAvroSchema != null, "Input schema's Avro schema cannot be null");

    if (schema.getType() == HoodieSchemaType.UNION) {
      // Already a union, check if it contains null
      List<HoodieSchema> unionTypes = schema.getTypes();
      boolean hasNull = unionTypes.stream().anyMatch(s -> s.getType() == HoodieSchemaType.NULL);

      if (hasNull) {
        return schema; // Already nullable
      }

      // Add null to existing union
      List<HoodieSchema> newUnionTypes = new ArrayList<>(unionTypes.size() + 1);
      newUnionTypes.add(NULL_SCHEMA);
      newUnionTypes.addAll(unionTypes);
      return HoodieSchema.createUnion(newUnionTypes);
    } else {
      // Create new union with null
      List<HoodieSchema> unionTypes = new ArrayList<>(2);
      unionTypes.add(NULL_SCHEMA);
      unionTypes.add(schema);
      return HoodieSchema.createUnion(unionTypes);
    }
  }

  /**
   * Creates an array schema with the specified element schema.
   *
   * @param elementSchema the schema for array elements
   * @return new HoodieSchema representing an array
   */
  public static HoodieSchema createArray(HoodieSchema elementSchema) {
    ValidationUtils.checkArgument(elementSchema != null, "Element schema cannot be null");

    Schema elementAvroSchema = elementSchema.avroSchema;
    ValidationUtils.checkState(elementAvroSchema != null, "Element schema's Avro schema cannot be null");
    ValidationUtils.checkArgument(!elementSchema.containsBlobType(),
        "Array element type cannot be or contain a BLOB type");

    Schema arraySchema = Schema.createArray(elementAvroSchema);
    return new HoodieSchema(arraySchema);
  }

  /**
   * Creates a map schema with the specified value schema.
   *
   * @param valueSchema the schema for map values
   * @return new HoodieSchema representing a map
   */
  public static HoodieSchema createMap(HoodieSchema valueSchema) {
    ValidationUtils.checkArgument(valueSchema != null, "Value schema cannot be null");

    Schema valueAvroSchema = valueSchema.avroSchema;
    ValidationUtils.checkState(valueAvroSchema != null, "Value schema's Avro schema cannot be null");
    ValidationUtils.checkArgument(!valueSchema.containsBlobType(),
        "Map value type cannot be or contain a BLOB type");

    Schema mapSchema = Schema.createMap(valueAvroSchema);
    return new HoodieSchema(mapSchema);
  }

  /**
   * Creates a record schema with the specified properties.
   *
   * @param name      the record name
   * @param namespace the namespace (can be null)
   * @param doc       the documentation (can be null)
   * @param fields    the list of fields
   * @return new HoodieSchema representing a record
   */
  public static HoodieSchema createRecord(String name, String namespace, String doc, List<HoodieSchemaField> fields) {
    return createRecord(name, doc, namespace, false, fields);
  }

  /**
   * Creates a record schema with the specified properties, including error flag.
   *
   * @param name      the record name
   * @param doc       the documentation (can be null)
   * @param namespace the namespace (can be null)
   * @param isError   whether this is an error record
   * @param fields    the list of fields
   * @return new HoodieSchema representing a record
   */
  public static HoodieSchema createRecord(String name, String doc, String namespace, boolean isError, List<HoodieSchemaField> fields) {
    ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Record name cannot be null or empty");
    ValidationUtils.checkArgument(fields != null, "Fields cannot be null");

    // Convert HoodieSchemaFields to Avro Fields
    List<Schema.Field> avroFields = fields.stream()
        .map(HoodieSchemaField::getAvroField)
        .collect(Collectors.toList());

    Schema recordSchema = Schema.createRecord(name, doc, namespace, isError);
    recordSchema.setFields(avroFields);
    return new HoodieSchema(recordSchema, fields);
  }

  /**
   * Creates a union schema from multiple schemas.
   *
   * @param schemas the schemas to include in the union
   * @return new HoodieSchema representing the union
   */
  public static HoodieSchema createUnion(List<HoodieSchema> schemas) {
    ValidationUtils.checkArgument(schemas != null && !schemas.isEmpty(), "Union schemas cannot be null or empty");

    List<Schema> avroSchemas = schemas.stream()
        .map(HoodieSchema::getAvroSchema)
        .collect(Collectors.toList());

    Schema unionSchema = Schema.createUnion(avroSchemas);
    return new HoodieSchema(unionSchema);
  }

  /**
   * Creates a union schema from multiple schemas (varargs version).
   *
   * @param schemas the schemas to include in the union
   * @return new HoodieSchema representing the union
   */
  public static HoodieSchema createUnion(HoodieSchema... schemas) {
    ValidationUtils.checkArgument(schemas != null && schemas.length > 0, "Union schemas cannot be null or empty");
    return createUnion(Arrays.asList(schemas));
  }

  /**
   * Creates an enum schema with the specified properties.
   *
   * @param name      the enum name
   * @param namespace the namespace (can be null)
   * @param doc       the documentation (can be null)
   * @param symbols   the list of enum symbols
   * @return new HoodieSchema representing an enum
   */
  public static HoodieSchema createEnum(String name, String namespace, String doc, List<String> symbols) {
    ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Enum name cannot be null or empty");
    ValidationUtils.checkArgument(symbols != null && !symbols.isEmpty(), "Enum symbols cannot be null or empty");

    Schema enumSchema = Schema.createEnum(name, doc, namespace, symbols);
    return new HoodieSchema(enumSchema);
  }

  /**
   * Creates a fixed schema with the specified properties.
   *
   * @param name      the fixed type name
   * @param namespace the namespace (can be null)
   * @param doc       the documentation (can be null)
   * @param size      the size in bytes
   * @return new HoodieSchema representing a fixed type
   */
  public static HoodieSchema createFixed(String name, String namespace, String doc, int size) {
    ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Fixed name cannot be null or empty");
    ValidationUtils.checkArgument(size > 0, "Fixed size must be positive: " + size);

    Schema fixedSchema = Schema.createFixed(name, doc, namespace, size);
    return new HoodieSchema(fixedSchema);
  }

  /**
   * Creates a decimal schema backed by a fixed size byte array with the specified properties.
   * @param name      the fixed type name
   * @param namespace the namespace (can be null)
   * @param doc       the documentation (can be null)
   * @param precision the precision of the decimal value
   * @param scale     the scale of the decimal value
   * @param fixedSize the size in bytes
   * @return a new HoodieSchema representing a decimal type
   */
  public static HoodieSchema createDecimal(String name, String namespace, String doc, int precision, int scale, int fixedSize) {
    ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Decimal name cannot be null or empty");
    ValidationUtils.checkArgument(precision > 0, () -> "Decimal precision must be positive: " + precision);
    ValidationUtils.checkArgument(scale >= 0, () -> "Decimal scale cannot be negative: " + scale);
    ValidationUtils.checkArgument(scale <= precision, () -> "Decimal scale cannot be greater than precision: " + scale + " > " + precision);

    Schema decimalSchema = Schema.createFixed(name, doc, namespace, fixedSize);
    LogicalTypes.decimal(precision, scale).addToSchema(decimalSchema);
    return new HoodieSchema.Decimal(decimalSchema);
  }

  /**
   * Creates a decimal schema with the specified precision and scale.
   * @param precision the precision of the decimal value
   * @param scale     the scale of the decimal value
   * @return a new HoodieSchema representing a decimal type
   */
  public static HoodieSchema createDecimal(int precision, int scale) {
    ValidationUtils.checkArgument(precision > 0, () -> "Decimal precision must be positive: " + precision);
    ValidationUtils.checkArgument(scale >= 0, () -> "Decimal scale cannot be negative: " + scale);
    ValidationUtils.checkArgument(scale <= precision, () -> "Decimal scale cannot be greater than precision: " + scale + " > " + precision);

    Schema decimalSchema = Schema.create(Schema.Type.BYTES);
    LogicalTypes.decimal(precision, scale).addToSchema(decimalSchema);
    return new HoodieSchema.Decimal(decimalSchema);
  }

  /**
   * Creates a timestamp with milliseconds precision that is adjusted to UTC.
   * @return a new HoodieSchema representing a timestamp
   */
  public static HoodieSchema createTimestampMillis() {
    Schema timestampSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timestampMillis().addToSchema(timestampSchema);
    return new HoodieSchema.Timestamp(timestampSchema);
  }

  /**
   * Creates a timestamp with microseconds precision that is adjusted to UTC.
   * @return a new HoodieSchema representing a timestamp
   */
  public static HoodieSchema createTimestampMicros() {
    Schema timestampSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timestampMicros().addToSchema(timestampSchema);
    return new HoodieSchema.Timestamp(timestampSchema);
  }

  /**
   * Creates a local timestamp with milliseconds precision (no timezone adjustment).
   * @return a new HoodieSchema representing a local timestamp
   */
  public static HoodieSchema createLocalTimestampMillis() {
    Schema localTimestampSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.localTimestampMillis().addToSchema(localTimestampSchema);
    return new HoodieSchema.Timestamp(localTimestampSchema);
  }

  /**
   * Creates a local timestamp with microseconds precision (no timezone adjustment).
   * @return a new HoodieSchema representing a local timestamp
   */
  public static HoodieSchema createLocalTimestampMicros() {
    Schema localTimestampSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.localTimestampMicros().addToSchema(localTimestampSchema);
    return new HoodieSchema.Timestamp(localTimestampSchema);
  }

  /**
   * Creates a schema representing a time of day with milliseconds precision.
   * @return a new HoodieSchema representing time of day
   */
  public static HoodieSchema createTimeMillis() {
    Schema timeSchema = Schema.create(Schema.Type.INT);
    LogicalTypes.timeMillis().addToSchema(timeSchema);
    return new HoodieSchema.Time(timeSchema);
  }

  /**
   * Creates a schema representing a time of day with microseconds precision.
   * @return a new HoodieSchema representing time of day
   */
  public static HoodieSchema createTimeMicros() {
    Schema timeSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timeMicros().addToSchema(timeSchema);
    return new HoodieSchema.Time(timeSchema);
  }

  /**
   * Creates a schema representing a date.
   * @return a new HoodieSchema representing a date
   */
  public static HoodieSchema createDate() {
    Schema dateSchema = Schema.create(Schema.Type.INT);
    LogicalTypes.date().addToSchema(dateSchema);
    return new HoodieSchema(dateSchema);
  }

  /**
   * Creates a schema representing a random generated universally unique identifier.
   * @return a new HoodieSchema representing a UUID
   */
  public static HoodieSchema createUUID() {
    Schema uuidSchema = Schema.create(Schema.Type.STRING);
    LogicalTypes.uuid().addToSchema(uuidSchema);
    return new HoodieSchema(uuidSchema);
  }

  /**
   * Creates an unshredded Variant schema.
   * Unshredded variants have both metadata and value fields as required (non-nullable) binary fields.
   *
   * @return a new HoodieSchema.Variant representing an unshredded variant
   */
  public static HoodieSchema.Variant createVariant() {
    return createVariant(null, null, null);
  }

  /**
   * Creates an unshredded Variant schema with the specified name and namespace.
   *
   * @param name      the variant record name (can be null, defaults to "variant")
   * @param namespace the namespace (can be null)
   * @param doc       the documentation (can be null)
   * @return a new HoodieSchema.Variant representing an unshredded variant
   */
  public static HoodieSchema.Variant createVariant(String name, String namespace, String doc) {
    String variantName = (name != null && !name.isEmpty()) ? name : VariantLogicalType.VARIANT_LOGICAL_TYPE_NAME;

    // Create metadata field (required bytes)
    HoodieSchemaField metadataField = HoodieSchemaField.of(
        Variant.VARIANT_METADATA_FIELD,
        HoodieSchema.create(HoodieSchemaType.BYTES),
        "Variant metadata component",
        null
    );

    // Create value field (required bytes)
    HoodieSchemaField valueField = HoodieSchemaField.of(
        Variant.VARIANT_VALUE_FIELD,
        HoodieSchema.create(HoodieSchemaType.BYTES),
        "Variant value component",
        null
    );

    List<HoodieSchemaField> fields = Arrays.asList(metadataField, valueField);

    Schema recordSchema = Schema.createRecord(variantName, doc, namespace, false);
    List<Schema.Field> avroFields = fields.stream()
        .map(HoodieSchemaField::getAvroField)
        .collect(Collectors.toList());
    recordSchema.setFields(avroFields);

    // Add Variant logical type
    VariantLogicalType.variant().addToSchema(recordSchema);

    return new HoodieSchema.Variant(recordSchema);
  }

  /**
   * Creates a shredded Variant schema with an optional typed_value field.
   * Shredded variants have metadata (required), value (optional/nullable), and typed_value (optional) fields.
   *
   * @param typedValueSchema the schema for the typed_value field (can be null if typed_value is not needed)
   * @return a new HoodieSchema.Variant representing a shredded variant
   */
  public static HoodieSchema.Variant createVariantShredded(HoodieSchema typedValueSchema) {
    return createVariantShredded(null, null, null, typedValueSchema);
  }

  /**
   * Creates a shredded Variant schema with the specified name, namespace, and typed_value field.
   *
   * @param name             the variant record name (can be null, defaults to "variant")
   * @param namespace        the namespace (can be null)
   * @param doc              the documentation (can be null)
   * @param typedValueSchema the schema for the typed_value field (can be null if typed_value is not needed)
   * @return a new HoodieSchema.Variant representing a shredded variant
   */
  public static HoodieSchema.Variant createVariantShredded(String name, String namespace, String doc, HoodieSchema typedValueSchema) {
    String variantName = (name != null && !name.isEmpty()) ? name : VariantLogicalType.VARIANT_LOGICAL_TYPE_NAME;

    List<HoodieSchemaField> fields = new ArrayList<>();

    // Create metadata field (required bytes)
    fields.add(HoodieSchemaField.of(
        Variant.VARIANT_METADATA_FIELD,
        HoodieSchema.create(HoodieSchemaType.BYTES),
        "Variant metadata component",
        null
    ));

    // Create value field (nullable bytes for shredded)
    fields.add(HoodieSchemaField.of(
        Variant.VARIANT_VALUE_FIELD,
        HoodieSchema.createNullable(HoodieSchemaType.BYTES),
        "Variant value component",
        NULL_VALUE
    ));

    // Add typed_value field if provided
    if (typedValueSchema != null) {
      fields.add(HoodieSchemaField.of(
          Variant.VARIANT_TYPED_VALUE_FIELD,
          typedValueSchema,
          "Typed value for shredded variant",
          null
      ));
    }

    Schema recordSchema = Schema.createRecord(variantName, doc, namespace, false);
    List<Schema.Field> avroFields = fields.stream()
        .map(HoodieSchemaField::getAvroField)
        .collect(Collectors.toList());
    recordSchema.setFields(avroFields);

    // Add Variant logical type
    VariantLogicalType.variant().addToSchema(recordSchema);

    return new HoodieSchema.Variant(recordSchema);
  }

  public static HoodieSchema.Blob createBlob() {
    return new HoodieSchema.Blob(Blob.DEFAULT_NAME);
  }

  /**
   * Returns the Hudi schema version information.
   *
   * @return version string of the Hudi schema system
   */
  public static String getHudiSchemaVersion() {
    Package pkg = HoodieSchema.class.getPackage();
    String version = pkg != null ? pkg.getImplementationVersion() : null;
    return version != null ? version : "unknown";
  }

  /**
   * Returns the type of this schema.
   *
   * @return the schema type
   */
  public HoodieSchemaType getType() {
    return type;
  }

  /**
   * Returns the name of the schema if a record, otherwise it returns the name of the type.
   *
   * @return the schema name
   */
  public String getName() {
    if (avroSchema.getLogicalType() != null) {
      return type.name().toLowerCase(Locale.ENGLISH);
    }
    return avroSchema.getName();
  }

  /**
   * Returns the namespace of this schema, if it has one.
   *
   * @return Option containing the schema namespace, or Option.empty() if none
   */
  public Option<String> getNamespace() {
    return Option.ofNullable(avroSchema.getNamespace());
  }

  /**
   * Returns the full name of this schema (namespace + name) if a record, or the type name otherwise.
   *
   * @return The full schema name, or name of the type if not a record
   */
  public String getFullName() {
    return avroSchema.getFullName();
  }

  /**
   * Returns the documentation string for this schema, if any.
   *
   * @return Option containing the documentation, or Option.empty() if none
   */
  public Option<String> getDoc() {
    return Option.ofNullable(avroSchema.getDoc());
  }

  /**
   * Checks if this schema type supports field access.
   * Only RECORD and VARIANT types can have fields.
   *
   * @return true if this type can have fields (RECORD or VARIANT)
   */
  public boolean hasFields() {
    return type == HoodieSchemaType.RECORD || type == HoodieSchemaType.VARIANT || type == HoodieSchemaType.BLOB;
  }

  /**
   * Returns the fields of this record or variant schema.
   *
   * @return list of HoodieSchemaField objects
   * @throws IllegalStateException if this schema type does not support fields
   */
  public List<HoodieSchemaField> getFields() {
    if (!hasFields()) {
      throw new IllegalStateException("Cannot get fields from schema type: " + type);
    }
    if (fields == null) {
      fields = Collections.unmodifiableList(avroSchema.getFields().stream().map(HoodieSchemaField::new).collect(Collectors.toList()));
    }
    return fields;
  }

  /**
   * Sets the fields for this record or variant schema.
   *
   * @param fields the list of fields to set
   * @throws IllegalStateException if this schema type does not support fields
   */
  public void setFields(List<HoodieSchemaField> fields) {
    if (!hasFields()) {
      throw new IllegalStateException("Cannot set fields on schema type: " + type);
    }
    ValidationUtils.checkArgument(fields != null, "Fields cannot be null");

    List<Schema.Field> avroFields = fields.stream()
        .map(HoodieSchemaField::getAvroField)
        .collect(Collectors.toList());

    avroSchema.setFields(avroFields);
  }

  /**
   * Returns the field with the specified name.
   *
   * @param name the field name to look up
   * @return Option containing the field, or Option.empty() if not found
   * @throws IllegalStateException if this schema type does not support fields
   */
  public Option<HoodieSchemaField> getField(String name) {
    if (!hasFields()) {
      throw new IllegalStateException("Cannot get field from schema type: " + type);
    }

    ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Field name cannot be null or empty");

    return Option.ofNullable(getFieldMap().get(name));
  }

  private Map<String, HoodieSchemaField> getFieldMap() {
    if (fieldMap == null) {
      fieldMap = getFields().stream()
          .collect(Collectors.toMap(HoodieSchemaField::name, field -> field));
    }
    return fieldMap;
  }

  /**
   * Returns the element schema for array types.
   *
   * @return the element schema
   * @throws IllegalStateException if this is not an array schema
   */
  public HoodieSchema getElementType() {
    if (type != HoodieSchemaType.ARRAY) {
      throw new IllegalStateException("Cannot get element type from non-array schema: " + type);
    }

    return HoodieSchema.fromAvroSchema(avroSchema.getElementType());
  }

  /**
   * Returns the key schema for map types.
   *
   * @return the key schema
   * @throws IllegalStateException if this is not a map schema
   */
  public HoodieSchema getKeyType() {
    if (type != HoodieSchemaType.MAP) {
      throw new IllegalStateException("Cannot get key type from non-map schema: " + type);
    }

    return new HoodieSchema(Schema.create(Schema.Type.STRING));
  }

  /**
   * Returns the value schema for map types.
   *
   * @return the value schema
   * @throws IllegalStateException if this is not a map schema
   */
  public HoodieSchema getValueType() {
    if (type != HoodieSchemaType.MAP) {
      throw new IllegalStateException("Cannot get value type from non-map schema: " + type);
    }

    return HoodieSchema.fromAvroSchema(avroSchema.getValueType());
  }

  /**
   * Returns the schemas in a union type.
   *
   * @return list of schemas in the union
   * @throws IllegalStateException if this is not a union schema
   */
  public List<HoodieSchema> getTypes() {
    if (type != HoodieSchemaType.UNION) {
      throw new IllegalStateException("Cannot get types from non-union schema: " + type);
    }

    return avroSchema.getTypes().stream()
        .map(HoodieSchema::fromAvroSchema)
        .collect(Collectors.toList());
  }

  /**
   * Returns the size of a fixed schema.
   *
   * @return the fixed size in bytes
   * @throws IllegalStateException if this is not a fixed schema
   */
  public int getFixedSize() {
    if (type != HoodieSchemaType.FIXED) {
      throw new IllegalStateException("Cannot get fixed size from non-fixed schema: " + type);
    }

    return avroSchema.getFixedSize();
  }

  /**
   * Returns the symbols for an enum schema.
   *
   * @return list of enum symbols
   * @throws IllegalStateException if this is not an enum schema
   */
  public List<String> getEnumSymbols() {
    if (type != HoodieSchemaType.ENUM) {
      throw new IllegalStateException("Cannot get symbols from non-enum schema: " + type);
    }

    return new ArrayList<>(avroSchema.getEnumSymbols());
  }

  /**
   * Returns custom properties attached to this schema.
   *
   * @return map of custom properties
   */
  public Map<String, Object> getObjectProps() {
    return avroSchema.getObjectProps();
  }

  /**
   * Returns the value of a custom property.
   *
   * @param key the property key
   * @return the property value, or null if not found
   */
  public Object getProp(String key) {
    return avroSchema.getObjectProp(key);
  }

  /**
   * Adds a custom property to this schema.
   *
   * @param key   the property key
   * @param value the property value
   */
  public void addProp(String key, Object value) {
    ValidationUtils.checkArgument(key != null && !key.isEmpty(), "Property key cannot be null or empty");
    avroSchema.addProp(key, value);
  }

  /**
   * Checks if this is an error record schema.
   *
   * @return true if this is an error record
   * @throws IllegalStateException if this is not a record schema
   */
  public boolean isError() {
    if (type != HoodieSchemaType.RECORD) {
      throw new IllegalStateException("Cannot check error flag on non-record schema: " + type);
    }
    return avroSchema.isError();
  }

  /**
   * Checks if this schema is nullable (union containing null).
   *
   * @return true if the schema is nullable
   */
  public boolean isNullable() {
    if (type != HoodieSchemaType.UNION) {
      return false;
    }

    return avroSchema.getTypes().stream()
        .anyMatch(schema -> schema.getType() == Schema.Type.NULL);
  }

  public boolean isSchemaNull() {
    return type == null || type == HoodieSchemaType.NULL;
  }

  /**
   * If this is a union schema, returns the non-null type. Otherwise, returns this schema.
   *
   * @return the non-null schema from a union or the current schema
   */
  public HoodieSchema getNonNullType() {
    if (type != HoodieSchemaType.UNION) {
      return this;
    }

    List<HoodieSchema> types = getTypes();
    if (types.size() == 2) {
      if (types.get(0).getType() == HoodieSchemaType.NULL) {
        return types.get(1);
      } else if (types.get(1).getType() == HoodieSchemaType.NULL) {
        return types.get(0);
      } else {
        // This is a non-null union of types
        return this;
      }
    }
    List<HoodieSchema> nonNullTypes = new ArrayList<>(types.size() - 1);
    for (int i = 0; i < types.size(); i++) {
      HoodieSchema schema = types.get(i);
      if (schema.getType() != HoodieSchemaType.NULL) {
        if (i == types.size() - 1 && nonNullTypes.size() == types.size() - 1) {
          // Last type and all previous were non-null, return original schema
          return this;
        }
        nonNullTypes.add(schema);
      }
    }
    return HoodieSchema.createUnion(nonNullTypes);
  }

  boolean containsBlobType() {
    if (getType() == HoodieSchemaType.BLOB) {
      return true;
    } else if (getType() == HoodieSchemaType.UNION) {
      return getTypes().stream().anyMatch(HoodieSchema::containsBlobType);
    } else if (getType() == HoodieSchemaType.RECORD) {
      return getFields().stream().anyMatch(field -> field.schema().containsBlobType());
    }
    return false;
  }

  /**
   * Validates that the schema does not contain arrays or maps with blob types.
   * This method recursively traverses the schema tree to check for invalid structures.
   *
   * @param schema the schema to validate
   * @throws HoodieSchemaException if the schema contains arrays or maps with blob types
   */
  private static void validateNoBlobsInArrayOrMap(HoodieSchema schema) {
    if (schema == null) {
      return;
    }

    HoodieSchemaType type = schema.getType();

    switch (type) {
      case ARRAY:
        HoodieSchema elementType = schema.getElementType();
        if (elementType.containsBlobType()) {
          throw new HoodieSchemaException("Array element type cannot be or contain a BLOB type");
        }
        break;
      case MAP:
        HoodieSchema valueType = schema.getValueType();
        if (valueType.containsBlobType()) {
          throw new HoodieSchemaException("Map value type cannot be or contain a BLOB type");
        }
        break;
      case RECORD:
        // Validate all record fields
        List<HoodieSchemaField> fields = schema.getFields();
        for (HoodieSchemaField field : fields) {
          validateNoBlobsInArrayOrMap(field.schema());
        }
        break;
      case UNION:
        // Validate all union types
        List<HoodieSchema> types = schema.getTypes();
        for (HoodieSchema unionType : types) {
          validateNoBlobsInArrayOrMap(unionType);
        }
        break;
      // For primitives, BLOB, ENUM, FIXED, NULL - no nested validation needed
      default:
        break;
    }
  }

  /**
   * Gets a nested field using dot notation, supporting Parquet-style array/map accessors.
   *
   * <p>Supports nested field access using dot notation including MAP and ARRAY types
   * using Parquet-style accessor patterns:</p>
   *
   * <ul>
   *   <li><b>RECORD types:</b> Standard dot notation (e.g., {@code "user.profile.name"})</li>
   *   <li><b>ARRAY types:</b> Use {@code ".list.element"} to access array elements
   *       <ul>
   *         <li>Example: {@code "items.list.element"} accesses element schema of array</li>
   *         <li>Example: {@code "items.list.element.id"} accesses nested field within array elements</li>
   *       </ul>
   *   </li>
   *   <li><b>MAP types:</b> Use {@code ".key_value.key"} or {@code ".key_value.value"} to access map components
   *       <ul>
   *         <li>Example: {@code "metadata.key_value.key"} accesses map keys (always STRING)</li>
   *         <li>Example: {@code "metadata.key_value.value"} accesses map value schema</li>
   *         <li>Example: {@code "nested_map.key_value.value.field"} accesses nested field within map values</li>
   *       </ul>
   *   </li>
   * </ul>
   *
   * <p>Note: Spark Parquet files may use {@code ".array"} format instead of {@code ".list.element"}.
   * This translation is handled at the Parquet reading level in ParquetUtils, not here.</p>
   *
   * @param fieldName Field path (e.g., "user.profile.name", "items.list.element", "metadata.key_value.value")
   * @return Option containing a pair of canonical field name and the HoodieSchemaField, or Option.empty() if not found
   */
  public Option<Pair<String, HoodieSchemaField>> getNestedField(String fieldName) {
    ValidationUtils.checkArgument(fieldName != null && !fieldName.isEmpty(), "Field name cannot be null or empty");
    return getNestedFieldInternal(this, fieldName, 0, "");
  }

  /**
   * Internal helper method for recursively retrieving nested fields using offset-based navigation.
   *
   * @param schema   the current schema to search in
   * @param fullPath the full field path string
   * @param offset   current position in fullPath
   * @param prefix   the accumulated field path prefix
   * @return Option containing a pair of canonical field name and the HoodieSchemaField, or Option.empty() if field not found
   */
  private static Option<Pair<String, HoodieSchemaField>> getNestedFieldInternal(
      HoodieSchema schema, String fullPath, int offset, String prefix) {
    HoodieSchema nonNullableSchema = schema.getNonNullType();
    int nextDot = fullPath.indexOf('.', offset);
    // Terminal case: no more dots in this segment
    if (nextDot == -1) {
      String fieldName = fullPath.substring(offset);
      // Handle RECORD terminal case
      if (nonNullableSchema.getType() != HoodieSchemaType.RECORD) {
        return Option.empty();
      }
      return nonNullableSchema.getField(fieldName)
          .map(field -> Pair.of(prefix + fieldName, field));
    }
    // Recursive case: more nesting to explore
    String rootFieldName = fullPath.substring(offset, nextDot);
    int nextOffset = nextDot + 1;
    // Handle RECORD: standard field navigation
    if (nonNullableSchema.getType() == HoodieSchemaType.RECORD) {
      return nonNullableSchema.getField(rootFieldName)
          .flatMap(field -> getNestedFieldInternal(field.schema(), fullPath, nextOffset, prefix + rootFieldName + "."));
    }
    // Handle ARRAY: expect ".list.element"
    if (nonNullableSchema.getType() == HoodieSchemaType.ARRAY && ARRAY_LIST.equals(rootFieldName)) {
      return handleArrayNavigation(nonNullableSchema, fullPath, nextOffset, prefix);
    }
    // Handle MAP: expect ".key_value.key" or ".key_value.value"
    if (nonNullableSchema.getType() == HoodieSchemaType.MAP && MAP_KEY_VALUE.equals(rootFieldName)) {
      return handleMapNavigation(nonNullableSchema, fullPath, nextOffset, prefix);
    }
    return Option.empty();
  }

  /**
   * Handles navigation into ARRAY types using the ".list.element" pattern.
   *
   * @param arraySchema the ARRAY schema to navigate into
   * @param fullPath    the full field path string
   * @param offset      current position in fullPath (should point to "element")
   * @param prefix      the accumulated field path prefix
   * @return Option containing the nested field, or Option.empty() if invalid path
   */
  private static Option<Pair<String, HoodieSchemaField>> handleArrayNavigation(
      HoodieSchema arraySchema, String fullPath, int offset, String prefix) {
    int nextPos = getNextOffset(fullPath, offset, ARRAY_ELEMENT);
    if (nextPos == -1) {
      return Option.empty();
    }

    HoodieSchema elementSchema = arraySchema.getElementType();
    if (nextPos == fullPath.length()) {
      return Option.of(Pair.of(prefix + ARRAY_LIST_ELEMENT,
          HoodieSchemaField.of(ARRAY_ELEMENT, elementSchema, null, null)));
    }
    return getNestedFieldInternal(elementSchema, fullPath, nextPos, prefix + ARRAY_LIST_ELEMENT + ".");
  }

  /**
   * Handles navigation into MAP types using the Parquet-style ".key_value.key" or ".key_value.value" patterns.
   *
   * @param mapSchema the MAP schema to navigate into
   * @param fullPath  the full field path string
   * @param offset    current position in fullPath (should point to "key" or "value")
   * @param prefix    the accumulated field path prefix
   * @return Option containing the nested field, or Option.empty() if invalid path
   */
  private static Option<Pair<String, HoodieSchemaField>> handleMapNavigation(
      HoodieSchema mapSchema, String fullPath, int offset, String prefix) {
    // Check for "key" path
    int keyPos = getNextOffset(fullPath, offset, MAP_KEY);
    if (keyPos != -1) {
      if (keyPos == fullPath.length()) {
        return Option.of(Pair.of(prefix + MAP_KEY_VALUE_KEY,
            HoodieSchemaField.of(MAP_KEY, mapSchema.getKeyType(), null, null)));
      }
      // Map keys are primitives, cannot navigate further
      return Option.empty();
    }

    // Check for "value" path
    int valuePos = getNextOffset(fullPath, offset, MAP_VALUE);
    if (valuePos == -1) {
      return Option.empty();
    }

    HoodieSchema valueSchema = mapSchema.getValueType();
    if (valuePos == fullPath.length()) {
      return Option.of(Pair.of(prefix + MAP_KEY_VALUE_VALUE,
          HoodieSchemaField.of(MAP_VALUE, valueSchema, null, null)));
    }
    return getNestedFieldInternal(valueSchema, fullPath, valuePos, prefix + MAP_KEY_VALUE_VALUE + ".");
  }

  /**
   * Advances offset past a component name in the path, handling end-of-path and dot separator.
   *
   * @param path      the full path string
   * @param offset    current position in path
   * @param component the component name to match (e.g., "element", "key", "value")
   * @return new offset after component and dot, or path.length() if at end, or -1 if no match
   */
  private static int getNextOffset(String path, int offset, String component) {
    if (!path.regionMatches(offset, component, 0, component.length())) {
      return -1;
    }
    int next = offset + component.length();
    if (next == path.length()) {
      return next;
    }
    return (path.charAt(next) == '.') ? next + 1 : -1;
  }

  /**
   * Returns the underlying Avro schema for compatibility purposes.
   *
   * <p>This method is provided for gradual migration and should be used
   * sparingly. New code should prefer the HoodieSchema API.</p>
   *
   * @return the wrapped Avro Schema
   */
  public Schema getAvroSchema() {
    return avroSchema;
  }

  /**
   * Converts this HoodieSchema to an Avro Schema.
   * This is an alias for getAvroSchema() provided for API consistency.
   *
   * @return the equivalent Avro Schema
   */
  public Schema toAvroSchema() {
    return getAvroSchema();
  }

  /**
   * Returns the JSON representation of this schema.
   *
   * @return JSON schema string
   */
  public String toString() {
    return avroSchema.toString();
  }

  /**
   * Returns a pretty-printed JSON representation of this schema.
   *
   * @return pretty-printed JSON schema string
   */
  public String toString(boolean pretty) {
    return avroSchema.toString(pretty);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    HoodieSchema that = (HoodieSchema) obj;
    return Objects.equals(avroSchema, that.avroSchema);
  }

  // Static utility methods for table schema operations

  @Override
  public int hashCode() {
    return Objects.hash(avroSchema);
  }

  /**
   * Parser class for parsing JSON schemas, equivalent to Avro's Schema.Parser.
   *
   * <p>This provides a Hudi-specific way to parse JSON schema strings while
   * maintaining compatibility with Avro's parsing behavior.</p>
   *
   * <p>Usage example:
   * <pre>{@code
   * HoodieSchema schema = new HoodieSchema.Parser().parse(jsonSchemaString);
   * }</pre></p>
   */
  public static class Parser {

    private final Schema.Parser avroParser;

    /**
     * Creates a new HoodieSchema Parser.
     */
    public Parser() {
      this.avroParser = new Schema.Parser();
    }

    public Parser(boolean validateDefaults) {
      this.avroParser = new Schema.Parser().setValidateDefaults(validateDefaults);
    }

    /**
     * Parses a JSON schema string.
     *
     * @param jsonSchema the JSON schema string to parse
     * @return parsed HoodieSchema
     * @throws HoodieAvroSchemaException if the schema string is invalid
     */
    public HoodieSchema parse(String jsonSchema) {
      ValidationUtils.checkArgument(jsonSchema != null && !jsonSchema.trim().isEmpty(),
          "Schema string cannot be null or empty");

      try {
        Schema avroSchema = avroParser.parse(jsonSchema);
        return fromAvroSchema(avroSchema);
      } catch (IllegalArgumentException e) {
        throw new HoodieAvroSchemaException("Invalid schema string format", e);
      } catch (Exception e) {
        throw new HoodieAvroSchemaException("Failed to parse schema: " + jsonSchema, e);
      }
    }

    /**
     * Parses a schema from an InputStream.
     *
     * @param inputStream the InputStream containing the JSON schema
     * @return parsed HoodieSchema
     * @throws HoodieIOException if reading from the stream fails
     */
    public HoodieSchema parse(InputStream inputStream) {
      ValidationUtils.checkArgument(inputStream != null, "InputStream cannot be null");

      try {
        Schema avroSchema = avroParser.parse(inputStream);
        return fromAvroSchema(avroSchema);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to parse schema from InputStream", e);
      } catch (IllegalArgumentException e) {
        throw new HoodieAvroSchemaException("Invalid schema format in InputStream", e);
      } catch (Exception e) {
        throw new HoodieAvroSchemaException("Failed to parse schema from InputStream", e);
      }
    }
  }

  /**
   * Builder class for constructing HoodieSchema instances with a fluent API.
   *
   * <p>This builder provides a convenient way to construct complex schemas
   * while maintaining binary compatibility with Avro by building the underlying
   * Avro schema and wrapping it.</p>
   *
   * <p>Usage example:
   * <pre>{@code
   * HoodieSchema recordSchema = new HoodieSchema.Builder(HoodieSchemaType.RECORD)
   *     .setName("User")
   *     .setNamespace("com.example")
   *     .setDoc("User record schema")
   *     .setFields(fields)
   *     .build();
   * }</pre></p>
   */
  public static class Builder {

    private final HoodieSchemaType type;
    private String name;
    private String namespace;
    private String doc;
    private List<HoodieSchemaField> fields;
    private List<HoodieSchema> unionTypes;
    private HoodieSchema elementType;
    private HoodieSchema valueType;
    private int fixedSize;
    private List<String> enumSymbols;

    /**
     * Creates a new builder for the specified schema type.
     *
     * @param type the schema type to build
     * @throws IllegalArgumentException if type is null
     */
    public Builder(HoodieSchemaType type) {
      ValidationUtils.checkArgument(type != null, "Schema type cannot be null");
      this.type = type;
    }

    /**
     * Sets the name for named schema types (RECORD, ENUM, FIXED).
     *
     * @param name the schema name
     * @return this builder for chaining
     */
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the namespace for named schema types.
     *
     * @param namespace the schema namespace
     * @return this builder for chaining
     */
    public Builder setNamespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * Sets the documentation string.
     *
     * @param doc the documentation string
     * @return this builder for chaining
     */
    public Builder setDoc(String doc) {
      this.doc = doc;
      return this;
    }

    /**
     * Sets the fields for RECORD schemas.
     *
     * @param fields the list of fields
     * @return this builder for chaining
     */
    public Builder setFields(List<HoodieSchemaField> fields) {
      this.fields = fields;
      return this;
    }

    /**
     * Sets the union types for UNION schemas.
     *
     * @param unionTypes the list of schemas in the union
     * @return this builder for chaining
     */
    public Builder setUnionTypes(List<HoodieSchema> unionTypes) {
      this.unionTypes = unionTypes;
      return this;
    }

    /**
     * Sets the element type for ARRAY schemas.
     *
     * @param elementType the schema of array elements
     * @return this builder for chaining
     */
    public Builder setElementType(HoodieSchema elementType) {
      this.elementType = elementType;
      return this;
    }

    /**
     * Sets the value type for MAP schemas.
     *
     * @param valueType the schema of map values
     * @return this builder for chaining
     */
    public Builder setValueType(HoodieSchema valueType) {
      this.valueType = valueType;
      return this;
    }

    /**
     * Sets the size for FIXED schemas.
     *
     * @param fixedSize the size in bytes
     * @return this builder for chaining
     */
    public Builder setFixedSize(int fixedSize) {
      this.fixedSize = fixedSize;
      return this;
    }

    /**
     * Sets the symbols for ENUM schemas.
     *
     * @param enumSymbols the list of enum symbols
     * @return this builder for chaining
     */
    public Builder setEnumSymbols(List<String> enumSymbols) {
      this.enumSymbols = enumSymbols;
      return this;
    }

    /**
     * Builds the HoodieSchema instance.
     *
     * @return new HoodieSchema instance
     * @throws IllegalArgumentException if required properties are missing or invalid
     */
    public HoodieSchema build() {
      Schema avroSchema;

      switch (type) {
        case NULL:
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BYTES:
        case STRING:
          avroSchema = Schema.create(type.toAvroType());
          break;

        case RECORD:
          ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Record name is required");
          ValidationUtils.checkArgument(fields != null, "Record fields are required");

          List<Schema.Field> avroFields = fields.stream()
              .map(HoodieSchemaField::getAvroField)
              .collect(Collectors.toList());

          avroSchema = Schema.createRecord(name, doc, namespace, false);
          avroSchema.setFields(avroFields);
          break;

        case ARRAY:
          ValidationUtils.checkArgument(elementType != null, "Array element type is required");
          ValidationUtils.checkArgument(!elementType.containsBlobType(),
              "Array element type cannot be or contain a BLOB type");
          avroSchema = Schema.createArray(elementType.getAvroSchema());
          break;

        case MAP:
          ValidationUtils.checkArgument(valueType != null, "Map value type is required");
          ValidationUtils.checkArgument(!valueType.containsBlobType(),
              "Map value type cannot be or contain a BLOB type");
          avroSchema = Schema.createMap(valueType.getAvroSchema());
          break;

        case UNION:
          ValidationUtils.checkArgument(unionTypes != null && !unionTypes.isEmpty(), "Union types are required");
          List<Schema> avroUnionTypes = unionTypes.stream()
              .map(HoodieSchema::getAvroSchema)
              .collect(Collectors.toList());
          avroSchema = Schema.createUnion(avroUnionTypes);
          break;

        case ENUM:
          ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Enum name is required");
          ValidationUtils.checkArgument(enumSymbols != null && !enumSymbols.isEmpty(),
              "Enum symbols are required");
          avroSchema = Schema.createEnum(name, doc, namespace, enumSymbols);
          break;

        case FIXED:
          ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Fixed name is required");
          ValidationUtils.checkArgument(fixedSize > 0, "Fixed size must be positive: " + fixedSize);
          avroSchema = Schema.createFixed(name, doc, namespace, fixedSize);
          break;

        default:
          throw new IllegalArgumentException("Unsupported schema type: " + type);
      }

      return new HoodieSchema(avroSchema);
    }
  }

  public static class Decimal extends HoodieSchema {
    private final int precision;
    private final int scale;
    private final Option<Integer> fixedSize;

    /**
     * Creates a new HoodieSchema wrapping the given Avro schema.
     *
     * @param avroSchema the Avro schema to wrap, cannot be null
     * @throws IllegalArgumentException if avroSchema is null or does not have a valid decimal logical type
     */
    private Decimal(Schema avroSchema) {
      super(avroSchema);
      LogicalType logicalType = avroSchema.getLogicalType();
      if (!(logicalType instanceof LogicalTypes.Decimal)) {
        throw new IllegalArgumentException("Decimal schema does not have a decimal logical type: " + avroSchema);
      }
      this.precision = ((LogicalTypes.Decimal) logicalType).getPrecision();
      this.scale = ((LogicalTypes.Decimal) logicalType).getScale();
      if (avroSchema.getType() == Schema.Type.FIXED) {
        this.fixedSize = Option.of(avroSchema.getFixedSize());
      } else {
        this.fixedSize = Option.empty();
      }
    }

    public int getPrecision() {
      return precision;
    }

    public int getScale() {
      return scale;
    }

    @Override
    public String getName() {
      return String.format("decimal(%d,%d)", precision, scale);
    }

    public boolean isFixed() {
      return fixedSize.isPresent();
    }

    @Override
    public int getFixedSize() {
      if (fixedSize.isPresent()) {
        return fixedSize.get();
      } else {
        throw new IllegalStateException("Cannot get fixed size from non-fixed decimal schema");
      }
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      Decimal decimal = (Decimal) o;
      return precision == decimal.precision && scale == decimal.scale && Objects.equals(fixedSize, decimal.fixedSize);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), precision, scale, fixedSize);
    }
  }

  public static class Timestamp extends HoodieSchema {
    private final boolean isUtcAdjusted;
    private final TimePrecision precision;

    /**
     * Creates a new HoodieSchema wrapping the given Avro schema.
     *
     * @param avroSchema the Avro schema to wrap, cannot be null
     * @throws IllegalArgumentException if avroSchema is null or does not have a valid timestamp logical type
     */
    private Timestamp(Schema avroSchema) {
      super(avroSchema);
      LogicalType logicalType = avroSchema.getLogicalType();
      if (logicalType == null) {
        throw new IllegalArgumentException("Timestamp schema does not have a logical type: " + avroSchema);
      } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
        this.precision = TimePrecision.MILLIS;
        this.isUtcAdjusted = true;
      } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
        this.precision = TimePrecision.MICROS;
        this.isUtcAdjusted = true;
      } else if (logicalType instanceof LogicalTypes.LocalTimestampMillis) {
        this.precision = TimePrecision.MILLIS;
        this.isUtcAdjusted = false;
      } else if (logicalType instanceof LogicalTypes.LocalTimestampMicros) {
        this.precision = TimePrecision.MICROS;
        this.isUtcAdjusted = false;
      } else {
        throw new IllegalArgumentException("Unsupported timestamp logical type: " + logicalType);
      }
    }

    public TimePrecision getPrecision() {
      return precision;
    }

    public boolean isUtcAdjusted() {
      return isUtcAdjusted;
    }

    @Override
    public String getName() {
      if (isUtcAdjusted) {
        if (precision == TimePrecision.MILLIS) {
          return "timestamp-millis";
        } else {
          return "timestamp-micros";
        }
      } else {
        if (precision == TimePrecision.MILLIS) {
          return "local-timestamp-millis";
        } else {
          return "local-timestamp-micros";
        }
      }
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      Timestamp timestamp = (Timestamp) o;
      return isUtcAdjusted == timestamp.isUtcAdjusted && precision == timestamp.precision;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), isUtcAdjusted, precision);
    }
  }

  public static class Time extends HoodieSchema {
    private final TimePrecision precision;

    /**
     * Creates a new HoodieSchema wrapping the given Avro schema.
     *
     * @param avroSchema the Avro schema to wrap, cannot be null
     * @throws IllegalArgumentException if avroSchema is null or does not have a valid time logical type
     */
    private Time(Schema avroSchema) {
      super(avroSchema);
      LogicalType logicalType = avroSchema.getLogicalType();
      if (logicalType == null) {
        throw new IllegalArgumentException("Time schema does not have a logical type: " + avroSchema);
      } else if (logicalType instanceof LogicalTypes.TimeMillis) {
        this.precision = TimePrecision.MILLIS;
      } else if (logicalType instanceof LogicalTypes.TimeMicros) {
        this.precision = TimePrecision.MICROS;
      } else {
        throw new IllegalArgumentException("Unsupported time logical type: " + logicalType);
      }
    }

    public TimePrecision getPrecision() {
      return precision;
    }

    @Override
    public String getName() {
      if (precision == TimePrecision.MILLIS) {
        return "time-millis";
      } else {
        return "time-micros";
      }
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      Time time = (Time) o;
      return precision == time.precision;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), precision);
    }
  }

  public enum TimePrecision {
    MILLIS,
    MICROS
  }

  /**
   * Custom Avro LogicalType for Variant.
   * This logical type is applied to RECORD schemas that represent Variant types.
   *
   * <p>This is a singleton type - use {@link #variant()} to get the instance.</p>
   */
  static class VariantLogicalType extends LogicalType {

    private static final String VARIANT_LOGICAL_TYPE_NAME = "variant";
    // Eager initialization of singleton
    private static final VariantLogicalType INSTANCE = new VariantLogicalType();

    private VariantLogicalType() {
      super(VariantLogicalType.VARIANT_LOGICAL_TYPE_NAME);
    }

    /**
     * Returns the singleton instance of VariantLogicalType.
     *
     * @return the Variant logical type instance
     */
    public static VariantLogicalType variant() {
      return INSTANCE;
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.RECORD) {
        throw new IllegalArgumentException("Variant logical type can only be applied to RECORD schemas, got: " + schema.getType());
      }
    }
  }

  /**
   * Factory for creating VariantLogicalType instances.
   */
  private static class VariantLogicalTypeFactory implements LogicalTypes.LogicalTypeFactory {
    @Override
    public LogicalType fromSchema(Schema schema) {
      return VariantLogicalType.variant();
    }

    @Override
    public String getTypeName() {
      return VariantLogicalType.VARIANT_LOGICAL_TYPE_NAME;
    }
  }

  /**
   * Variant schema type representing semi-structured data that can store values of different types.
   *
   * <p>According to the Parquet specification, a Variant is represented as a record/group with binary fields:
   * <ul>
   *   <li>metadata: Binary field containing the Variant metadata component (always required)</li>
   *   <li>value: Binary field containing the Variant value component (required for unshredded, optional for shredded)</li>
   *   <li>typed_value: Optional field for shredded variants, stores values matching a specific type (type varies)</li>
   * </ul>
   * </p>
   *
   * <p>This implementation supports both:</p>
   * <ul>
   *   <li><b>Unshredded Variant</b>: metadata (required) and value (required) fields.</li>
   *   <li><b>Shredded Variant</b>: metadata (required), value (optional), and typed_value (optional).</li>
   * </ul>
   *
   * <p>Backwards compatibility:</p>
   * <ul>
   *   <li>Old Hudi versions will read it as a regular record with byte array fields</li>
   *   <li>New Hudi versions can detect it as a Variant type via the Avro LogicalType mechanism</li>
   * </ul>
   */
  public static class Variant extends HoodieSchema {

    private static final String VARIANT_DEFAULT_NAME = "variant";
    private static final String VARIANT_METADATA_FIELD = "metadata";
    private static final String VARIANT_VALUE_FIELD = "value";
    private static final String VARIANT_TYPED_VALUE_FIELD = "typed_value";

    private final boolean isShredded;
    private final Option<HoodieSchema> typedValueSchema;

    /**
     * Creates a new Variant HoodieSchema wrapping the given Avro schema.
     *
     * @param avroSchema the Avro schema to wrap, must be a valid Variant schema
     * @throws IllegalArgumentException if avroSchema is null or not a valid Variant schema
     */
    private Variant(Schema avroSchema) {
      super(avroSchema);
      this.isShredded = determineIfShredded(avroSchema);
      this.typedValueSchema = extractTypedValueSchema(avroSchema);
      validateVariantSchema(avroSchema);
    }

    /**
     * Determines if the variant schema is shredded based on the value field nullability or presence of typed_value.
     *
     * @param avroSchema the schema to check
     * @return true if the value field is nullable or typed_value exists (shredded), false otherwise (unshredded)
     */
    private boolean determineIfShredded(Schema avroSchema) {
      // Check if typed_value field exists
      Schema.Field typedValueField = avroSchema.getField(VARIANT_TYPED_VALUE_FIELD);
      if (typedValueField != null) {
        return true;
      }

      // Check if value field is nullable
      Schema.Field valueField = avroSchema.getField(VARIANT_VALUE_FIELD);
      if (valueField == null) {
        return false;
      }
      Schema valueSchema = valueField.schema();
      if (valueSchema.getType() == Schema.Type.UNION) {
        return valueSchema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
      }
      return false;
    }

    /**
     * Extracts the typed_value field schema if present.
     *
     * @param avroSchema the schema to extract from
     * @return Option containing the typed_value schema, or Option.empty() if not present
     */
    private Option<HoodieSchema> extractTypedValueSchema(Schema avroSchema) {
      Schema.Field typedValueField = avroSchema.getField(VARIANT_TYPED_VALUE_FIELD);
      if (typedValueField != null) {
        return Option.of(HoodieSchema.fromAvroSchema(typedValueField.schema()));
      }
      return Option.empty();
    }

    /**
     * Validates that the given Avro schema conforms to the Variant specification.
     *
     * @param avroSchema the schema to validate
     * @throws IllegalArgumentException if the schema is not a valid Variant schema
     */
    private void validateVariantSchema(Schema avroSchema) {
      if (avroSchema.getType() != Schema.Type.RECORD) {
        throw new IllegalArgumentException("Variant schema must be a RECORD type, got: " + avroSchema.getType());
      }

      // Check for metadata field (always required)
      Schema.Field metadataField = avroSchema.getField(VARIANT_METADATA_FIELD);
      if (metadataField == null) {
        throw new IllegalArgumentException("Variant schema must have a '" + VARIANT_METADATA_FIELD + "' field");
      }
      if (metadataField.schema().getType() != Schema.Type.BYTES) {
        throw new IllegalArgumentException("Variant metadata field must be BYTES type, got: " + metadataField.schema().getType());
      }

      // Check for value field
      Schema.Field valueField = avroSchema.getField(VARIANT_VALUE_FIELD);
      if (valueField == null) {
        throw new IllegalArgumentException("Variant schema must have a '" + VARIANT_VALUE_FIELD + "' field");
      }

      Schema valueSchema = valueField.schema();
      if (isShredded) {
        // Shredded: value should be nullable (union with null and bytes)
        if (valueSchema.getType() == Schema.Type.UNION) {
          boolean hasNull = valueSchema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
          boolean hasBytes = valueSchema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.BYTES);
          if (!hasNull || !hasBytes) {
            throw new IllegalArgumentException("Shredded Variant value field should be a union of [null, bytes]");
          }
        } else if (valueSchema.getType() != Schema.Type.BYTES) {
          // If not a union, it should at least be bytes (some shredded variants may have non-null value)
          throw new IllegalArgumentException("Shredded Variant value field must be BYTES or nullable BYTES, got: " + valueSchema.getType());
        }
      } else {
        // Unshredded: value must be non-nullable bytes
        if (valueSchema.getType() != Schema.Type.BYTES) {
          throw new IllegalArgumentException("Unshredded Variant value field must be BYTES type, got: " + valueSchema.getType());
        }
      }
    }

    /**
     * Checks if this is a shredded variant (has typed_value field or nullable value field).
     *
     * @return true if this is a shredded variant, false for unshredded
     */
    public boolean isShredded() {
      return isShredded;
    }

    /**
     * Returns the metadata field schema.
     *
     * @return HoodieSchema for the metadata field (always BYTES)
     */
    public HoodieSchema getMetadataField() {
      Schema.Field metadataField = getAvroSchema().getField(VARIANT_METADATA_FIELD);
      return HoodieSchema.fromAvroSchema(metadataField.schema());
    }

    /**
     * Returns the value field schema.
     *
     * @return HoodieSchema for the value field (BYTES for unshredded, nullable BYTES for shredded)
     */
    public HoodieSchema getValueField() {
      Schema.Field valueField = getAvroSchema().getField(VARIANT_VALUE_FIELD);
      return HoodieSchema.fromAvroSchema(valueField.schema());
    }

    /**
     * Returns the typed_value field schema if present (shredded variants only).
     *
     * @return Option containing the typed_value schema, or Option.empty() if not present
     */
    public Option<HoodieSchema> getTypedValueField() {
      return typedValueSchema;
    }

    @Override
    public String getName() {
      return VARIANT_DEFAULT_NAME;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      Variant variant = (Variant) o;
      return isShredded == variant.isShredded && Objects.equals(typedValueSchema, variant.typedValueSchema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), isShredded, typedValueSchema);
    }
  }

  static class BlobLogicalType extends LogicalType {

    private static final String BLOB_LOGICAL_TYPE_NAME = "blob";
    // Eager initialization of singleton
    private static final BlobLogicalType INSTANCE = new BlobLogicalType();

    private BlobLogicalType() {
      super(BlobLogicalType.BLOB_LOGICAL_TYPE_NAME);
    }

    public static BlobLogicalType blob() {
      return INSTANCE;
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.RECORD) {
        throw new IllegalArgumentException("Blob logical type can only be applied to RECORD schemas, got: " + schema.getType());
      }
    }
  }

  /**
   * Factory for creating VariantLogicalType instances.
   */
  private static class BlogLogicalTypeFactory implements LogicalTypes.LogicalTypeFactory {
    @Override
    public LogicalType fromSchema(Schema schema) {
      return BlobLogicalType.blob();
    }

    @Override
    public String getTypeName() {
      return BlobLogicalType.BLOB_LOGICAL_TYPE_NAME;
    }
  }

  /**
   * Blob types represent raw binary data. The data can be stored in-line as a byte array or out-of-line as a reference to a file or offset and length within that file.
   */
  public static class Blob extends HoodieSchema {
    private static final String DEFAULT_NAME = "blob";
    public static final String TYPE = "type";
    public static final String INLINE_DATA_FIELD = "data";
    public static final String EXTERNAL_REFERENCE = "reference";
    public static final String EXTERNAL_REFERENCE_PATH = "external_path";
    // if offset is not specified, it is assumed to be 0 (start of file)
    public static final String EXTERNAL_REFERENCE_OFFSET = "offset";
    // if length is not specified, it is assumed to be the rest of the file starting from offset
    public static final String EXTERNAL_REFERENCE_LENGTH = "length";
    public static final String EXTERNAL_REFERENCE_IS_MANAGED = "managed";

    /**
     * Creates a new HoodieSchema wrapping the given Avro schema.
     *
     * @param name Name for the blob schema
     * @throws IllegalArgumentException if avroSchema is null or does not have a valid blob logical type
     */
    private Blob(String name) {
      super(createSchema(name));
    }

    private Blob(Schema avroSchema) {
      super(avroSchema);
    }

    @Override
    public String getName() {
      return "blob";
    }

    @Override
    public HoodieSchemaType getType() {
      return HoodieSchemaType.BLOB;
    }

    private static Schema createSchema(String name) {
      Schema bytesField = Schema.create(Schema.Type.BYTES);
      Schema referenceField = Schema.createRecord(EXTERNAL_REFERENCE, null, null, false);
      List<Schema.Field> referenceFields = Arrays.asList(
          new Schema.Field(EXTERNAL_REFERENCE_PATH, Schema.create(Schema.Type.STRING), null, null),
          new Schema.Field(EXTERNAL_REFERENCE_OFFSET, AvroSchemaUtils.createNullableSchema(Schema.create(Schema.Type.LONG)), null, null),
          new Schema.Field(EXTERNAL_REFERENCE_LENGTH, AvroSchemaUtils.createNullableSchema(Schema.create(Schema.Type.LONG)), null, null),
          new Schema.Field(EXTERNAL_REFERENCE_IS_MANAGED, Schema.create(Schema.Type.BOOLEAN), null, null)
      );
      referenceField.setFields(referenceFields);

      Schema blobSchema = Schema.createRecord(name, null, null, false);
      List<Schema.Field> blobFields = Arrays.asList(
          new Schema.Field(TYPE, Schema.createEnum("blob_storage_type", null, null, Arrays.asList("INLINE", "OUT_OF_LINE")), null, null),
          new Schema.Field(INLINE_DATA_FIELD, AvroSchemaUtils.createNullableSchema(bytesField), null, Schema.Field.NULL_DEFAULT_VALUE),
          new Schema.Field(EXTERNAL_REFERENCE, AvroSchemaUtils.createNullableSchema(referenceField), null, Schema.Field.NULL_DEFAULT_VALUE)
      );
      blobSchema.setFields(blobFields);
      BlobLogicalType.blob().addToSchema(blobSchema);
      return blobSchema;
    }

  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.defaultWriteObject();
    oos.writeObject(avroSchema.toString());
  }

  private void readObject(ObjectInputStream ois)
      throws ClassNotFoundException, IOException {
    ois.defaultReadObject();
    String schemaString = (String) ois.readObject();
    avroSchema = new Schema.Parser().parse(schemaString);
    type = HoodieSchemaType.fromAvro(avroSchema);
  }
}
