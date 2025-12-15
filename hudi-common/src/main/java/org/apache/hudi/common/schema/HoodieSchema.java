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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.exception.HoodieIOException;

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

  /**
   * Constant representing a null JSON value, equivalent to JsonProperties.NULL_VALUE.
   * This provides compatibility with Avro's JsonProperties while maintaining Hudi's API.
   */
  public static final Object NULL_VALUE = JsonProperties.NULL_VALUE;
  private static final long serialVersionUID = 1L;
  private Schema avroSchema;
  private HoodieSchemaType type;

  /**
   * Creates a new HoodieSchema wrapping the given Avro schema.
   *
   * @param avroSchema the Avro schema to wrap, cannot be null
   * @throws IllegalArgumentException if avroSchema is null
   */
  private HoodieSchema(Schema avroSchema) {
    ValidationUtils.checkArgument(avroSchema != null, "Avro schema cannot be null");
    this.avroSchema = avroSchema;
    Schema.Type avroType = avroSchema.getType();
    ValidationUtils.checkState(avroType != null, "Avro schema type cannot be null");
    this.type = HoodieSchemaType.fromAvro(avroSchema);
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
    LogicalType logicalType = avroSchema.getLogicalType();
    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Decimal) {
        return new HoodieSchema.Decimal(avroSchema);
      } else if (logicalType instanceof LogicalTypes.TimeMillis || logicalType instanceof LogicalTypes.TimeMicros) {
        return new HoodieSchema.Time(avroSchema);
      } else if (logicalType instanceof LogicalTypes.TimestampMillis || logicalType instanceof LogicalTypes.TimestampMicros
          || logicalType instanceof LogicalTypes.LocalTimestampMillis || logicalType instanceof LogicalTypes.LocalTimestampMicros) {
        return new HoodieSchema.Timestamp(avroSchema);
      }
    }
    return new HoodieSchema(avroSchema);
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
      List<Schema> unionTypes = inputAvroSchema.getTypes();
      boolean hasNull = unionTypes.stream().anyMatch(s -> s.getType() == Schema.Type.NULL);

      if (hasNull) {
        return schema; // Already nullable
      }

      // Add null to existing union
      List<Schema> newUnionTypes = new ArrayList<>(unionTypes.size() + 1);
      newUnionTypes.add(Schema.create(Schema.Type.NULL));
      newUnionTypes.addAll(unionTypes);
      Schema nullableSchema = Schema.createUnion(newUnionTypes);
      return new HoodieSchema(nullableSchema);
    } else {
      // Create new union with null
      List<Schema> unionTypes = new ArrayList<>(2);
      unionTypes.add(Schema.create(Schema.Type.NULL));
      unionTypes.add(inputAvroSchema);
      Schema nullableSchema = Schema.createUnion(unionTypes);
      return new HoodieSchema(nullableSchema);
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
    return new HoodieSchema(recordSchema);
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
   * Creates a Hudi write schema from a given schema string with optional operation field.
   * This is equivalent to HoodieAvroUtils.createHoodieWriteSchema() but returns HoodieSchema.
   *
   * @param schemaStr          the schema string to convert
   * @param withOperationField whether to include operation field metadata
   * @return HoodieSchema configured for write operations
   * @throws IllegalArgumentException if schema string is invalid
   */
  public static HoodieSchema createHoodieWriteSchema(String schemaStr, boolean withOperationField) {
    ValidationUtils.checkArgument(schemaStr != null && !schemaStr.trim().isEmpty(),
        "Schema string cannot be null or empty");
    Schema avroSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaStr, withOperationField);
    return HoodieSchema.fromAvroSchema(avroSchema);
  }

  /**
   * Adds metadata fields to an existing HoodieSchema.
   * This is equivalent to HoodieAvroUtils.addMetadataFields() but operates on HoodieSchemas.
   *
   * @param schema             the base schema to add metadata fields to
   * @param withOperationField whether to include operation field metadata
   * @return new HoodieSchema with metadata fields added
   * @throws IllegalArgumentException if schema is null
   */
  public static HoodieSchema addMetadataFields(HoodieSchema schema, boolean withOperationField) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");
    Schema avroSchema = schema.toAvroSchema();
    Schema resultAvro = HoodieAvroUtils.addMetadataFields(avroSchema, withOperationField);
    return HoodieSchema.fromAvroSchema(resultAvro);
  }

  /**
   * Removes metadata fields from a HoodieSchema.
   * This is equivalent to HoodieAvroUtils.removeMetadataFields() but operates on HoodieSchemas.
   *
   * @param schema the schema to remove metadata fields from
   * @return new HoodieSchema without metadata fields
   * @throws IllegalArgumentException if schema is null
   */
  public static HoodieSchema removeMetadataFields(HoodieSchema schema) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");
    Schema avroSchema = schema.toAvroSchema();
    Schema resultAvro = HoodieAvroUtils.removeMetadataFields(avroSchema);
    return HoodieSchema.fromAvroSchema(resultAvro);
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
   * Returns the fields of this record schema.
   *
   * @return list of HoodieSchemaField objects
   * @throws IllegalStateException if this is not a record schema
   */
  public List<HoodieSchemaField> getFields() {
    if (type != HoodieSchemaType.RECORD) {
      throw new IllegalStateException("Cannot get fields from non-record schema: " + type);
    }

    return avroSchema.getFields().stream()
        .map(HoodieSchemaField::new)
        .collect(Collectors.toList());
  }

  /**
   * Sets the fields for this record schema.
   *
   * @param fields the list of fields to set
   * @throws IllegalStateException if this is not a record schema
   */
  public void setFields(List<HoodieSchemaField> fields) {
    if (type != HoodieSchemaType.RECORD) {
      throw new IllegalStateException("Cannot set fields on non-record schema: " + type);
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
   * @throws IllegalStateException if this is not a record schema
   */
  public Option<HoodieSchemaField> getField(String name) {
    if (type != HoodieSchemaType.RECORD) {
      throw new IllegalStateException("Cannot get field from non-record schema: " + type);
    }

    ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Field name cannot be null or empty");

    Schema.Field avroField = avroSchema.getField(name);
    return avroField != null ? Option.of(new HoodieSchemaField(avroField)) : Option.empty();
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
    return avroSchema.getProp(key);
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
   * @throws IllegalStateException if the union has more than two types
   */
  public HoodieSchema getNonNullType() {
    if (type != HoodieSchemaType.UNION) {
      return this;
    }

    List<HoodieSchema> types = getTypes();
    if (types.size() != 2) {
      throw new IllegalStateException("Union schema has more than two types");
    }
    return types.get(0).getType() != HoodieSchemaType.NULL ? types.get(0) : types.get(1);
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
      } catch (Exception e) {
        throw new HoodieAvroSchemaException("Failed to parse schema", e);
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
          avroSchema = Schema.createArray(elementType.getAvroSchema());
          break;

        case MAP:
          ValidationUtils.checkArgument(valueType != null, "Map value type is required");
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
