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

package org.apache.hudi.avro;

import org.apache.hudi.common.avro.ConvertingGenericData;
import org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.avro.VariantSchemaUtils;
import org.apache.hudi.common.avro.VariantShreddingProvider;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_PROVIDER_CLASS;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED;

/**
 * Wrap AvroWriterSupport for plugging in the bloom filter and variant shredding support.
 *
 * <p>When variant columns are configured for shredding (via {@link HoodieSchema.Variant#isShredded()}),
 * this class transforms variant records at write time to populate {@code typed_value} columns
 * by parsing variant binary data using a {@link VariantShreddingProvider} loaded via reflection.</p>
 */
public class HoodieAvroWriteSupport<T> extends AvroWriteSupport<T> {

  private final Option<HoodieBloomFilterWriteSupport<String>> bloomFilterWriteSupportOpt;
  private final Map<String, String> footerMetadata = new HashMap<>();
  protected final Properties properties;

  /**
   * Whether variant write shredding is enabled via config.
   */
  private final boolean variantWriteShreddingEnabled;

  /**
   * The effective (possibly shredded) Avro schema used for writing.
   */
  private final Schema effectiveAvroSchema;

  /**
   * Variant fields that need shredding, keyed by their index in the effective schema.
   * Empty if no shredding is needed.
   */
  private final Map<Integer, ShreddedVariantField> shreddedVariantFields;

  /**
   * Provider for variant shredding (loaded via reflection). Null if no shredding is needed.
   */
  private final VariantShreddingProvider shreddingProvider;

  public HoodieAvroWriteSupport(MessageType schema, HoodieSchema hoodieSchema, Option<BloomFilter> bloomFilterOpt,
                                Properties properties) {
    this(schema, hoodieSchema, generateEffectiveSchema(hoodieSchema, properties), bloomFilterOpt, properties);
  }

  private HoodieAvroWriteSupport(MessageType schema, HoodieSchema hoodieSchema, HoodieSchema effectiveSchema,
                                 Option<BloomFilter> bloomFilterOpt, Properties properties) {
    super(schema, effectiveSchema.toAvroSchema(), ConvertingGenericData.INSTANCE);
    this.bloomFilterWriteSupportOpt = bloomFilterOpt.map(HoodieBloomFilterAvroWriteSupport::new);
    this.properties = properties;
    String vectorMeta = HoodieSchema.buildVectorColumnsMetadataValue(hoodieSchema);
    if (!vectorMeta.isEmpty()) {
      footerMetadata.put(HoodieSchema.VECTOR_COLUMNS_METADATA_KEY, vectorMeta);
    }

    this.effectiveAvroSchema = effectiveSchema.toAvroSchema();
    this.variantWriteShreddingEnabled = Boolean.parseBoolean(
        properties.getProperty(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(),
            String.valueOf(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.defaultValue())));

    // When shredding is enabled, collect the variant fields that need shredding.
    Map<Integer, ShreddedVariantField> shreddedFields = new LinkedHashMap<>();

    if (effectiveSchema.getType() == HoodieSchemaType.RECORD) {
      List<HoodieSchemaField> fields = effectiveSchema.getFields();
      for (int i = 0; i < fields.size(); i++) {
        HoodieSchemaField field = fields.get(i);
        HoodieSchema fieldSchema = field.schema();
        // Unwrap nullable union to get the underlying type
        if (fieldSchema.isNullable()) {
          fieldSchema = fieldSchema.getNonNullType();
        }
        if (fieldSchema.getType() != HoodieSchemaType.VARIANT) {
          continue;
        }
        if (variantWriteShreddingEnabled) {
          HoodieSchema.Variant variant = (HoodieSchema.Variant) fieldSchema;
          if (variant.isShredded() && variant.getTypedValueField().isPresent()) {
            // Get the Avro sub-schema for this variant field from the effective schema
            Schema fieldAvroSchema = effectiveAvroSchema.getFields().get(i).schema();
            // Unwrap nullable union
            if (fieldAvroSchema.getType() == Schema.Type.UNION) {
              fieldAvroSchema = getNonNullFromUnion(fieldAvroSchema);
            }
            shreddedFields.put(i, new ShreddedVariantField(fieldAvroSchema, variant));
          }
        }
      }
    }

    this.shreddedVariantFields = shreddedFields;

    // Load shredding provider via reflection if needed
    if (!shreddedVariantFields.isEmpty()) {
      String providerClass = properties.getProperty(PARQUET_VARIANT_SHREDDING_PROVIDER_CLASS.key());
      if (providerClass == null || providerClass.isEmpty()) {
        throw new HoodieException("Variant write shredding is enabled and the write schema requires shredding "
            + "(typed_value columns present), but no VariantShreddingProvider is configured or available on the "
            + "classpath. Set " + PARQUET_VARIANT_SHREDDING_PROVIDER_CLASS.key() + " or add a provider "
            + "implementation (e.g. the Spark variant module) to the classpath.");
      }
      this.shreddingProvider = (VariantShreddingProvider) ReflectionUtils.loadClass(providerClass);
    } else {
      this.shreddingProvider = null;
    }
  }

  /**
   * Generates the effective schema for writing, applying variant shredding configuration.
   *
   * <p>When shredding is disabled, shredded variant fields are replaced with unshredded
   * variants (removing {@code typed_value}) so that the Parquet file does not contain
   * unused typed_value columns.</p>
   *
   * <p>When shredding is enabled and a forced shredding schema is configured via
   * {@link HoodieStorageConfig#PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST},
   * all variant fields are replaced with shredded variants using the forced schema.
   * This handles the case where the input schema is unshredded but shredding is desired.</p>
   *
   * <p>When shredding is enabled without a forced schema, the schema is returned as-is
   * (already-shredded variants stay shredded, unshredded variants stay unshredded).</p>
   *
   * @param hoodieSchema the original HoodieSchema
   * @param properties   writer properties containing shredding configuration
   * @return the effective schema to use for writing
   */
  public static HoodieSchema generateEffectiveSchema(HoodieSchema hoodieSchema, Properties properties) {
    boolean shreddingEnabled = Boolean.parseBoolean(
        properties.getProperty(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(),
            String.valueOf(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.defaultValue())));

    if (!shreddingEnabled) {
      // Schemas from clustering/compaction may still be shredded (read from on-disk Parquet files
      // written with shredding enabled), so we need to strip typed_value when shredding
      // is disabled.
      return VariantSchemaUtils.stripVariantShredding(hoodieSchema);
    }

    // Check if a forced shredding schema is configured
    String forceShreddingSchema = properties.getProperty(
        PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key());
    if (forceShreddingSchema != null && !forceShreddingSchema.isEmpty()) {
      return applyForcedShreddingSchema(hoodieSchema, forceShreddingSchema);
    }

    // When enabled without forced schema, use the schema as-is
    // (shredded variants stay shredded, unshredded variants stay unshredded)
    return hoodieSchema;
  }

  /**
   * Overloaded version accepting HoodieConfig for use by factories.
   */
  public static HoodieSchema generateEffectiveSchema(HoodieSchema hoodieSchema, HoodieConfig config) {
    return generateEffectiveSchema(hoodieSchema, config.getProps());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(T record) {
    if (!shreddedVariantFields.isEmpty() && shreddingProvider != null) {
      super.write((T) shredRecord((IndexedRecord) record));
    } else {
      super.write(record);
    }
  }

  /**
   * Builds a shredded copy of {@code inputRecord}: variant fields configured for shredding are
   * transformed via the {@link VariantShreddingProvider} to populate {@code typed_value}; all other
   * fields are copied across as-is.
   */
  private GenericRecord shredRecord(IndexedRecord inputRecord) {
    GenericRecord shreddedRecord = new GenericData.Record(effectiveAvroSchema);

    // Copy all fields, transforming variant fields that need shredding
    List<Schema.Field> effectiveFields = effectiveAvroSchema.getFields();
    Schema inputSchema = inputRecord.getSchema();

    for (int i = 0; i < effectiveFields.size(); i++) {
      Schema.Field effectiveField = effectiveFields.get(i);
      String fieldName = effectiveField.name();
      Schema.Field inputField = inputSchema.getField(fieldName);
      if (inputField == null) {
        continue;
      }

      ShreddedVariantField shreddedField = shreddedVariantFields.get(i);
      if (shreddedField != null) {
        // This is a shredded variant field - transform it
        Object fieldValue = inputRecord.get(inputField.pos());
        if (fieldValue instanceof GenericRecord) {
          GenericRecord variantRecord = (GenericRecord) fieldValue;
          GenericRecord shreddedVariant = shreddingProvider.shredVariantRecord(
              variantRecord,
              shreddedField.avroSchema,
              shreddedField.hoodieSchema);
          shreddedRecord.put(i, shreddedVariant);
        } else {
          // Null or unexpected type - copy as-is
          shreddedRecord.put(i, fieldValue);
        }
      } else {
        // Non-variant field - copy as-is
        shreddedRecord.put(i, inputRecord.get(inputField.pos()));
      }
    }

    return shreddedRecord;
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    Map<String, String> extraMetadata =
        CollectionUtils.combine(footerMetadata,
            bloomFilterWriteSupportOpt.map(HoodieBloomFilterWriteSupport::finalizeMetadata)
                .orElse(Collections.emptyMap())
        );

    return new WriteSupport.FinalizedWriteContext(extraMetadata);
  }

  public void add(String recordKey) {
    this.bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport ->
        bloomFilterWriteSupport.addKey(recordKey));
  }

  public void addFooterMetadata(String key, String value) {
    footerMetadata.put(key, value);
  }

  /**
   * Bundles the Avro sub-schema and {@link HoodieSchema.Variant} for a shredded variant field,
   * keyed by effective-schema field index in {@link #shreddedVariantFields}.
   */
  private static final class ShreddedVariantField {
    private final Schema avroSchema;
    private final HoodieSchema.Variant hoodieSchema;

    ShreddedVariantField(Schema avroSchema, HoodieSchema.Variant hoodieSchema) {
      this.avroSchema = avroSchema;
      this.hoodieSchema = hoodieSchema;
    }
  }

  private static final Pattern DECIMAL_PATTERN = Pattern.compile(
      "decimal\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)");

  /**
   * Applies a forced shredding schema to all variant fields in the given schema.
   * The forced schema DDL (e.g., {@code "a int, b string"}) defines the typed_value
   * fields that will be added to each variant column.
   */
  private static HoodieSchema applyForcedShreddingSchema(HoodieSchema schema, String ddl) {
    if (schema.getType() != HoodieSchemaType.RECORD) {
      return schema;
    }

    Map<String, HoodieSchema> shreddedFields = parseShreddingDDL(ddl);

    List<HoodieSchemaField> fields = schema.getFields();
    List<HoodieSchemaField> newFields = new ArrayList<>();
    boolean changed = false;

    for (HoodieSchemaField field : fields) {
      HoodieSchema fieldSchema = field.schema();
      boolean wasNullable = fieldSchema.isNullable();
      HoodieSchema unwrapped = wasNullable ? fieldSchema.getNonNullType() : fieldSchema;

      if (unwrapped.getType() == HoodieSchemaType.VARIANT) {
        HoodieSchema.Variant shreddedVariant = HoodieSchema.createVariantShreddedObject(
            unwrapped.getAvroSchema().getName(),
            unwrapped.getAvroSchema().getNamespace(),
            unwrapped.getAvroSchema().getDoc(),
            shreddedFields);
        HoodieSchema replacement = wasNullable
            ? HoodieSchema.createNullable(shreddedVariant) : shreddedVariant;
        // replacement already mirrors the field's original nullability, so withSchema alone suffices.
        newFields.add(HoodieSchemaUtils.createNewSchemaField(field.withSchema(replacement)));
        changed = true;
      } else {
        newFields.add(HoodieSchemaUtils.createNewSchemaField(field));
      }
    }

    if (!changed) {
      return schema;
    }

    return HoodieSchema.createRecord(
        schema.getAvroSchema().getName(),
        schema.getAvroSchema().getNamespace(),
        schema.getAvroSchema().getDoc(),
        newFields);
  }

  /**
   * Parses a DDL-style shredding schema string (e.g., {@code "a int, b string, c decimal(15,1)"})
   * into a map of field names to their HoodieSchema types.
   */
  private static Map<String, HoodieSchema> parseShreddingDDL(String ddl) {
    Map<String, HoodieSchema> fields = new LinkedHashMap<>();
    // Split on top-level commas only so parameterized types such as decimal(15, 1) survive intact.
    for (String fieldDef : StringUtils.splitTopLevelCommas(ddl)) {
      String[] parts = fieldDef.split("\\s+", 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException(
            "Invalid shredding DDL field definition (expected 'name type'): " + fieldDef);
      }
      fields.put(parts[0].trim(), parseSimpleType(parts[1].trim()));
    }
    return fields;
  }

  /**
   * Parses a simple type name into a HoodieSchema.
   * Supports common types: int, long, string, double, float, boolean, binary, decimal(p,s).
   */
  private static HoodieSchema parseSimpleType(String type) {
    String lower = type.toLowerCase();
    switch (lower) {
      case "int":
      case "integer":
        return HoodieSchema.create(HoodieSchemaType.INT);
      case "long":
      case "bigint":
        return HoodieSchema.create(HoodieSchemaType.LONG);
      case "string":
        return HoodieSchema.create(HoodieSchemaType.STRING);
      case "double":
        return HoodieSchema.create(HoodieSchemaType.DOUBLE);
      case "float":
        return HoodieSchema.create(HoodieSchemaType.FLOAT);
      case "boolean":
        return HoodieSchema.create(HoodieSchemaType.BOOLEAN);
      case "binary":
        return HoodieSchema.create(HoodieSchemaType.BYTES);
      default:
        Matcher m = DECIMAL_PATTERN.matcher(lower);
        if (m.matches()) {
          return HoodieSchema.createDecimal(
              Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
        }
        throw new IllegalArgumentException("Unsupported shredding type: " + type);
    }
  }

  /**
   * Extracts the non-null type from a union schema.
   */
  private static Schema getNonNullFromUnion(Schema unionSchema) {
    for (Schema type : unionSchema.getTypes()) {
      if (type.getType() != Schema.Type.NULL) {
        return type;
      }
    }
    throw new IllegalArgumentException("Union schema does not contain a non-null type: " + unionSchema);
  }

  private static class HoodieBloomFilterAvroWriteSupport extends HoodieBloomFilterWriteSupport<String> {
    public HoodieBloomFilterAvroWriteSupport(BloomFilter bloomFilter) {
      super(bloomFilter);
    }

    @Override
    protected byte[] getUTF8Bytes(String key) {
      return StringUtils.getUTF8Bytes(key);
    }
  }
}