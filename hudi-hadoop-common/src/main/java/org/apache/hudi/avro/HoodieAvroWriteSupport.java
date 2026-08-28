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

import java.util.Collection;
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
 * by parsing variant binary data using a {@link VariantShreddingProvider} loaded via reflection.
 * A variant is transformed wherever the effective schema declares {@code typed_value} - a record
 * field at any depth, an array element or a map value - mirroring the row write path.</p>
 */
public class HoodieAvroWriteSupport<T> extends AvroWriteSupport<T> {

  private final Option<HoodieBloomFilterWriteSupport<String>> bloomFilterWriteSupportOpt;
  private final Map<String, String> footerMetadata = new HashMap<>();
  protected final Properties properties;

  /**
   * Plans the value-level transform for the whole record, or is null when the effective schema
   * declares no shredded variant anywhere and records can be written untouched.
   */
  private final Shredder rootShredder;

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

    boolean shreddingEnabled = Boolean.parseBoolean(
        properties.getProperty(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.key(),
            String.valueOf(PARQUET_VARIANT_WRITE_SHREDDING_ENABLED.defaultValue())));

    // When shredding is enabled, plan the transform for every position the effective schema
    // declares shredded; null means there is none and records pass through untouched.
    this.rootShredder = shreddingEnabled ? buildShredder(effectiveSchema) : null;

    // Load shredding provider via reflection if needed. A schema that is shredded only below the
    // top level needs it just as much, so this is keyed off the whole tree, not the root fields.
    if (rootShredder != null) {
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
   * {@link HoodieStorageConfig#PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST}, every variant
   * that is a record member - at the top level or at any depth, including records nested under
   * arrays and maps - is replaced with a shredded variant using the forced schema. This handles
   * the case where the input schema is unshredded but shredding is desired. A variant that is
   * directly an array element or a map value is left alone, mirroring the row write path; such a
   * position only shreds when the write schema itself declares {@code typed_value} there.</p>
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
    super.write(rootShredder == null ? record : (T) rootShredder.shred(record, shreddingProvider));
  }

  /**
   * Plans the shredding transform for one position of the effective schema, or returns {@code null}
   * when nothing below it is a shredded variant and the value can be copied by reference. The
   * mirror image of {@code HoodieVariantReconstruction.buildRebuilder} on the read side.
   *
   * <p>Walking values (and not just splicing the schema) is mandatory: {@link AvroWriteSupport}
   * serializes through {@link ConvertingGenericData}, which inherits the positional
   * {@code GenericData.getField}, so a {@code typed_value} spliced into a nested variant while the
   * record below stays the untransformed {@code {metadata, value}} would have parquet read index 2
   * off a 2-field record and fail the write.</p>
   */
  private static Shredder buildShredder(HoodieSchema effective) {
    HoodieSchema unwrapped = effective.getNonNullType();
    switch (unwrapped.getType()) {
      case VARIANT: {
        HoodieSchema.Variant variant = (HoodieSchema.Variant) unwrapped;
        return variant.isShredded() && variant.getTypedValueField().isPresent()
            ? new VariantShredder(unwrapped.getAvroSchema(), variant) : null;
      }
      case RECORD: {
        List<HoodieSchemaField> fields = unwrapped.getFields();
        String[] fieldNames = new String[fields.size()];
        Shredder[] fieldShredders = new Shredder[fields.size()];
        boolean anyShredded = false;
        for (int i = 0; i < fields.size(); i++) {
          fieldNames[i] = fields.get(i).name();
          fieldShredders[i] = buildShredder(fields.get(i).schema());
          anyShredded |= fieldShredders[i] != null;
        }
        return anyShredded
            ? new RecordShredder(unwrapped.getAvroSchema(), fieldNames, fieldShredders) : null;
      }
      case ARRAY: {
        Shredder elementShredder = buildShredder(unwrapped.getElementType());
        return elementShredder == null
            ? null : new ArrayShredder(unwrapped.getAvroSchema(), elementShredder);
      }
      case MAP: {
        Shredder valueShredder = buildShredder(unwrapped.getValueType());
        return valueShredder == null ? null : new MapShredder(valueShredder);
      }
      default:
        return null;
    }
  }

  /** Transforms one value into the shape the effective schema declares for its position. */
  private interface Shredder {
    Object shred(Object value, VariantShreddingProvider provider);
  }

  private static final class VariantShredder implements Shredder {
    private final Schema shreddedAvroSchema;
    private final HoodieSchema.Variant variant;

    private VariantShredder(Schema shreddedAvroSchema, HoodieSchema.Variant variant) {
      this.shreddedAvroSchema = shreddedAvroSchema;
      this.variant = variant;
    }

    @Override
    public Object shred(Object value, VariantShreddingProvider provider) {
      // A null variant, or an unexpected type, passes through unchanged.
      return value instanceof GenericRecord
          ? provider.shredVariantRecord((GenericRecord) value, shreddedAvroSchema, variant)
          : value;
    }
  }

  private static final class RecordShredder implements Shredder {
    private final Schema effectiveSchema;
    private final String[] fieldNames;
    // Indexed by field position in the effective record; null for fields copied by reference.
    private final Shredder[] fieldShredders;
    // Input records are not rewritten before they reach the writer (HoodieAvroIndexedRecord passes
    // them through) and nested records carry their own schema, so fields are matched by NAME.
    // One writer sees one input schema instance per level, so a single-entry cache resolves the
    // name lookups once; the write support is single-threaded.
    private Schema cachedInputSchema;
    private int[] cachedInputPositions;

    private RecordShredder(Schema effectiveSchema, String[] fieldNames, Shredder[] fieldShredders) {
      this.effectiveSchema = effectiveSchema;
      this.fieldNames = fieldNames;
      this.fieldShredders = fieldShredders;
    }

    @Override
    public Object shred(Object value, VariantShreddingProvider provider) {
      if (!(value instanceof IndexedRecord)) {
        return value;
      }
      IndexedRecord in = (IndexedRecord) value;
      int[] inputPositions = inputPositionsFor(in.getSchema());
      GenericRecord out = new GenericData.Record(effectiveSchema);
      for (int i = 0; i < inputPositions.length; i++) {
        if (inputPositions[i] < 0) {
          // The input does not carry this field; leave it null rather than guessing a position.
          continue;
        }
        Object fieldValue = in.get(inputPositions[i]);
        out.put(i, fieldShredders[i] == null
            ? fieldValue : fieldShredders[i].shred(fieldValue, provider));
      }
      return out;
    }

    private int[] inputPositionsFor(Schema inputSchema) {
      if (inputSchema == cachedInputSchema) {
        return cachedInputPositions;
      }
      int[] inputPositions = new int[fieldNames.length];
      for (int i = 0; i < fieldNames.length; i++) {
        Schema.Field inputField = inputSchema.getField(fieldNames[i]);
        inputPositions[i] = inputField == null ? -1 : inputField.pos();
      }
      cachedInputSchema = inputSchema;
      cachedInputPositions = inputPositions;
      return inputPositions;
    }
  }

  private static final class ArrayShredder implements Shredder {
    private final Schema effectiveSchema;
    private final Shredder elementShredder;

    private ArrayShredder(Schema effectiveSchema, Shredder elementShredder) {
      this.effectiveSchema = effectiveSchema;
      this.elementShredder = elementShredder;
    }

    @Override
    public Object shred(Object value, VariantShreddingProvider provider) {
      if (!(value instanceof Collection)) {
        return value;
      }
      Collection<?> in = (Collection<?>) value;
      GenericData.Array<Object> out = new GenericData.Array<>(in.size(), effectiveSchema);
      for (Object element : in) {
        out.add(elementShredder.shred(element, provider));
      }
      return out;
    }
  }

  private static final class MapShredder implements Shredder {
    private final Shredder valueShredder;

    private MapShredder(Shredder valueShredder) {
      this.valueShredder = valueShredder;
    }

    @Override
    public Object shred(Object value, VariantShreddingProvider provider) {
      if (!(value instanceof Map)) {
        return value;
      }
      Map<?, ?> in = (Map<?, ?>) value;
      Map<Object, Object> out = new LinkedHashMap<>(in.size());
      for (Map.Entry<?, ?> entry : in.entrySet()) {
        out.put(entry.getKey(), valueShredder.shred(entry.getValue(), provider));
      }
      return out;
    }
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

  private static final Pattern DECIMAL_PATTERN = Pattern.compile(
      "decimal\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)");

  /**
   * Applies a forced shredding schema to the variant record members of the given schema, at any
   * depth. The forced schema DDL (e.g., {@code "a int, b string"}) defines the typed_value
   * fields that will be added to each variant column.
   */
  private static HoodieSchema applyForcedShreddingSchema(HoodieSchema schema, String ddl) {
    return VariantSchemaUtils.applyForcedShredding(schema, parseShreddingDDL(ddl));
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