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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.common.avro.VariantSchemaUtils;
import org.apache.hudi.common.avro.VariantShreddingProvider;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Reconstructs unshredded variants when reading an already-shredded base file on the Avro
 * ({@code HoodieRecordType.AVRO}) read path.
 *
 * <p>parquet-avro does not understand variant shredding, so a shredded variant column comes back as
 * a raw {@code {metadata, value, typed_value}} record. This reads such columns at their shredded
 * (typed_value-bearing) schema, then rebuilds the standard unshredded {@code {metadata, value}}
 * variant via {@link VariantShreddingProvider#rebuildVariantRecord} before records reach the
 * merger/writer. The Spark/InternalRow read path reconstructs natively and does not use this.
 *
 * <p>See https://github.com/apache/hudi/issues/18931.
 */
final class HoodieVariantReconstruction {

  private final HoodieSchema intermediateSchema;
  private final Schema outputAvroSchema;
  private final VariantShreddingProvider provider;
  // Indexed by field position in the (requested == output) record. For target fields, the file's
  // shredded sub-schema and the unshredded target sub-schema for rebuild; null for non-targets.
  private final boolean[] isTarget;
  private final Schema[] shreddedSubSchemas;
  private final Schema[] unshreddedSubSchemas;

  private HoodieVariantReconstruction(HoodieSchema intermediateSchema, Schema outputAvroSchema,
                                VariantShreddingProvider provider, boolean[] isTarget,
                                Schema[] shreddedSubSchemas, Schema[] unshreddedSubSchemas) {
    this.intermediateSchema = intermediateSchema;
    this.outputAvroSchema = outputAvroSchema;
    this.provider = provider;
    this.isTarget = isTarget;
    this.shreddedSubSchemas = shreddedSubSchemas;
    this.unshreddedSubSchemas = unshreddedSubSchemas;
  }

  /**
   * Schema to read the parquet file with: the requested schema, but with shredded variant columns
   * swapped to their file (typed_value-bearing) form so parquet-avro materializes {@code typed_value}.
   */
  HoodieSchema intermediateSchema() {
    return intermediateSchema;
  }

  /**
   * Builds a reconstruction for the given file and requested schemas, or returns {@code null} when
   * none is needed (the file has no shredded variant columns). Throws when the file has shredded
   * variant columns to reconstruct but reading shredded variants is disabled, or no provider is
   * available: either way, reading at the unshredded schema would silently drop the typed_value payload.
   */
  static HoodieVariantReconstruction create(HoodieSchema fileSchema, HoodieSchema requestedSchema, HoodieStorage storage) {
    if (requestedSchema.getType() != HoodieSchemaType.RECORD || fileSchema.getType() != HoodieSchemaType.RECORD) {
      return null;
    }

    List<HoodieSchemaField> requestedFields = requestedSchema.getFields();
    List<HoodieSchemaField> intermediateFields = new ArrayList<>();
    boolean[] isTarget = new boolean[requestedFields.size()];
    boolean anyTarget = false;
    for (int i = 0; i < requestedFields.size(); i++) {
      HoodieSchemaField requestedField = requestedFields.get(i);
      Option<HoodieSchemaField> fileField = fileSchema.getField(requestedField.name());
      if (fileField.isPresent() && isShreddedVariant(fileField.get().schema())) {
        isTarget[i] = true;
        anyTarget = true;
        // Read this column in its on-disk shredded shape.
        intermediateFields.add(requestedField.withSchema(fileField.get().schema()));
      } else {
        // Copy non-target fields too (withSchema makes a fresh Avro Field): reusing the requested
        // field's Avro Field, already bound to the requested record, would fail Schema.setFields with
        // "Field already used" when building the intermediate record below.
        intermediateFields.add(requestedField.withSchema(requestedField.schema()));
      }
    }
    if (!anyTarget) {
      // No shredded variant columns in the file: nothing to reconstruct, regardless of the flag.
      return null;
    }

    if (!storage.getConf().getBoolean(HoodieStorageConfig.PARQUET_VARIANT_ALLOW_READING_SHREDDED.key(),
        HoodieStorageConfig.PARQUET_VARIANT_ALLOW_READING_SHREDDED.defaultValue())) {
      // Reading at the unshredded schema would drop typed_value and silently corrupt variants whose
      // payload lives there, so fail fast. Mirrors the no-provider branch and Spark's
      // allowReadingShredded=false, which rejects shredded reads rather than discarding data.
      throw new HoodieException("Base file has shredded variant column(s) but reading shredded variants is "
          + "disabled (" + HoodieStorageConfig.PARQUET_VARIANT_ALLOW_READING_SHREDDED.key()
          + "=false). Enable it to reconstruct them; otherwise the typed_value payload would be silently dropped.");
    }

    VariantShreddingProvider provider = loadProvider(storage);
    if (provider == null) {
      // Reading would drop typed_value and silently corrupt variants whose payload lives there, so fail fast.
      throw new HoodieException("Base file has shredded variant column(s) and reading shredded variants is "
          + "enabled, but no VariantShreddingProvider is available to reconstruct them. Set "
          + HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_PROVIDER_CLASS.key()
          + " or add a provider implementation (e.g. the Spark variant module) to the classpath.");
    }

    HoodieSchema intermediateSchema = HoodieSchema.createRecord(
        requestedSchema.getAvroSchema().getName(),
        requestedSchema.getAvroSchema().getNamespace(),
        requestedSchema.getAvroSchema().getDoc(),
        intermediateFields);
    // Records leave this reader unshredded; output field order matches the requested/intermediate order.
    HoodieSchema outputSchema = VariantSchemaUtils.stripVariantShredding(requestedSchema);

    Schema[] shreddedSubSchemas = new Schema[requestedFields.size()];
    Schema[] unshreddedSubSchemas = new Schema[requestedFields.size()];
    for (int i = 0; i < requestedFields.size(); i++) {
      if (isTarget[i]) {
        shreddedSubSchemas[i] = fileSchema.getField(requestedFields.get(i).name()).get().schema().getNonNullType().getAvroSchema();
        unshreddedSubSchemas[i] = outputSchema.getFields().get(i).schema().getNonNullType().getAvroSchema();
      }
    }

    return new HoodieVariantReconstruction(intermediateSchema, outputSchema.toAvroSchema(), provider,
        isTarget, shreddedSubSchemas, unshreddedSubSchemas);
  }

  /**
   * Rebuilds shredded variant columns of {@code in} (read in the intermediate shredded shape) into
   * a record conforming to the unshredded output schema.
   */
  IndexedRecord reconstruct(IndexedRecord in) {
    GenericRecord out = new GenericData.Record(outputAvroSchema);
    for (int i = 0; i < isTarget.length; i++) {
      Object value = in.get(i);
      if (isTarget[i] && value instanceof GenericRecord) {
        out.put(i, provider.rebuildVariantRecord((GenericRecord) value, shreddedSubSchemas[i], unshreddedSubSchemas[i]));
      } else {
        // Non-variant column, or a null variant column: pass through unchanged.
        out.put(i, value);
      }
    }
    return out;
  }

  private static boolean isShreddedVariant(HoodieSchema schema) {
    HoodieSchema unwrapped = schema.getNonNullType();
    return unwrapped.getType() == HoodieSchemaType.VARIANT
        && ((HoodieSchema.Variant) unwrapped).isShredded();
  }

  private static VariantShreddingProvider loadProvider(HoodieStorage storage) {
    String providerClass = storage.getConf()
        .getString(HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_PROVIDER_CLASS.key()).orElse(null);
    if (providerClass == null || providerClass.isEmpty()) {
      providerClass = VariantShreddingProvider.detectProviderClassOnClasspath();
    }
    return providerClass == null ? null : (VariantShreddingProvider) ReflectionUtils.loadClass(providerClass);
  }
}
