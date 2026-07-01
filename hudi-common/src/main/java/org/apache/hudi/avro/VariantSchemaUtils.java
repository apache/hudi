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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Shared helpers for converting between shredded and unshredded variant schemas.
 * Used by both the write path ({@link HoodieAvroWriteSupport}) and the read path
 * (variant reconstruction in the parquet reader).
 */
public class VariantSchemaUtils {

  // Defined in hudi-client's HoodieWriteConfig; referenced literally because hudi-common cannot
  // depend on it (same precedent as "hoodie.index.type" in HoodieFileWriterFactory).
  private static final String INTERNAL_SCHEMA_KEY = "hoodie.internal.schema";
  private static final String WRITE_SCHEMA_OVERRIDE_KEY = "hoodie.write.schema";
  private static final String AVRO_SCHEMA_KEY = "hoodie.avro.schema";

  private VariantSchemaUtils() {
  }

  /**
   * Strips shredding from top-level variant fields in {@code schema}, replacing each shredded
   * variant with its unshredded form (dropping {@code typed_value}). Non-variant fields and
   * already-unshredded variants pass through unchanged; returns {@code schema} as-is when nothing
   * changes.
   */
  public static HoodieSchema stripVariantShredding(HoodieSchema schema) {
    if (schema.getType() != HoodieSchemaType.RECORD) {
      return schema;
    }

    List<HoodieSchemaField> fields = schema.getFields();
    List<HoodieSchemaField> newFields = new ArrayList<>();
    boolean changed = false;

    for (HoodieSchemaField field : fields) {
      HoodieSchema fieldSchema = field.schema();
      boolean wasNullable = fieldSchema.isNullable();
      HoodieSchema unwrapped = wasNullable ? fieldSchema.getNonNullType() : fieldSchema;

      if (unwrapped.getType() == HoodieSchemaType.VARIANT) {
        HoodieSchema.Variant variant = (HoodieSchema.Variant) unwrapped;
        if (variant.isShredded()) {
          HoodieSchema.Variant unshredded = HoodieSchema.createVariant(
              unwrapped.getAvroSchema().getName(),
              unwrapped.getAvroSchema().getNamespace(),
              unwrapped.getAvroSchema().getDoc());
          HoodieSchema replacement = wasNullable ? HoodieSchema.createNullable(unshredded) : unshredded;
          // createNewSchemaField: the existing avro fields are attached to the source schema and
          // cannot be reused in a new record ("Field already used").
          newFields.add(HoodieSchemaUtils.createNewSchemaField(field.withSchema(replacement)));
          changed = true;
          continue;
        }
      }
      newFields.add(HoodieSchemaUtils.createNewSchemaField(field));
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
   * Strips {@code typed_value} from top-level fields that have the variant SHAPE but lost the
   * variant logical type, i.e. plain records of {@code {metadata: bytes, value: [nullable]
   * bytes, typed_value}}. Parquet-footer-derived schemas come back this way (the converter
   * does not attach the variant logical type), so {@link #stripVariantShredding} alone cannot
   * see them. Used by the table-schema footer fallback only; returns {@code schema} as-is when
   * nothing matches.
   */
  public static HoodieSchema stripVariantShreddingByShape(HoodieSchema schema) {
    if (schema.getType() != HoodieSchemaType.RECORD) {
      return schema;
    }

    List<HoodieSchemaField> newFields = new ArrayList<>();
    boolean changed = false;

    for (HoodieSchemaField field : schema.getFields()) {
      HoodieSchema fieldSchema = field.schema();
      boolean wasNullable = fieldSchema.isNullable();
      HoodieSchema unwrapped = wasNullable ? fieldSchema.getNonNullType() : fieldSchema;

      if (isShreddedVariantShape(unwrapped)) {
        List<HoodieSchemaField> strippedFields = new ArrayList<>();
        for (HoodieSchemaField member : unwrapped.getFields()) {
          if (!HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD.equals(member.name())) {
            strippedFields.add(HoodieSchemaUtils.createNewSchemaField(member));
          }
        }
        HoodieSchema stripped = HoodieSchema.createRecord(
            unwrapped.getAvroSchema().getName(),
            unwrapped.getAvroSchema().getNamespace(),
            unwrapped.getAvroSchema().getDoc(),
            strippedFields);
        HoodieSchema replacement = wasNullable ? HoodieSchema.createNullable(stripped) : stripped;
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
   * Whether {@code schema} has the on-disk shredded variant SHAPE: a record of exactly
   * {@code {metadata: bytes, value: [nullable] bytes, typed_value}}. Parquet-footer-derived
   * schemas lose the variant logical type (the converter falls back to a plain record), so
   * shape is the only signal that a file shreds a column; used by the reader-side
   * reconstruction and the footer-fallback schema strip.
   */
  public static boolean isShreddedVariantShape(HoodieSchema schema) {
    if (schema.getType() != HoodieSchemaType.RECORD || schema.getFields().size() != 3) {
      return false;
    }
    if (!schema.getField(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD).isPresent()) {
      return false;
    }
    return isBytesField(schema, HoodieSchema.Variant.VARIANT_METADATA_FIELD)
        && isBytesField(schema, HoodieSchema.Variant.VARIANT_VALUE_FIELD);
  }

  private static boolean isBytesField(HoodieSchema schema, String fieldName) {
    return schema.getField(fieldName)
        .map(HoodieSchemaField::schema)
        .map(s -> s.isNullable() ? s.getNonNullType() : s)
        .map(s -> s.getType() == HoodieSchemaType.BYTES)
        .orElse(false);
  }

  /**
   * The top-level variant columns of {@code schema} that are candidates for shredding-schema
   * inference under {@code config}, or an empty list when inference does not apply.
   *
   * <p>Inference applies only when it is enabled, write shredding is enabled, no forced test
   * DDL is set (force wins), and no internal schema is set (the Spark write support prefers the
   * internal schema and would silently ignore an inferred one). Candidate columns are top-level
   * unshredded variants; columns with an explicit typed_value keep their schema-driven shredding.</p>
   */
  public static List<String> getInferableVariantColumns(HoodieConfig config, HoodieSchema schema) {
    if (!config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_SCHEMA_INFERENCE_ENABLED)
        || !config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED)
        || !StringUtils.isNullOrEmpty(config.getString(HoodieStorageConfig.PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST))
        || !StringUtils.isNullOrEmpty(config.getString(INTERNAL_SCHEMA_KEY))
        || schema.getType() != HoodieSchemaType.RECORD) {
      return Collections.emptyList();
    }

    List<String> columns = new ArrayList<>();
    for (HoodieSchemaField field : schema.getFields()) {
      HoodieSchema unwrapped = field.schema().isNullable() ? field.schema().getNonNullType() : field.schema();
      if (unwrapped.getType() == HoodieSchemaType.VARIANT && !((HoodieSchema.Variant) unwrapped).isShredded()) {
        columns.add(field.name());
      }
    }
    return columns;
  }

  /**
   * Returns a copy of {@code config} with the inferred typed_value schemas spliced into its
   * schema-carrying keys ({@code hoodie.write.schema}, {@code hoodie.avro.schema}), or
   * {@code config} itself when there is nothing to splice. Used for write supports that resolve
   * their schema from the config rather than a schema argument (the Spark row write support);
   * the original config is never mutated.
   */
  public static HoodieConfig applyInferredShreddingToConfig(HoodieConfig config, Map<String, HoodieSchema> typedValueByField) {
    if (typedValueByField.isEmpty()) {
      return config;
    }
    HoodieConfig copy = new HoodieConfig(TypedProperties.copy(config.getProps()));
    for (String key : new String[] {WRITE_SCHEMA_OVERRIDE_KEY, AVRO_SCHEMA_KEY}) {
      String schemaString = copy.getString(key);
      if (!StringUtils.isNullOrEmpty(schemaString)) {
        HoodieSchema spliced = applyInferredShredding(HoodieSchema.parse(schemaString), typedValueByField);
        copy.setValue(key, spliced.getAvroSchema().toString());
      }
    }
    return copy;
  }

  /**
   * Splices inferred typed_value schemas into the matching top-level variant fields of
   * {@code schema}; the inverse direction of {@link #stripVariantShredding}. Only unshredded
   * variant fields with an entry in {@code typedValueByField} are replaced; everything else
   * passes through unchanged, and {@code schema} is returned as-is when nothing matches.
   *
   * @param schema            the writer schema
   * @param typedValueByField column name to typed_value schema in the nested shredding-spec
   *                          form, as produced by a {@link VariantShreddingSchemaInferrer}
   */
  public static HoodieSchema applyInferredShredding(HoodieSchema schema, Map<String, HoodieSchema> typedValueByField) {
    if (schema.getType() != HoodieSchemaType.RECORD || typedValueByField.isEmpty()) {
      return schema;
    }

    List<HoodieSchemaField> newFields = new ArrayList<>();
    boolean changed = false;

    for (HoodieSchemaField field : schema.getFields()) {
      HoodieSchema fieldSchema = field.schema();
      boolean wasNullable = fieldSchema.isNullable();
      HoodieSchema unwrapped = wasNullable ? fieldSchema.getNonNullType() : fieldSchema;
      HoodieSchema typedValue = typedValueByField.get(field.name());

      if (typedValue != null
          && unwrapped.getType() == HoodieSchemaType.VARIANT
          && !((HoodieSchema.Variant) unwrapped).isShredded()) {
        // typed_value must be nullable: rows whose variant value does not match the inferred
        // schema are written with a null typed_value and the full binary in the value column.
        HoodieSchema nullableTypedValue = typedValue.isNullable() ? typedValue : HoodieSchema.createNullable(typedValue);
        // The shredded record needs a name unique to this column: variant columns commonly share
        // one record type (named "variant"), which Avro serializes as name references after the
        // first occurrence. Reusing the shared name would alias every other variant column to
        // this column's shredded definition once the schema is serialized (the config-splice
        // path), and two inferred columns could not coexist in one schema.
        HoodieSchema.Variant shredded = HoodieSchema.createVariantShredded(
            field.name() + "_variant",
            unwrapped.getAvroSchema().getNamespace(),
            unwrapped.getAvroSchema().getDoc(),
            nullableTypedValue);
        HoodieSchema replacement = wasNullable ? HoodieSchema.createNullable(shredded) : shredded;
        // createNewSchemaField: the existing avro fields are attached to the source schema and
        // cannot be reused in a new record ("Field already used").
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
}
