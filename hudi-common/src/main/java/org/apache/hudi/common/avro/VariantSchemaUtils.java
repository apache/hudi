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

package org.apache.hudi.common.avro;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared helpers for converting between shredded and unshredded variant schemas.
 * Used by both the write path ({@code HoodieAvroWriteSupport}) and the read path
 * (variant reconstruction in the parquet reader).
 */
public class VariantSchemaUtils {

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
          newFields.add(field.withSchema(replacement));
          changed = true;
          continue;
        }
      }
      newFields.add(field);
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
   * Whether this column sits shredded on disk and must be read in that shape and reconstructed to
   * serve the requested schema. A file schema that kept its variant logical type answers via
   * {@link HoodieSchema.Variant#isShredded()}; but a file schema derived from converting the
   * parquet footer MessageType loses the logical type (variant groups come back as plain records),
   * so the on-disk side is detected by SHAPE, anchored by the requested side: the requested column
   * (from the table schema, logical type intact) must be a variant for the shape match to count,
   * leaving plain user structs of the same shape alone (#19567).
   */
  public static boolean isShreddedVariantTarget(HoodieSchema requestedFieldSchema, HoodieSchema fileFieldSchema) {
    HoodieSchema file = fileFieldSchema.getNonNullType();
    if (file.getType() == HoodieSchemaType.VARIANT && ((HoodieSchema.Variant) file).isShredded()) {
      return true;
    }
    HoodieSchema requested = requestedFieldSchema.getNonNullType();
    return requested.getType() == HoodieSchemaType.VARIANT && isShreddedVariantShape(file);
  }

  /**
   * Returns {@code fileSchema} with each top-level shredded variant column (per
   * {@link #isShreddedVariantTarget}) replaced by its requested counterpart, for projection or
   * compatibility checks against {@code requestedSchema}. A footer-derived shredded variant column
   * surfaces as a plain {@code {metadata, value, typed_value}} record and so can never look like a
   * projection source of the requested variant, even though the readers reconstruct it (see
   * HoodieVariantReconstruction). Returns {@code fileSchema} as-is when nothing matches.
   */
  public static HoodieSchema alignShreddedVariants(HoodieSchema fileSchema, HoodieSchema requestedSchema) {
    if (fileSchema.getType() != HoodieSchemaType.RECORD || requestedSchema.getType() != HoodieSchemaType.RECORD) {
      return fileSchema;
    }
    List<HoodieSchemaField> newFields = new ArrayList<>();
    boolean changed = false;
    for (HoodieSchemaField fileField : fileSchema.getFields()) {
      Option<HoodieSchemaField> requestedField = requestedSchema.getField(fileField.name());
      if (requestedField.isPresent() && isShreddedVariantTarget(requestedField.get().schema(), fileField.schema())) {
        newFields.add(fileField.withSchema(requestedField.get().schema()));
        changed = true;
      } else {
        // Copy untouched fields too (withSchema makes a fresh Avro Field): reusing a field already
        // bound to the file's record would fail Schema.setFields with "Field already used" when
        // building the aligned record below.
        newFields.add(fileField.withSchema(fileField.schema()));
      }
    }
    if (!changed) {
      return fileSchema;
    }
    return HoodieSchema.createRecord(
        fileSchema.getAvroSchema().getName(),
        fileSchema.getAvroSchema().getNamespace(),
        fileSchema.getAvroSchema().getDoc(),
        newFields);
  }

  /** The on-disk shredded variant shape: a record of exactly {metadata: bytes, value: [nullable] bytes, typed_value}. */
  private static boolean isShreddedVariantShape(HoodieSchema schema) {
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
}
