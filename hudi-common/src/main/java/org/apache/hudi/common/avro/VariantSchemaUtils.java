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
}
