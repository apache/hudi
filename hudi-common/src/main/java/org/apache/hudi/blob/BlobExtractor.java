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

package org.apache.hudi.blob;

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.Option;

import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public class BlobExtractor {
  private static final BlobExtractor INSTANCE = new BlobExtractor();

  public static BlobExtractor getInstance() {
    return INSTANCE;
  }

  /**
   * Finds all the blob columns and returns a schema limited to these columns.
   * This is used to read only the blob columns when finding the blob files to clean.
   * @param schema the table's schema
   * @return an option of the reduced schema containing only the blob columns or empty option if no blobs are found
   */
  public Option<HoodieSchema> getReducedBlobSchema(HoodieSchema schema) {
    List<HoodieSchemaField> blobFields = new ArrayList<>();

    // Traverse schema to find blob columns
    for (HoodieSchemaField field : schema.getFields()) {
      filterSchemaForBlobFields(field, blobFields);
    }

    // Create projection schema with only blob fields
    return blobFields.isEmpty()
        ? Option.empty()
        : Option.of(HoodieSchema.createRecord(
        schema.getName(),
        schema.getNamespace().orElse(null),
        schema.getDoc().orElse(null),
        blobFields));
  }

  private void filterSchemaForBlobFields(HoodieSchemaField field,
                                         List<HoodieSchemaField> blobFields) {
    HoodieSchema fieldSchema = field.schema();
    HoodieSchema nonNullSchema = fieldSchema.getNonNullType();

    switch (nonNullSchema.getType()) {
      case BLOB:
        blobFields.add(HoodieSchemaUtils.createNewSchemaField(field));
        break;
      case RECORD:
        // Recursively traverse nested record fields
        List<HoodieSchemaField> nestedBlobFields = new ArrayList<>();
        for (HoodieSchemaField nestedField : nonNullSchema.getFields()) {
          filterSchemaForBlobFields(nestedField, nestedBlobFields);
        }

        // If any nested field contains blob, include this record field
        if (!nestedBlobFields.isEmpty()) {
          HoodieSchema nestedRecordSchema = HoodieSchema.createRecord(
              nonNullSchema.getName(),
              nonNullSchema.getNamespace().orElse(null),
              nonNullSchema.getDoc().orElse(null),
              nestedBlobFields);

          HoodieSchema finalSchema = fieldSchema.isNullable()
              ? HoodieSchema.createNullable(nestedRecordSchema)
              : nestedRecordSchema;

          blobFields.add(HoodieSchemaUtils.createNewSchemaField(
              field.name(), finalSchema, field.doc().orElse(null), field.defaultVal().orElse(null)));
        }
        break;
      case ARRAY:
        // Check if array element type contains blob
        HoodieSchema elementType = nonNullSchema.getElementType();
        if (elementType.containsBlobType()) {
          blobFields.add(HoodieSchemaUtils.createNewSchemaField(field));
        }
        break;
      case MAP:
        // Check if map value type contains blob
        HoodieSchema valueType = nonNullSchema.getValueType();
        if (valueType.containsBlobType()) {
          blobFields.add(HoodieSchemaUtils.createNewSchemaField(field));
        }
        break;
      default:
        // No blob type, do nothing
        break;
    }
  }

  /**
   * Finds the blob file paths referenced by a record. The schema is used to find all blob columns in the record.
   * Any blob column that has an external reference with isManaged=true will be included in the result.
   * @param schema the record schema
   * @param record the record to inspect
   * @param recordContext the record context to use for retrieving values from the record
   * @return a list of managed blob file paths referenced at this path
   * @param <R> the record type
   */
  public <R> List<String> getManagedBlobPaths(HoodieSchema schema, R record, RecordContext<R> recordContext) {
    List<String> managedPaths = new ArrayList<>();

    for (int i = 0; i < schema.getFields().size(); i++) {
      HoodieSchemaField field = schema.getFields().get(i);
      HoodieSchema fieldSchema = field.schema().getNonNullType();
      Object value = recordContext.getValue(record, schema, field.name());
      managedPaths.addAll(getManagedBlobPathsForField(value, fieldSchema, recordContext));
    }
    return managedPaths;
  }

  private <R> List<String> getManagedBlobPathsForField(Object value, HoodieSchema fieldSchema, RecordContext<R> recordContext) {
    if (value == null) {
      return Collections.emptyList();
    }
    List<String> managedPaths = new ArrayList<>();
    switch (fieldSchema.getType()) {
      case BLOB:
        // Process blob field
        extractManagedPathFromBlob((R) value, fieldSchema, recordContext).ifPresent(managedPaths::add);
        break;
      case RECORD:
        managedPaths.addAll(getManagedBlobPaths(fieldSchema, (R) value, recordContext));
        break;
      case ARRAY:
        if (value instanceof Iterable) {
          for (Object element : (Iterable<?>) value) {
            if (element != null) {
              managedPaths.addAll(getManagedBlobPathsForField(element, fieldSchema.getElementType().getNonNullType(), recordContext));
            }
          }
        }
        break;
      case MAP:
        if (value instanceof Map) {
          for (Object entry : ((Map<?, ?>) value).values()) {
            if (entry != null) {
              managedPaths.addAll(getManagedBlobPathsForField(entry, fieldSchema.getValueType().getNonNullType(), recordContext));
            }
          }
        }
        break;
      default:
        // No blob type, skip
    }
    return managedPaths;
  }

  private <R> Option<String> extractManagedPathFromBlob(R blobRecord, HoodieSchema blobSchema, RecordContext<R> recordContext) {
    // Handle null values
    if (blobRecord == null) {
      return Option.empty();
    }

    // Extract the "type" field to check if blob is OUT_OF_LINE
    Object typeValue = recordContext.getValue(blobRecord, blobSchema, HoodieSchema.Blob.TYPE);
    if (!HoodieSchema.Blob.OUT_OF_LINE.equalsIgnoreCase(typeValue.toString())) {
      // Inline blob or invalid type, no external file
      return Option.empty();
    }

    // Check if this is a managed reference
    boolean isManaged = (boolean) recordContext.getValue(blobRecord, blobSchema, HoodieSchema.Blob.EXTERNAL_REFERENCE + "." + HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED);
    if (!isManaged) {
      // Unmanaged blob, don't delete
      return Option.empty();
    }

    String path = recordContext.getValue(blobRecord, blobSchema, HoodieSchema.Blob.EXTERNAL_REFERENCE + "." + HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH).toString();
    return Option.ofNullable(path);
  }
}
