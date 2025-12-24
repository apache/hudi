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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class to assist with merging two versions of the record that may contain partial updates using
 * {@link org.apache.hudi.common.table.PartialUpdateMode#KEEP_VALUES} mode.
 */
public class KeepValuesPartialMergingUtils<T> {
  static KeepValuesPartialMergingUtils INSTANCE = new KeepValuesPartialMergingUtils();
  private static final Map<HoodieSchema, Map<String, Integer>>
      FIELD_NAME_TO_ID_MAPPING_CACHE = new ConcurrentHashMap<>();
  private static final Map<Pair<Pair<HoodieSchema, HoodieSchema>, HoodieSchema>, HoodieSchema>
      MERGED_SCHEMA_CACHE = new ConcurrentHashMap<>();

  /**
   * Merges records which can contain partial updates.
   *
   * @param lowOrderRecord  record with lower commit time or lower ordering value
   * @param lowOrderSchema  The schema of the older record
   * @param highOrderRecord record with higher commit time or higher ordering value
   * @param highOrderSchema The schema of highOrderRecord
   * @param newSchema       The schema of the new incoming record
   * @return The merged record and schema.
   */
  Pair<BufferedRecord<T>, HoodieSchema> mergePartialRecords(BufferedRecord<T> lowOrderRecord,
                                                             HoodieSchema lowOrderSchema,
                                                             BufferedRecord<T> highOrderRecord,
                                                             HoodieSchema highOrderSchema,
                                                             HoodieSchema newSchema,
                                                             RecordContext<T> recordContext) {
    // The merged schema contains fields that only appear in either older and/or newer record.
    HoodieSchema mergedSchema =
        getCachedMergedSchema(lowOrderSchema, highOrderSchema, newSchema);
    boolean isNewerPartial = isPartial(highOrderSchema, mergedSchema);
    if (!isNewerPartial) {
      return Pair.of(highOrderRecord, highOrderSchema);
    }
    Set<String> fieldNamesInNewRecord =
        getCachedFieldNameToIdMapping(highOrderSchema).keySet();
    // Collect field values.
    List<HoodieSchemaField> fields = mergedSchema.getFields();
    Object[] fieldVals = new Object[fields.size()];
    int idx = 0;
    List<HoodieSchemaField> mergedSchemaFields = mergedSchema.getFields();
    for (HoodieSchemaField mergedSchemaField : mergedSchemaFields) {
      String fieldName = mergedSchemaField.name();
      if (fieldNamesInNewRecord.contains(fieldName)) { // field present in newer record.
        fieldVals[idx++] = recordContext.getValue(highOrderRecord.getRecord(), highOrderSchema, fieldName);
      } else { // if not present in newer record pick from old record
        fieldVals[idx++] = recordContext.getValue(lowOrderRecord.getRecord(), lowOrderSchema, fieldName);
      }
    }
    // Build merged record.
    T engineRecord = recordContext.constructEngineRecord(mergedSchema, fieldVals);
    BufferedRecord<T> mergedRecord = new BufferedRecord<>(
        highOrderRecord.getRecordKey(),
        highOrderRecord.getOrderingValue(),
        engineRecord,
        recordContext.encodeSchema(mergedSchema),
        highOrderRecord.getHoodieOperation());
    return Pair.of(mergedRecord, mergedSchema);
  }

  /**
   * @param hoodieSchema Hoodie schema.
   * @return The field name to ID mapping.
   */
  static Map<String, Integer> getCachedFieldNameToIdMapping(HoodieSchema hoodieSchema) {
    return FIELD_NAME_TO_ID_MAPPING_CACHE.computeIfAbsent(hoodieSchema, schema -> {
      Map<String, Integer> schemaFieldIdMapping = new HashMap<>();
      int fieldId = 0;
      for (HoodieSchemaField field : schema.getFields()) {
        schemaFieldIdMapping.put(field.name(), fieldId);
        fieldId++;
      }
      return schemaFieldIdMapping;
    });
  }

  /**
   * Merges the two schemas so the merged schema contains all the fields from the two schemas,
   * with the same ordering of fields based on the provided reader schema.
   *
   * @param oldSchema    Old schema.
   * @param newSchema    New schema.
   * @param readerSchema Reader schema containing all the fields to read.
   * @return             The merged Avro schema.
   */
  static HoodieSchema getCachedMergedSchema(HoodieSchema oldSchema,
                                             HoodieSchema newSchema,
                                             HoodieSchema readerSchema) {
    return MERGED_SCHEMA_CACHE.computeIfAbsent(
        Pair.of(Pair.of(oldSchema, newSchema), readerSchema), schemaPair -> {
          HoodieSchema schema1 = schemaPair.getLeft().getLeft();
          HoodieSchema schema2 = schemaPair.getLeft().getRight();
          HoodieSchema refSchema = schemaPair.getRight();
          Set<String> schema1Keys =
              getCachedFieldNameToIdMapping(schema1).keySet();
          Set<String> schema2Keys =
              getCachedFieldNameToIdMapping(schema2).keySet();
          List<HoodieSchemaField> mergedFieldList = new ArrayList<>();
          for (int i = 0; i < refSchema.getFields().size(); i++) {
            HoodieSchemaField field = refSchema.getFields().get(i);
            if (schema1Keys.contains(field.name()) || schema2Keys.contains(field.name())) {
              mergedFieldList.add(HoodieSchemaField.of(
                  field.name(),
                  field.schema(),
                  field.doc().orElse(null),
                  field.defaultVal().orElse(null)));
            }
          }
          HoodieSchema mergedSchema = new HoodieSchema.Builder(HoodieSchemaType.RECORD)
              .setName(readerSchema.getName())
              .setDoc(readerSchema.getDoc().orElse(null))
              .setNamespace(readerSchema.getNamespace().orElse(null))
              .setFields(mergedFieldList)
              .build();
          return mergedSchema;
        });
  }

  /**
   * @param schema       Avro schema to check.
   * @param mergedSchema The merged schema for the merged record.
   * @return whether the Avro schema is partial compared to the merged schema.
   */
  @VisibleForTesting
  public static boolean isPartial(HoodieSchema schema, HoodieSchema mergedSchema) {
    return !schema.equals(mergedSchema);
  }
}