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

import org.apache.avro.Schema;
import org.apache.hudi.common.engine.HoodieReaderContext;
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
  private static final Map<Schema, Map<String, Integer>>
      FIELD_NAME_TO_ID_MAPPING_CACHE = new ConcurrentHashMap<>();
  private static final Map<Pair<Pair<Schema, Schema>, Schema>, Schema>
      MERGED_SCHEMA_CACHE = new ConcurrentHashMap<>();

  /**
   * Merges records which can contain partial updates.
   *
   * @param older         Older record of type {@BufferedRecord<T>}.
   * @param oldSchema     Schema of the older record.
   * @param newer         Newer record of type {@BufferedRecord<T>}.
   * @param newSchema     Schema of the newer record.
   * @param readerSchema  Reader schema containing all the fields to read. This is used to maintain
   *                      the ordering of the fields of the merged record.
   * @param readerContext ReaderContext instance.
   * @return The merged record and schema.
   */
  Pair<BufferedRecord<T>, Schema> mergePartialRecords(BufferedRecord<T> older,
                                                             Schema oldSchema,
                                                             BufferedRecord<T> newer,
                                                             Schema newSchema,
                                                             Schema readerSchema,
                                                             HoodieReaderContext<T> readerContext) {
    // The merged schema contains fields that only appear in either older and/or newer record.
    Schema mergedSchema =
        getCachedMergedSchema(oldSchema, newSchema, readerSchema);
    boolean isNewerPartial = isPartial(newSchema, mergedSchema);
    if (!isNewerPartial) {
      return Pair.of(newer, newSchema);
    }
    Set<String> fieldNamesInNewRecord =
        getCachedFieldNameToIdMapping(newSchema).keySet();
    // Collect field values.
    List<Object> values = new ArrayList<>();
    List<Schema.Field> mergedSchemaFields = mergedSchema.getFields();
    for (Schema.Field mergedSchemaField : mergedSchemaFields) {
      String fieldName = mergedSchemaField.name();
      if (fieldNamesInNewRecord.contains(fieldName)) { // field present in newer record.
        values.add(readerContext.getValue(newer.getRecord(), newSchema, fieldName));
      } else { // if not present in newer record pick from old record
        values.add(readerContext.getValue(older.getRecord(), oldSchema, fieldName));
      }
    }
    // Build merged record.
    T engineRecord = readerContext.createEngineRecord(mergedSchema, values);
    BufferedRecord<T> mergedRecord = new BufferedRecord<>(
        newer.getRecordKey(),
        newer.getOrderingValue(),
        engineRecord,
        readerContext.encodeAvroSchema(mergedSchema),
        newer.isDelete());
    return Pair.of(mergedRecord, mergedSchema);
  }

  /**
   * @param avroSchema Avro schema.
   * @return The field name to ID mapping.
   */
  static Map<String, Integer> getCachedFieldNameToIdMapping(Schema avroSchema) {
    return FIELD_NAME_TO_ID_MAPPING_CACHE.computeIfAbsent(avroSchema, schema -> {
      Map<String, Integer> schemaFieldIdMapping = new HashMap<>();
      int fieldId = 0;
      for (Schema.Field field : schema.getFields()) {
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
  static Schema getCachedMergedSchema(Schema oldSchema,
                                             Schema newSchema,
                                             Schema readerSchema) {
    return MERGED_SCHEMA_CACHE.computeIfAbsent(
        Pair.of(Pair.of(oldSchema, newSchema), readerSchema), schemaPair -> {
          Schema schema1 = schemaPair.getLeft().getLeft();
          Schema schema2 = schemaPair.getLeft().getRight();
          Schema refSchema = schemaPair.getRight();
          Set<String> schema1Keys =
              getCachedFieldNameToIdMapping(schema1).keySet();
          Set<String> schema2Keys =
              getCachedFieldNameToIdMapping(schema2).keySet();
          List<Schema.Field> mergedFieldList = new ArrayList<>();
          for (int i = 0; i < refSchema.getFields().size(); i++) {
            Schema.Field field = refSchema.getFields().get(i);
            if (schema1Keys.contains(field.name()) || schema2Keys.contains(field.name())) {
              mergedFieldList.add(new Schema.Field(
                  field.name(),
                  field.schema(),
                  field.doc(),
                  field.defaultVal()));
            }
          }
          Schema mergedSchema = Schema.createRecord(
              readerSchema.getName(),
              readerSchema.getDoc(),
              readerSchema.getNamespace(),
              false);
          mergedSchema.setFields(mergedFieldList);
          return mergedSchema;
        });
  }

  /**
   * @param schema       Avro schema to check.
   * @param mergedSchema The merged schema for the merged record.
   * @return whether the Avro schema is partial compared to the merged schema.
   */
  @VisibleForTesting
  public static boolean isPartial(Schema schema, Schema mergedSchema) {
    return !schema.equals(mergedSchema);
  }
}