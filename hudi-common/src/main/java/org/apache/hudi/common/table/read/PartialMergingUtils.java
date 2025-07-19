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
import org.apache.hudi.common.util.collection.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Generic util class to merge records of type T that may contain partial updates.
 */
public class PartialMergingUtils<T> {
  private static final Map<Schema, Map<Integer, String>>
      FIELD_ID_TO_NAME_MAPPING_CACHE = new ConcurrentHashMap<>();
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
  public Pair<BufferedRecord<T>, Schema> mergePartialRecords(BufferedRecord<T> older,
                                                             Schema oldSchema,
                                                             BufferedRecord<T> newer,
                                                             Schema newSchema,
                                                             Schema readerSchema,
                                                             HoodieReaderContext<T> readerContext) {
    boolean isNewerPartial = isPartial(newSchema, readerSchema);
    if (!isNewerPartial) {
      return Pair.of(newer, newSchema);
    }
    // The merged schema contains fields that only appear in either older and/or newer record.
    Schema mergedSchema =
        getCachedMergedSchema(oldSchema, newSchema, readerSchema);
    Map<String, Integer> fieldNameToIdFromNewRecordSchema =
        getCachedFieldNameToIdMapping(newSchema);
    // Collect field values.
    List<Object> values = new ArrayList<>();
    for (int index = 0; index < mergedSchema.getFields().size(); index++) {
      String fieldName = mergedSchema.getFields().get(index).name();
      Integer fieldIdFromNewRecordSchema =
          fieldNameToIdFromNewRecordSchema.get(fieldName);
      if (fieldIdFromNewRecordSchema == null) {
        values.add(readerContext.getValue(older.getRecord(), oldSchema, fieldName));
      } else {
        values.add(readerContext.getValue(newer.getRecord(), newSchema, fieldName));
      }
    }
    // Build merged record.
    T engineRecord = readerContext.constructEngineRecord(mergedSchema, values);
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
   * @return The field ID to field name mapping.
   */
  public static Map<Integer, String> getCachedFieldIdToNameMapping(Schema avroSchema) {
    return FIELD_ID_TO_NAME_MAPPING_CACHE.computeIfAbsent(avroSchema, schema -> {
      Map<Integer, String> schemaFieldIdMapping = new HashMap<>();
      int fieldId = 0;
      for (Schema.Field field : schema.getFields()) {
        schemaFieldIdMapping.put(fieldId, field.name());
        fieldId++;
      }
      return schemaFieldIdMapping;
    });
  }

  /**
   * @param avroSchema Avro schema.
   * @return The field name to ID mapping.
   */
  public static Map<String, Integer> getCachedFieldNameToIdMapping(Schema avroSchema) {
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
  public static Schema getCachedMergedSchema(Schema oldSchema,
                                             Schema newSchema,
                                             Schema readerSchema) {
    return MERGED_SCHEMA_CACHE.computeIfAbsent(
        Pair.of(Pair.of(oldSchema, newSchema), readerSchema), schemaPair -> {
          Schema schema1 = schemaPair.getLeft().getLeft();
          Schema schema2 = schemaPair.getLeft().getRight();
          Schema refSchema = schemaPair.getRight();
          Map<String, Integer> nameToIdMapping1 =
              getCachedFieldNameToIdMapping(schema1);
          Map<String, Integer> nameToIdMapping2 =
              getCachedFieldNameToIdMapping(schema2);
          // This field name set contains all the fields that appear
          // either in the oldSchema and/or the newSchema
          Set<String> fieldNameSet = new HashSet<>();
          fieldNameSet.addAll(nameToIdMapping1.keySet());
          fieldNameSet.addAll(nameToIdMapping2.keySet());
          List<Schema.Field> mergedFieldList = new ArrayList<>();
          for (int i = 0; i < refSchema.getFields().size(); i++) {
            Schema.Field field = refSchema.getFields().get(i);
            if (fieldNameSet.contains(field.name())) {
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
  public static boolean isPartial(Schema schema, Schema mergedSchema) {
    return !schema.equals(mergedSchema);
  }
}
