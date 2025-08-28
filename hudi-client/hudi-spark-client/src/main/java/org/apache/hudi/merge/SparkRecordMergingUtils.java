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

package org.apache.hudi.merge;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Util class to merge records that may contain partial updates.
 * This can be used by any Spark {@link HoodieRecordMerger} implementation.
 */
public class SparkRecordMergingUtils {
  private static final Map<Schema, Map<Integer, StructField>> FIELD_ID_TO_FIELD_MAPPING_CACHE = new ConcurrentHashMap<>();
  private static final Map<Schema, Map<String, Integer>> FIELD_NAME_TO_ID_MAPPING_CACHE = new ConcurrentHashMap<>();
  private static final Map<Pair<Pair<Schema, Schema>, Schema>,
      Pair<Map<Integer, StructField>, Pair<StructType, Schema>>> MERGED_SCHEMA_CACHE = new ConcurrentHashMap<>();

  /**
   * Merges records which can contain partial updates.
   * <p>
   * For example, the reader schema is
   * {[
   * {"name":"id", "type":"string"},
   * {"name":"ts", "type":"long"},
   * {"name":"name", "type":"string"},
   * {"name":"price", "type":"double"},
   * {"name":"tags", "type":"string"}
   * ]}
   * The older and newer records can be (omitting Hudi meta fields):
   * <p>
   * (1) older (complete record update):
   * id | ts | name  | price | tags
   * 1 | 10 | apple |  2.3  | fruit
   * <p>
   * newer (partial record update):
   * ts | price
   * 16 |  2.8
   * <p>
   * The merging result is (updated values from newer replaces the ones in the older):
   * <p>
   * id | ts | name  | price | tags
   * 1 | 16 | apple |  2.8  | fruit
   * <p>
   * (2) older (partial record update):
   * ts | price
   * 10 | 2.8
   * <p>
   * newer (partial record update):
   * ts | tag
   * 16 | fruit,juicy
   * <p>
   * The merging result is (two partial updates are merged together, and values of overlapped
   * fields come from the newer):
   * <p>
   * ts | price | tags
   * 16 |  2.8  | fruit,juicy
   *
   * @param older         Older {@link HoodieSparkRecord}.
   * @param oldSchema     Schema of the older record.
   * @param newer         Newer {@link HoodieSparkRecord}.
   * @param newSchema     Schema of the newer record.
   * @param readerSchema  Reader schema containing all the fields to read. This is used to maintain
   *                      the ordering of the fields of the merged record.
   * @param recordContext Record context for working with the records.
   * @return The merged record and schema.
   */
  public static BufferedRecord<InternalRow> mergePartialRecords(BufferedRecord<InternalRow> older,
                                                                Schema oldSchema,
                                                                BufferedRecord<InternalRow> newer,
                                                                Schema newSchema,
                                                                Schema readerSchema,
                                                                RecordContext<InternalRow> recordContext) {
    // The merged schema contains fields that only appear in either older and/or newer record
    Pair<Map<Integer, StructField>, Pair<StructType, Schema>> mergedSchemaPair =
        getCachedMergedSchema(oldSchema, newSchema, readerSchema);
    boolean isNewerPartial = isPartial(newSchema, mergedSchemaPair.getRight().getRight());
    if (isNewerPartial) {
      InternalRow oldRow = older.getRecord();
      InternalRow newPartialRow = newer.getRecord();

      Map<Integer, StructField> mergedIdToFieldMapping = mergedSchemaPair.getLeft();
      Map<String, Integer> oldNameToIdMapping = getCachedFieldNameToIdMapping(oldSchema);
      Map<String, Integer> newPartialNameToIdMapping = getCachedFieldNameToIdMapping(newSchema);
      List<Object> values = new ArrayList<>(mergedIdToFieldMapping.size());
      for (int fieldId = 0; fieldId < mergedIdToFieldMapping.size(); fieldId++) {
        StructField structField = mergedIdToFieldMapping.get(fieldId);
        Integer ordInPartialUpdate = newPartialNameToIdMapping.get(structField.name());
        if (ordInPartialUpdate != null) {
          // The field exists in the newer record; picks the value from newer record
          values.add(newPartialRow.get(ordInPartialUpdate, structField.dataType()));
        } else {
          // The field does not exist in the newer record; picks the value from older record
          values.add(oldRow.get(oldNameToIdMapping.get(structField.name()), structField.dataType()));
        }
      }
      InternalRow mergedRow = new GenericInternalRow(values.toArray());
      return BufferedRecords.fromEngineRecord(mergedRow, readerSchema, recordContext, newer.getOrderingValue(), newer.getRecordKey(), newer.isDelete());
    } else {
      return newer;
    }
  }

  /**
   * @param avroSchema Avro schema.
   * @return The field ID to {@link StructField} instance mapping.
   */
  public static Map<Integer, StructField> getCachedFieldIdToFieldMapping(Schema avroSchema) {
    return FIELD_ID_TO_FIELD_MAPPING_CACHE.computeIfAbsent(avroSchema, schema -> {
      StructType structType = HoodieInternalRowUtils.getCachedSchema(schema);
      Map<Integer, StructField> schemaFieldIdMapping = new HashMap<>();
      int fieldId = 0;

      for (StructField field : structType.fields()) {
        schemaFieldIdMapping.put(fieldId, field);
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
      StructType structType = HoodieInternalRowUtils.getCachedSchema(schema);
      Map<String, Integer> schemaFieldIdMapping = new HashMap<>();
      int fieldId = 0;

      for (StructField field : structType.fields()) {
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
   * @return The ID to {@link StructField} instance mapping of the merged schema, and the
   * {@link StructType} and Avro schema of the merged schema.
   */
  public static Pair<Map<Integer, StructField>, Pair<StructType, Schema>> getCachedMergedSchema(Schema oldSchema,
                                                                                                Schema newSchema,
                                                                                                Schema readerSchema) {
    return MERGED_SCHEMA_CACHE.computeIfAbsent(
        Pair.of(Pair.of(oldSchema, newSchema), readerSchema), schemaPair -> {
          Schema schema1 = schemaPair.getLeft().getLeft();
          Schema schema2 = schemaPair.getLeft().getRight();
          Schema refSchema = schemaPair.getRight();
          Map<String, Integer> nameToIdMapping1 = getCachedFieldNameToIdMapping(schema1);
          Map<String, Integer> nameToIdMapping2 = getCachedFieldNameToIdMapping(schema2);
          // Mapping of field ID/position to the StructField instance of the readerSchema
          Map<Integer, StructField> refFieldIdToFieldMapping = getCachedFieldIdToFieldMapping(refSchema);
          // This field name set contains all the fields that appear
          // either in the oldSchema and/or the newSchema
          Set<String> fieldNameSet = new HashSet<>();
          fieldNameSet.addAll(nameToIdMapping1.keySet());
          fieldNameSet.addAll(nameToIdMapping2.keySet());
          int fieldId = 0;
          Map<Integer, StructField> mergedMapping = new HashMap<>();
          List<StructField> mergedFieldList = new ArrayList<>();
          // Iterates over the fields based on the original ordering of the fields of the
          // readerSchema using the field ID/position from 0
          for (int i = 0; i < refFieldIdToFieldMapping.size(); i++) {
            StructField field = refFieldIdToFieldMapping.get(i);
            if (fieldNameSet.contains(field.name())) {
              mergedMapping.put(fieldId, field);
              mergedFieldList.add(field);
              fieldId++;
            }
          }
          StructType mergedStructType = new StructType(mergedFieldList.toArray(new StructField[0]));
          Schema mergedSchema = AvroSchemaCache.intern(AvroConversionUtils.convertStructTypeToAvroSchema(
              mergedStructType, readerSchema.getName(), readerSchema.getNamespace()));
          return Pair.of(mergedMapping, Pair.of(mergedStructType, mergedSchema));
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
