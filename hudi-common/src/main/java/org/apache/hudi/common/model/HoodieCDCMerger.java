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

package org.apache.hudi.common.model;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

public abstract class HoodieCDCMerger implements HoodieRecordMerger {

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older,
                                                  Schema oldSchema,
                                                  HoodieRecord newer,
                                                  Schema newSchema,
                                                  TypedProperties props) {
    IndexedRecord oldRecord;
    IndexedRecord newRecord;
    try {
      oldRecord = older.toIndexedRecord(oldSchema, props).map(HoodieAvroIndexedRecord::getData).get();
      newRecord = newer.toIndexedRecord(newSchema, props).map(HoodieAvroIndexedRecord::getData).get();
      // Main comparison logic.
      if (shouldPickOldRecord(oldRecord, newRecord, oldSchema, newSchema)) {
        return Option.of(Pair.of(older, oldSchema));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Handle delete logic.
    if (isDeleteRecord((GenericRecord) newRecord)) {
      return Option.empty();
    }
    // Enrich newRecord if needed.
    postMergeOperation(newRecord, oldRecord);
    return Option.of(Pair.of(
        new HoodieAvroIndexedRecord(newRecord),
        newSchema));
  }

  @Override
  public HoodieRecord.HoodieRecordType getRecordType() {
    return HoodieRecord.HoodieRecordType.AVRO;
  }

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
  }

  protected void postMergeOperation(IndexedRecord mergedRecord,
                                    IndexedRecord oldRecord) {
    // No op.
  }

  static boolean isDeleteRecord(GenericRecord record, String deleteField) {
    Object value = HoodieAvroUtils.getFieldVal(record, deleteField);
    return value != null && value.toString().equalsIgnoreCase("D");
  }

  protected abstract boolean isDeleteRecord(GenericRecord record);

  protected abstract boolean shouldPickOldRecord(IndexedRecord oldRecord,
                                                 IndexedRecord newRecord,
                                                 Schema oldSchema,
                                                 Schema newSchema) throws Exception;

  protected boolean isHoodieDeleteRecord(GenericRecord genericRecord) {
    final String isDeleteKey = HoodieRecord.HOODIE_IS_DELETED_FIELD;
    // Modify to be compatible with new version Avro.
    // The new version Avro throws for GenericRecord.get if the field name
    // does not exist in the schema.
    if (genericRecord.getSchema().getField(isDeleteKey) == null) {
      return false;
    }
    Object deleteMarker = genericRecord.get(isDeleteKey);
    return (deleteMarker instanceof Boolean && (boolean) deleteMarker);
  }
}
