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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

/**
 * This is the merger that replaces PartialUpdateAvroPayload class.
 */
public class PartialUpdateAvroMerger extends EventTimeBasedAvroRecordMerger {
  public static final PartialUpdateAvroMerger INSTANCE = new PartialUpdateAvroMerger();

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.PARTIAL_UPDATE_MERGE_STRATEGY_UUID;
  }

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord oldRecord,
                                                  Schema oldSchema,
                                                  HoodieRecord newRecord,
                                                  Schema newSchema,
                                                  TypedProperties props) throws IOException {
    Comparable newOrderingVal = newRecord.getOrderingValue(newSchema, props);
    Comparable oldOrderingVal = oldRecord.getOrderingValue(oldSchema, props);
    HoodieRecord lowOrderRecord = oldRecord;
    HoodieRecord highOrderRecord = newRecord;
    Schema lowOrderSchema = oldSchema;
    Schema highOrderSchema = newSchema;

    if (oldOrderingVal.compareTo(newOrderingVal) > 0) {
      lowOrderRecord = newRecord;
      lowOrderSchema = newSchema;
      highOrderRecord = oldRecord;
      highOrderSchema = oldSchema;
    }

    if (lowOrderRecord.isDelete(lowOrderSchema, props)
        || highOrderRecord.isDelete(highOrderSchema, props)) {
      return Option.of(Pair.of(highOrderRecord, highOrderSchema));
    } else {
      return Option.of(Pair.of(
          mergeRecord(lowOrderRecord, lowOrderSchema, highOrderRecord, highOrderSchema),
          highOrderSchema));
    }
  }

  private HoodieRecord mergeRecord(HoodieRecord lowOrderRecord,
                                   Schema lowOrderSchema,
                                   HoodieRecord highOrderRecord,
                                   Schema highOrderSchema) {
    // Currently assume there is no schema evolution, solve it in HUDI-9253
    ValidationUtils.checkArgument(
        lowOrderSchema.getFields().size() == highOrderSchema.getFields().size());
    return new HoodieAvroIndexedRecord(mergeIndexedRecord(
        (IndexedRecord) lowOrderRecord.data, (IndexedRecord) highOrderRecord.data, highOrderSchema));
  }

  protected IndexedRecord mergeIndexedRecord(IndexedRecord lowOrderRecord,
                                             IndexedRecord highOrderRecord,
                                             Schema schema) {
    GenericRecord result = new GenericData.Record(schema);
    for (int i = 0; i < schema.getFields().size(); i++) {
      Object lowVal = lowOrderRecord.get(i);
      Object highVal = highOrderRecord.get(i);
      // Start with lowOrderRecord value
      Object value = lowVal;
      // Override if highOrderRecord has a non-null value
      if (highVal != null) {
        value = highVal;
      }
      result.put(i, value);
    }
    return result;
  }
}
