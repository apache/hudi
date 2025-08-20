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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * Record merger for Hoodie avro record.
 *
 * <p>It should only be used for base record from disk to merge with incoming record.
 */
public class HoodieAvroRecordMerger implements HoodieRecordMerger, OperationModeAwareness {
  public static final HoodieAvroRecordMerger INSTANCE = new HoodieAvroRecordMerger();

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
  }

  @Override
  public Pair<HoodieRecord, Schema> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
    Option<IndexedRecord> updatedValue = combineAndGetUpdateValue(older, newer, oldSchema, newSchema, props);
    if (updatedValue.isEmpty()) {
      return Pair.of(new HoodieEmptyRecord<>(newer.getKey(), HoodieOperation.DELETE, OrderingValues.getDefault(), HoodieRecordType.AVRO), newSchema);
    }
    return Pair.of(new HoodieAvroIndexedRecord(updatedValue.get()), updatedValue.get().getSchema());
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.AVRO;
  }

  private Option<IndexedRecord> combineAndGetUpdateValue(HoodieRecord older, HoodieRecord newer, Schema oldSchema, Schema newSchema, Properties props) throws IOException {
    Option<IndexedRecord> previousAvroData = older.toIndexedRecord(oldSchema, props).map(HoodieAvroIndexedRecord::getData);
    HoodieRecordPayload payload = ((HoodieAvroRecord) newer).getData();
    if (previousAvroData.isEmpty()) {
      return payload.getInsertValue(newSchema, props);
    }
    return ((HoodieAvroRecord) newer).getData().combineAndGetUpdateValue(previousAvroData.get(), newSchema, props);
  }

  @Override
  public HoodieRecordMerger asPreCombiningMode() {
    return HoodiePreCombineAvroRecordMerger.INSTANCE;
  }
}
