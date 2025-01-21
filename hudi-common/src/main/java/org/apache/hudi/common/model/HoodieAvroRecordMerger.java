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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
    return combineAndGetUpdateValue(older, newer, newSchema, props)
        .map(r -> Pair.of(new HoodieAvroIndexedRecord(r), r.getSchema()));
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.AVRO;
  }

  private Option<IndexedRecord> combineAndGetUpdateValue(HoodieRecord older, HoodieRecord newer, Schema schema, Properties props) throws IOException {
    Option<IndexedRecord> previousAvroData = older.toIndexedRecord(schema, props).map(HoodieAvroIndexedRecord::getData);
    if (!previousAvroData.isPresent()) {
      return Option.empty();
    }

    return ((HoodieAvroRecord) newer).getData().combineAndGetUpdateValue(previousAvroData.get(), schema, props);
  }

  @Override
  public HoodieRecordMerger asPreCombiningMode() {
    return HoodiePreCombineAvroRecordMerger.INSTANCE;
  }

  /**
   * Check if the projection is compatible with the record merger according to the payload class.
   */
  @Override
  public boolean isProjectionCompatible(HoodieTableConfig cfg, TypedProperties properties) {
    String payloadClass = cfg.getPayloadClass();
    ValidationUtils.checkArgument(payloadClass != null, "Payload class must be set in HoodieTableConfig for avro record merger");
    HoodieRecordPayload dummyInstance = HoodieRecordUtils.loadPayload(payloadClass, new Object[] {null, 0}, GenericRecord.class, Comparable.class);
    return dummyInstance.enableProjectionPushDown();
  }

  /**
   * Get the mandatory fields for merging according to the payload class.
   */
  @Override
  public String[] getMandatoryFieldsForMerging(Schema dataSchema, HoodieTableConfig cfg, TypedProperties properties) {
    String payloadClass = cfg.getPayloadClass();
    ValidationUtils.checkArgument(payloadClass != null, "Payload class must be set in HoodieTableConfig for avro record merger");
    HoodieRecordPayload dummyInstance = HoodieRecordUtils.loadPayload(payloadClass, new Object[] {null, 0}, GenericRecord.class, Comparable.class);
    String[] specifiedMandatoryFields = dummyInstance.mandatoryFields();
    String[] commonMandatoryFields = HoodieRecordMerger.super.getMandatoryFieldsForMerging(dataSchema, cfg, properties);
    List<String> allNeedMandatoryFields = Arrays.asList(commonMandatoryFields);
    Arrays.stream(specifiedMandatoryFields).filter(f -> !allNeedMandatoryFields.contains(f)).forEach(allNeedMandatoryFields::add);
    return allNeedMandatoryFields.toArray(new String[0]);
  }

}
