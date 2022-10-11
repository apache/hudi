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

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieMetadataPayload;

import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;

public class HoodieAvroRecordMerger implements HoodieRecordMerger {

  public static String DE_DUPING = "de_duping";

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID;
  }

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, Properties props) throws IOException {
    ValidationUtils.checkArgument(older.getRecordType() == HoodieRecordType.AVRO);
    ValidationUtils.checkArgument(newer.getRecordType() == HoodieRecordType.AVRO);
    boolean deDuping = Boolean.parseBoolean(props.getOrDefault(DE_DUPING, "false").toString());
    if (deDuping) {
      HoodieRecord res = preCombine(older, newer);
      if (res == older) {
        return Option.of(Pair.of(res, oldSchema));
      } else {
        return Option.of(Pair.of(res, newSchema));
      }
    } else {
      return combineAndGetUpdateValue(older, newer, newSchema, props)
          .map(r -> Pair.of(r, (((HoodieAvroIndexedRecord) r).getData()).getSchema()));
    }
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.AVRO;
  }

  private HoodieRecord preCombine(HoodieRecord older, HoodieRecord newer) {
    HoodieRecordPayload picked = unsafeCast(((HoodieAvroRecord) newer).getData().preCombine(((HoodieAvroRecord) older).getData()));
    if (picked instanceof HoodieMetadataPayload) {
      // NOTE: HoodieMetadataPayload return a new payload
      return new HoodieAvroRecord(newer.getKey(), picked, newer.getOperation());
    }
    return picked.equals(((HoodieAvroRecord) newer).getData()) ? newer : older;
  }

  private Option<HoodieRecord> combineAndGetUpdateValue(HoodieRecord older, HoodieRecord newer, Schema schema, Properties props) throws IOException {
    Option<IndexedRecord> previousAvroData = older.toIndexedRecord(schema, props).map(HoodieAvroIndexedRecord::getData);
    if (!previousAvroData.isPresent()) {
      return Option.empty();
    }

    return ((HoodieAvroRecord) newer).getData().combineAndGetUpdateValue(previousAvroData.get(), schema, props)
        .map(combinedAvroPayload -> new HoodieAvroIndexedRecord((IndexedRecord) combinedAvroPayload));
  }
}
