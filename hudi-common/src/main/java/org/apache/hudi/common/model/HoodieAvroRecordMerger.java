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

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID;
  }

  @Override
  public Pair<Option<HoodieRecord>, Schema> merge(HoodieRecord older, Schema oldSchema,
      HoodieRecord newer, Schema newSchema, Properties props) throws IOException {
    ValidationUtils.checkArgument(older.getRecordType() == HoodieRecordType.AVRO);
    ValidationUtils.checkArgument(newer.getRecordType() == HoodieRecordType.AVRO);
    if (older instanceof HoodieAvroRecord && newer instanceof HoodieAvroRecord) {
      HoodieRecord res = preCombine(older, newer);
      if (res == older) {
        return Pair.of(Option.of(res), oldSchema);
      } else {
        return Pair.of(Option.of(res), newSchema);
      }
    } else if (older instanceof HoodieAvroIndexedRecord && newer instanceof HoodieAvroRecord) {
      Option<HoodieRecord> res = combineAndGetUpdateValue(older, newer, newSchema, props);
      if (res.isPresent()) {
        return Pair.of(res, ((IndexedRecord) res.get().getData()).getSchema());
      } else {
        return Pair.of(res, null);
      }
    } else {
      throw new UnsupportedOperationException();
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
    Option<HoodieAvroIndexedRecord> previousRecordAvroPayload = older.toIndexedRecord(schema, props);
    if (!previousRecordAvroPayload.isPresent()) {
      return Option.empty();
    }

    return ((HoodieAvroRecord) newer).getData().combineAndGetUpdateValue(previousRecordAvroPayload.get().getData(), schema, props)
        .map(combinedAvroPayload -> new HoodieAvroIndexedRecord((IndexedRecord) combinedAvroPayload));
  }
}
