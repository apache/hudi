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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.io.IOException;

/**
 * Record merger for Hoodie avro record.
 *
 * <p>It should only be used for deduplication among incoming records.
 */
public class HoodiePreCombineAvroRecordMerger extends HoodieAvroRecordMerger {
  public static final HoodiePreCombineAvroRecordMerger INSTANCE = new HoodiePreCombineAvroRecordMerger();

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
    return Option.of(preCombine(older, oldSchema, newer, newSchema, props));
  }

  @SuppressWarnings("rawtypes, unchecked")
  private Pair<HoodieRecord, Schema> preCombine(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) {
    HoodieRecordPayload newerPayload = ((HoodieAvroRecord) newer).getData();
    HoodieRecordPayload olderPayload = ((HoodieAvroRecord) older).getData();
    HoodieRecordPayload payload = newerPayload.preCombine(olderPayload, newSchema, props);
    if (payload == olderPayload) {
      return Pair.of(older, oldSchema);
    } else if (payload == newerPayload) {
      return Pair.of(newer, newSchema);
    } else {
      HoodieRecord mergedRecord = new HoodieAvroRecord(newer.getKey(), payload, newer.getOperation());
      return Pair.of(mergedRecord, newSchema);
    }
  }
}
