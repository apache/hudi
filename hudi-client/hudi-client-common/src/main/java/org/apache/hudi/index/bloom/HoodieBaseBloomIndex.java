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

package org.apache.hudi.index.bloom;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;

/**
 * Base class for the bloom index implementations.
 */
public abstract class HoodieBaseBloomIndex<T extends HoodieRecordPayload<T>> extends HoodieIndex<T, Object, Object, Object> {

  protected HoodieBaseBloomIndex(HoodieWriteConfig config) {
    super(config);
  }

  /**
   * Tag the <rowKey, filename> back to the original HoodieRecord List.
   */
  protected HoodieData<HoodieRecord<T>> tagLocationBackToRecords(
      HoodiePairData<HoodieKey, HoodieRecordLocation> keyFilenamePair,
      HoodieData<HoodieRecord<T>> records) {
    HoodiePairData<HoodieKey, HoodieRecord<T>> keyRecordPairs =
        records.mapToPair(record -> new ImmutablePair<>(record.getKey(), record));
    // Here as the records might have more data than keyFilenamePairs
    // (some row keys' fileId is null), so we do left outer join.
    return keyRecordPairs.leftOuterJoin(keyFilenamePair).values()
        .map(v -> HoodieIndexUtils.getTaggedRecord(v.getLeft(), Option.ofNullable(v.getRight().orElse(null))));
  }

}
