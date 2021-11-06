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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieMetadataBloomFilter;
import org.apache.hudi.avro.model.HoodieMetadataFileInfo;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Util class for the metadata table bloom filter partition to
 * convert records while writing and reading to HFiles.
 */
public class HoodieMetadataBloomFilterUtil {

  // Version for the whole bloom filter metadata. Will be needed in the
  // future to detect old and new versions, across upgrades/migration.
  public static final int VERSION_METADATA_BLOOM_FILTER = 1;

  /**
   * TODO: Comment.
   *
   * @param partition
   * @param filesAdded
   * @param filesDeleted
   * @return
   */
  public static HoodieRecord<HoodieMetadataPayload> createBloomFilterMetadataRecords(
      String instantTime, String partition, Option<Map<String, Long>> filesAdded, Option<List<String>> filesDeleted) {
    Map<String, HoodieMetadataFileInfo> fileInfo = new HashMap<>();
    filesAdded.ifPresent(
        m -> m.forEach((filename, size) -> fileInfo.put(filename, new HoodieMetadataFileInfo(size, false))));
    filesDeleted.ifPresent(
        m -> m.forEach(filename -> fileInfo.put(filename, new HoodieMetadataFileInfo(0L, true))));

    HoodieKey key = new HoodieKey(partition, MetadataPartitionType.BLOOM_FILTERS.getPartitionPath());
    HoodieMetadataBloomFilter metadataBloomFilter = new HoodieMetadataBloomFilter();
    HoodieMetadataPayload payload = new HoodieMetadataPayload(key.getRecordKey(),
        HoodieMetadataPayload.METADATA_TYPE_BLOOM_FILTER, fileInfo,
        null, null);
    return new HoodieRecord<>(key, payload);
  }

}
