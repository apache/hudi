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

package org.apache.hudi.util;

import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.avro.HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;

public class HoodieFileMetadataMerger {

  private final HashMap<String, String> mergedMetadata = new HashMap<>();

  private BloomFilter bloomFilter;

  private String minRecordKey;

  private String maxRecordKey;

  public HoodieFileMetadataMerger() {

  }

  public Map<String, String> mergeMetaData(Map<String, String> metaMap) {
    mergedMetadata.putAll(metaMap);

    String minRecordKey = metaMap.get(HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER);
    String maxRecordKey = metaMap.get(HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER);
    if (this.minRecordKey == null || (minRecordKey != null && this.minRecordKey.compareTo(minRecordKey) > 0)) {
      this.minRecordKey = minRecordKey;
    }
    if (this.maxRecordKey == null || (maxRecordKey != null && this.maxRecordKey.compareTo(maxRecordKey) < 0)) {
      this.maxRecordKey = maxRecordKey;
    }

    String bloomFilterType = metaMap.get(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE);
    String avroBloom = metaMap.get(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
    if (!StringUtils.isNullOrEmpty(avroBloom)) {
      BloomFilter targetBloomFilter = BloomFilterFactory.fromString(avroBloom, bloomFilterType);
      if (this.bloomFilter == null) {
        this.bloomFilter = targetBloomFilter;
      } else {
        this.bloomFilter.or(targetBloomFilter);
      }
    }

    if (this.minRecordKey != null) {
      mergedMetadata.put(HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER, this.minRecordKey);
    }
    if (this.maxRecordKey != null) {
      mergedMetadata.put(HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER, this.maxRecordKey);
    }
    if (this.bloomFilter != null) {
      mergedMetadata.put(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, this.bloomFilter.serializeToString());
    }

    return mergedMetadata;
  }

  public Map<String, String> getMergedMetaData() {
    return mergedMetadata;
  }
}
