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

package org.apache.hudi.avro;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.HoodieDynamicBoundedBloomFilter;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.avro.HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;

/**
 * This is write-support utility base-class taking up handling of
 *
 * <ul>
 *   <li>Adding record keys to the Bloom Filter</li>
 *   <li>Keeping track of min/max record key (w/in single file)</li>
 * </ul>
 *
 * @param <T> record-key type being ingested by this clas
 */
public abstract class HoodieBloomFilterWriteSupport<T extends Comparable<T>> {

  public static final String HOODIE_MIN_RECORD_KEY_FOOTER = "hoodie_min_record_key";
  public static final String HOODIE_MAX_RECORD_KEY_FOOTER = "hoodie_max_record_key";
  public static final String HOODIE_BLOOM_FILTER_TYPE_CODE = "hoodie_bloom_filter_type_code";

  private final BloomFilter bloomFilter;

  private T minRecordKey;
  private T maxRecordKey;

  public HoodieBloomFilterWriteSupport(BloomFilter bloomFilter) {
    this.bloomFilter = bloomFilter;
  }

  public void addKey(T recordKey) {
    bloomFilter.add(getUTF8Bytes(recordKey));

    if (minRecordKey == null || minRecordKey.compareTo(recordKey) > 0) {
      minRecordKey = dereference(recordKey);
    }

    if (maxRecordKey == null || maxRecordKey.compareTo(recordKey) < 0) {
      maxRecordKey = dereference(recordKey);
    }
  }

  public Map<String, String> finalizeMetadata() {
    HashMap<String, String> extraMetadata = new HashMap<>();

    extraMetadata.put(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, bloomFilter.serializeToString());
    if (bloomFilter.getBloomFilterTypeCode().name().contains(HoodieDynamicBoundedBloomFilter.TYPE_CODE_PREFIX)) {
      extraMetadata.put(HOODIE_BLOOM_FILTER_TYPE_CODE, bloomFilter.getBloomFilterTypeCode().name());
    }

    if (minRecordKey != null && maxRecordKey != null) {
      extraMetadata.put(HOODIE_MIN_RECORD_KEY_FOOTER, minRecordKey.toString());
      extraMetadata.put(HOODIE_MAX_RECORD_KEY_FOOTER, maxRecordKey.toString());
    }

    return extraMetadata;
  }

  /**
   * Since Bloom Filter ingests record-keys represented as UTF8 encoded byte string,
   * this method have to be implemented for converting the original record key into one
   */
  protected abstract byte[] getUTF8Bytes(T key);

  /**
   * This method allows to dereference the key object (t/h cloning, for ex) that might be
   * pointing at a shared mutable buffer, to make sure that we're not keeping references
   * to mutable objects
   */
  protected T dereference(T key) {
    return key;
  }
}
