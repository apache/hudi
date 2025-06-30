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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.util.HoodieFileMetadataMerger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.avro.HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.avro.HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE;
import static org.apache.hudi.avro.HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER;
import static org.apache.hudi.avro.HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieFileMetadataMerger {

  @Test
  public void testMinKey() {
    HoodieFileMetadataMerger metaMerge = new HoodieFileMetadataMerger();
    // for empty map
    Map<String, String> mergeMap = metaMerge.mergeMetaData(newMap());
    Assertions.assertTrue(mergeMap.keySet().size() == 0);

    // just min key
    metaMerge = new HoodieFileMetadataMerger();
    mergeMap = metaMerge.mergeMetaData(newMap(HOODIE_MIN_RECORD_KEY_FOOTER, "1"));
    Assertions.assertTrue(mergeMap.keySet().size() == 1);
    assertEquals("1", mergeMap.get(HOODIE_MIN_RECORD_KEY_FOOTER));

    // then with empty map, min key should not change
    mergeMap = metaMerge.mergeMetaData(newMap());
    Assertions.assertTrue(mergeMap.keySet().size() == 1);
    assertEquals("1", mergeMap.get(HOODIE_MIN_RECORD_KEY_FOOTER));

    // then with key bigger than 1, should not change
    mergeMap = metaMerge.mergeMetaData(newMap(HOODIE_MIN_RECORD_KEY_FOOTER, "5"));
    Assertions.assertTrue(mergeMap.keySet().size() == 1);
    assertEquals("1", mergeMap.get(HOODIE_MIN_RECORD_KEY_FOOTER));

    // then with key smaller than 1, should change
    mergeMap = metaMerge.mergeMetaData(newMap(HOODIE_MIN_RECORD_KEY_FOOTER, "0"));
    Assertions.assertTrue(mergeMap.keySet().size() == 1);
    assertEquals("0", mergeMap.get(HOODIE_MIN_RECORD_KEY_FOOTER));
  }

  @Test
  public void testMaxKey() {
    HoodieFileMetadataMerger metaMerge = new HoodieFileMetadataMerger();
    // for empty map
    Map<String, String> mergeMap = metaMerge.mergeMetaData(newMap());
    Assertions.assertTrue(mergeMap.keySet().size() == 0);

    // just max key
    metaMerge = new HoodieFileMetadataMerger();
    mergeMap = metaMerge.mergeMetaData(newMap(HOODIE_MAX_RECORD_KEY_FOOTER, "1"));
    Assertions.assertTrue(mergeMap.keySet().size() == 1);
    assertEquals("1", mergeMap.get(HOODIE_MAX_RECORD_KEY_FOOTER));

    // then with empty map, min key should not change
    mergeMap = metaMerge.mergeMetaData(newMap());
    Assertions.assertTrue(mergeMap.keySet().size() == 1);
    assertEquals("1", mergeMap.get(HOODIE_MAX_RECORD_KEY_FOOTER));

    // then with key smaller than 1, should not change
    mergeMap = metaMerge.mergeMetaData(newMap(HOODIE_MAX_RECORD_KEY_FOOTER, "0"));
    Assertions.assertTrue(mergeMap.keySet().size() == 1);
    assertEquals("1", mergeMap.get(HOODIE_MAX_RECORD_KEY_FOOTER));

    // then with key bigger than 1, should change
    mergeMap = metaMerge.mergeMetaData(newMap(HOODIE_MAX_RECORD_KEY_FOOTER, "5"));
    assertEquals("5", mergeMap.get(HOODIE_MAX_RECORD_KEY_FOOTER));
    Assertions.assertFalse(mergeMap.containsKey(HOODIE_MIN_RECORD_KEY_FOOTER));
  }

  @Test
  public void tesMaxAndMin() {
    HoodieFileMetadataMerger metaMerge = new HoodieFileMetadataMerger();

    Map<String, String> mergeMap = metaMerge.mergeMetaData(newMap(HOODIE_MIN_RECORD_KEY_FOOTER, "1", HOODIE_MAX_RECORD_KEY_FOOTER, "6"));
    Assertions.assertTrue(mergeMap.keySet().size() == 2);
    assertEquals("1", mergeMap.get(HOODIE_MIN_RECORD_KEY_FOOTER));
    assertEquals("6", mergeMap.get(HOODIE_MAX_RECORD_KEY_FOOTER));

    mergeMap = metaMerge.mergeMetaData(newMap(HOODIE_MIN_RECORD_KEY_FOOTER, "0", HOODIE_MAX_RECORD_KEY_FOOTER, "5"));
    Assertions.assertTrue(mergeMap.keySet().size() == 2);
    assertEquals("0", mergeMap.get(HOODIE_MIN_RECORD_KEY_FOOTER));
    assertEquals("6", mergeMap.get(HOODIE_MAX_RECORD_KEY_FOOTER));

    mergeMap = metaMerge.mergeMetaData(newMap(HOODIE_MIN_RECORD_KEY_FOOTER, "4", HOODIE_MAX_RECORD_KEY_FOOTER, "5"));
    Assertions.assertTrue(mergeMap.keySet().size() == 2);
    assertEquals("0", mergeMap.get(HOODIE_MIN_RECORD_KEY_FOOTER));
    assertEquals("6", mergeMap.get(HOODIE_MAX_RECORD_KEY_FOOTER));
  }

  @ParameterizedTest()
  @ValueSource(strings = {"SIMPLE", "DYNAMIC_V0"})
  public void testBloomFilter(String bloomFilterType) {
    HoodieFileMetadataMerger metaMerge = new HoodieFileMetadataMerger();
    int[] sizes = {100, 1000, 10000};
    BloomFilter bloomFilter = null;
    for (int size : sizes) {
      BloomFilter filter = getBloomFilter(bloomFilterType, 1000, 0.000001, 100000);
      for (int i = 0; i < size; i++) {
        String key = String.format("key%d", size + i);
        filter.add(key);
      }
      if (bloomFilter == null) {
        bloomFilter = filter;
      } else {
        bloomFilter.or(filter);
      }

      metaMerge.mergeMetaData(
          newMap(
              HOODIE_BLOOM_FILTER_TYPE_CODE, bloomFilterType,
              HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, filter.serializeToString())
      );
    }

    Map<String, String> mergedMetaData = metaMerge.getMergedMetaData();
    assertEquals(bloomFilterType, mergedMetaData.get(HOODIE_BLOOM_FILTER_TYPE_CODE));
    assertEquals(bloomFilter.serializeToString(), mergedMetaData.get(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY));
  }

  @Test
  public void testDifferentTypeOfBloomFilter() {
    HoodieFileMetadataMerger metaMerge = new HoodieFileMetadataMerger();
    BloomFilter simpleFilter = getBloomFilter(BloomFilterTypeCode.SIMPLE.name(), 1000, 0.000001, 100000);
    for (int i = 0; i < 100; i++) {
      String key = String.format("key%d", 100 + i);
      simpleFilter.add(key);
    }
    metaMerge.mergeMetaData(
        newMap(
            HOODIE_BLOOM_FILTER_TYPE_CODE, BloomFilterTypeCode.SIMPLE.name(),
            HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, simpleFilter.serializeToString())
    );
    BloomFilter dynamicFilter = getBloomFilter(BloomFilterTypeCode.DYNAMIC_V0.name(), 1000, 0.000001, 100000);
    for (int i = 0; i < 100; i++) {
      String key = String.format("key%d", 100 + i);
      dynamicFilter.add(key);
    }

    Assertions.assertThrows(IllegalArgumentException.class, () ->
        metaMerge.mergeMetaData(
            newMap(
                HOODIE_BLOOM_FILTER_TYPE_CODE, BloomFilterTypeCode.DYNAMIC_V0.name(),
                HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, dynamicFilter.serializeToString())
        )
    );
  }

  private BloomFilter getBloomFilter(String typeCode, int numEntries, double errorRate, int maxEntries) {
    if (typeCode.equalsIgnoreCase(BloomFilterTypeCode.SIMPLE.name())) {
      return BloomFilterFactory.createBloomFilter(numEntries, errorRate, -1, typeCode);
    } else {
      return BloomFilterFactory.createBloomFilter(numEntries, errorRate, maxEntries, typeCode);
    }
  }

  private Map<String, String> newMap(String... kvs) {
    ValidationUtils.checkArgument(kvs.length == 0 || kvs.length % 2 == 0, "num of input args should be 0 or multiples of 2");
    HashMap map = new HashMap();
    for (int i = 0; i < kvs.length; i += 2) {
      map.put(kvs[i], kvs[i + 1]);
    }
    return map;
  }
}