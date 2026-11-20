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

package org.apache.hudi.metadata;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.metadata.HoodieSecondaryIndexEntrySizeEstimator.KeySample;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.RECORD_INDEX_AVERAGE_RECORD_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieSecondaryIndexEntrySizeEstimator {

  @Test
  void samplingCountsEveryRecordAndKeepsAtMostTheGivenKeys() {
    List<HoodieRecord> records = indexRecords(50, 10_000);
    KeySample sample = HoodieSecondaryIndexEntrySizeEstimator.sampleKeys(records.iterator(), 1_000, Long.MAX_VALUE);
    assertEquals(10_000, sample.getCount());
    assertEquals(1_000, sample.getKeys().size());
    List<String> allKeys = records.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList());
    assertTrue(allKeys.containsAll(sample.getKeys()));
    // keys past the first thousand are sampled too
    assertTrue(sample.getKeys().stream().anyMatch(key -> allKeys.indexOf(key) >= 1_000));

    KeySample small = HoodieSecondaryIndexEntrySizeEstimator.sampleKeys(records.subList(0, 10).iterator(), 1_000, Long.MAX_VALUE);
    assertEquals(10, small.getCount());
    assertEquals(allKeys.subList(0, 10), small.getKeys());
  }

  @Test
  void samplingStopsGrowingAtTheByteBudget() {
    // keys of "country_N$<uuid>": 10 bytes of secondary key or so, a separator and 36 bytes of record key
    List<HoodieRecord> records = indexRecords(50, 10_000);
    KeySample sample = HoodieSecondaryIndexEntrySizeEstimator.sampleKeys(records.iterator(), 1_000, 4_000);
    assertEquals(10_000, sample.getCount());
    long sampledBytes = sample.getKeys().stream().mapToLong(key -> key.getBytes(StandardCharsets.UTF_8).length).sum();
    // the sample holds about 4000 bytes, at most one key over, of keys about 47 bytes long
    assertTrue(sample.getKeys().size() > 70 && sample.getKeys().size() < 100, "sampled keys: " + sample.getKeys().size());
    assertTrue(sampledBytes > 3_800 && sampledBytes < 4_100, "sampled bytes: " + sampledBytes);
  }

  @Test
  void samplingStaysWithinTheByteBudgetWhenWiderKeysFollow() {
    // a thousand keys of about 47 bytes, then ten thousand of about 4KB
    String wide = String.join("", Collections.nCopies(400, "wide_value"));
    List<HoodieRecord> records = new ArrayList<>(indexRecords(50, 1_000));
    IntStream.range(0, 10_000).forEach(i -> records.add(
        HoodieMetadataPayload.createSecondaryIndexRecord(uuid("w" + i), wide + i, "secondary_index_idx", false)));
    KeySample sample = HoodieSecondaryIndexEntrySizeEstimator.sampleKeys(records.iterator(), 1_000, 64 * 1024);
    assertEquals(11_000, sample.getCount());
    long sampledBytes = sample.getKeys().stream().mapToLong(key -> key.getBytes(StandardCharsets.UTF_8).length).sum();
    assertTrue(sampledBytes <= 64 * 1024 + 4_100, "sampled bytes: " + sampledBytes);
    assertTrue(sample.getKeys().stream().anyMatch(key -> key.startsWith("wide_value")), "the wide keys are sampled");
  }

  @Test
  void countAndSampleCountsEveryRecordAndCapsTheSample() {
    HoodieData<HoodieRecord> records = HoodieListData.eager(indexRecords(50, 250_000));
    KeySample sample = HoodieSecondaryIndexEntrySizeEstimator.countAndSampleKeys(records);
    assertEquals(250_000, sample.getCount());
    assertEquals(HoodieSecondaryIndexEntrySizeEstimator.SAMPLE_KEY_COUNT, sample.getKeys().size());
  }

  @Test
  void entriesOfAFewSecondaryKeysCostAboutTheirRecordKeys() {
    int fewSecondaryKeys = estimate(indexRecords(50, 100_000));
    int uniqueSecondaryKeys = estimate(indexRecords(100_000, 100_000));
    // a uuid record key takes about 20 bytes once compressed; a unique uuid secondary key about as much again
    assertTrue(fewSecondaryKeys > 18 && fewSecondaryKeys < 30, "few secondary keys: " + fewSecondaryKeys);
    assertTrue(uniqueSecondaryKeys > fewSecondaryKeys + 15, "unique secondary keys: " + uniqueSecondaryKeys);
  }

  @Test
  void noKeysFallBackToTheFixedEntrySize() {
    assertEquals(RECORD_INDEX_AVERAGE_RECORD_SIZE, HoodieSecondaryIndexEntrySizeEstimator.estimateEntrySize(Collections.emptyList()));
  }

  private static int estimate(List<HoodieRecord> records) {
    return HoodieSecondaryIndexEntrySizeEstimator.estimateEntrySize(
        records.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()));
  }

  /**
   * Secondary index records of uuid record keys, whose secondary keys take the given number of distinct values: a
   * country-like code when there are few, a uuid when there are as many as the records.
   */
  private static List<HoodieRecord> indexRecords(int distinctSecondaryKeys, int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> {
          int secondary = i % distinctSecondaryKeys;
          String secondaryKey = distinctSecondaryKeys >= count ? uuid("s" + secondary) : "country_" + secondary;
          return (HoodieRecord) HoodieMetadataPayload.createSecondaryIndexRecord(uuid("r" + i), secondaryKey,
              "secondary_index_idx", false);
        })
        .collect(Collectors.toList());
  }

  private static String uuid(String seed) {
    return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
  }
}
