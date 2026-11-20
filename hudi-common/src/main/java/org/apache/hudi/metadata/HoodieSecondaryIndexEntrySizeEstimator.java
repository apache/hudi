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
import org.apache.hudi.common.model.HoodieRecord;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.RECORD_INDEX_AVERAGE_RECORD_SIZE;

/**
 * Estimates the bytes an entry takes in a secondary index, so that its file group count follows the keys it holds rather
 * than a fixed entry size.
 *
 * <p>A secondary index entry maps a key joining the secondary key and the record key to a value that only marks it
 * deleted or not, so its size is that of its key once compressed among the keys around it, which depends on how many
 * entries share a secondary key. The index records are counted before the index is written, and that same pass keeps a
 * sample of their keys, which are laid out as the index HFiles lay out their entries and compressed in HFile-sized GZ
 * blocks. A sample shares fewer key prefixes with its neighbours than all the keys do, so the estimate errs on the large
 * side.
 */
@Slf4j
public class HoodieSecondaryIndexEntrySizeEstimator {

  // Index keys kept to estimate the entry size, across all partitions of the index records.
  static final int SAMPLE_KEY_COUNT = 100_000;

  // UTF-8 bytes of index keys kept, across all partitions, so that wide secondary keys do not overwhelm the driver.
  static final long SAMPLE_KEY_BYTES = 64L * 1024 * 1024;

  // Fixed seed, so that the same records always sample the same keys.
  private static final long SAMPLE_SEED = 42L;

  /**
   * Counts the index records and samples up to about {@link #SAMPLE_KEY_COUNT} of their keys, holding up to about
   * {@link #SAMPLE_KEY_BYTES} bytes, in one pass.
   */
  public static KeySample countAndSampleKeys(HoodieData<HoodieRecord> records) {
    int partitions = Math.max(1, records.getNumPartitions());
    int keysPerPartition = (int) Math.ceil((double) SAMPLE_KEY_COUNT / partitions);
    long bytesPerPartition = (long) Math.ceil((double) SAMPLE_KEY_BYTES / partitions);
    List<KeySample> partitionSamples = records.mapPartitions(recordIterator ->
        Collections.singletonList(sampleKeys(recordIterator, keysPerPartition, bytesPerPartition)).iterator(), true).collectAsList();
    long count = partitionSamples.stream().mapToLong(KeySample::getCount).sum();
    List<String> keys = partitionSamples.stream().flatMap(sample -> sample.getKeys().stream()).collect(Collectors.toList());
    return new KeySample(count, keys);
  }

  /**
   * Counts the records and keeps a uniform sample of up to {@code maxKeys} of their keys, holding at most
   * {@code maxBytes} UTF-8 bytes beyond its last key. The sample stops growing once its keys reach that budget, and
   * drops keys whenever wider keys replacing others take it over the budget.
   */
  static KeySample sampleKeys(Iterator<HoodieRecord> records, int maxKeys, long maxBytes) {
    Random random = new Random(SAMPLE_SEED);
    List<String> keys = new ArrayList<>(Math.min(maxKeys, 1024));
    int capacity = maxKeys;
    long sampledBytes = 0;
    long count = 0;
    while (records.hasNext()) {
      String key = records.next().getRecordKey();
      if (keys.size() < capacity) {
        keys.add(key);
        sampledBytes += key.getBytes(StandardCharsets.UTF_8).length;
        if (sampledBytes >= maxBytes) {
          capacity = keys.size();
        }
      } else {
        long slot = (long) (random.nextDouble() * (count + 1));
        if (slot < capacity) {
          sampledBytes += key.getBytes(StandardCharsets.UTF_8).length
              - keys.set((int) slot, key).getBytes(StandardCharsets.UTF_8).length;
          // drop keys from the end until the sample fits its budget again
          while (sampledBytes > maxBytes && keys.size() > 1) {
            sampledBytes -= keys.remove(keys.size() - 1).getBytes(StandardCharsets.UTF_8).length;
          }
          capacity = keys.size();
        }
      }
      count++;
    }
    return new KeySample(count, keys);
  }

  /**
   * Estimates the size of a secondary index entry from a sample of the index keys.
   *
   * @param indexKeys sampled keys of the index records, joining the secondary key and the record key
   * @return the estimated bytes per entry, or {@link HoodieTableMetadataUtil#RECORD_INDEX_AVERAGE_RECORD_SIZE} when
   *         there are no keys to sample
   */
  public static int estimateEntrySize(List<String> indexKeys) {
    if (indexKeys.isEmpty()) {
      return RECORD_INDEX_AVERAGE_RECORD_SIZE;
    }
    // every entry of the index is written with the same value, as the index is initialized with live records only
    byte[] value = HoodieIndexEntrySample.serializeValue(HoodieMetadataPayload.createSecondaryIndexRecord("", "", "", false));
    List<HoodieIndexEntrySample> entries = indexKeys.stream()
        .map(key -> new HoodieIndexEntrySample(key.getBytes(StandardCharsets.UTF_8), value))
        .sorted(HoodieIndexEntrySample.HFILE_KEY_ORDER)
        .collect(Collectors.toList());
    int entrySize = Math.max(1, (int) Math.ceil((double) HoodieIndexEntrySample.compressedSize(entries, true) / entries.size()));
    log.info("Estimating secondary index entries at {} bytes from {} sampled keys", entrySize, entries.size());
    return entrySize;
  }

  /**
   * The count of the index records and a sample of their keys.
   */
  @Value
  public static class KeySample implements Serializable {
    private static final long serialVersionUID = 1L;

    long count;
    List<String> keys;
  }
}
