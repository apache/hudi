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

package org.apache.hudi.io;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;

/**
 * Takes a bunch of keys and returns ones that are present in the file group.
 */
public class HoodieKeyLookupHandle<T, I, K, O> extends HoodieReadHandle<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(HoodieKeyLookupHandle.class);

  private final BloomFilter bloomFilter;
  private final List<String> candidateRecordKeys;
  private long totalKeysChecked;

  public HoodieKeyLookupHandle(HoodieWriteConfig config, HoodieTable<T, I, K, O> hoodieTable,
                               Pair<String, String> partitionPathFileIDPair) {
    super(config, hoodieTable, partitionPathFileIDPair);
    this.candidateRecordKeys = new ArrayList<>();
    this.totalKeysChecked = 0;
    this.bloomFilter = getBloomFilter();
  }

  private BloomFilter getBloomFilter() {
    BloomFilter bloomFilter = null;
    HoodieTimer timer = new HoodieTimer().startTimer();
    try {
      if (config.getBloomIndexUseMetadata()
          && hoodieTable.getMetaClient().getTableConfig().getMetadataPartitions()
          .contains(BLOOM_FILTERS.getPartitionPath())) {
        bloomFilter = hoodieTable.getMetadataTable().getBloomFilter(partitionPathFileIDPair.getLeft(), partitionPathFileIDPair.getRight())
            .orElseThrow(() -> new HoodieIndexException("BloomFilter missing for " + partitionPathFileIDPair.getRight()));
      } else {
        try (HoodieFileReader reader = createNewFileReader()) {
          bloomFilter = reader.readBloomFilter();
        }
      }
    } catch (IOException e) {
      throw new HoodieIndexException(String.format("Error reading bloom filter from %s", getPartitionPathFileIDPair()), e);
    }
    LOG.info(String.format("Read bloom filter from %s in %d ms", partitionPathFileIDPair, timer.endTimer()));
    return bloomFilter;
  }

  /**
   * Adds the key for look up.
   */
  public void addKey(String recordKey) {
    // check record key against bloom filter of current file & add to possible keys if needed
    if (bloomFilter.mightContain(recordKey)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Record key " + recordKey + " matches bloom filter in  " + partitionPathFileIDPair);
      }
      candidateRecordKeys.add(recordKey);
    }
    totalKeysChecked++;
  }

  /**
   * Of all the keys, that were added, return a list of keys that were actually found in the file group.
   */
  public HoodieKeyLookupResult getLookupResult() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("#The candidate row keys for " + partitionPathFileIDPair + " => " + candidateRecordKeys);
    }

    HoodieBaseFile dataFile = getLatestDataFile();
    List<String> matchingKeys = HoodieIndexUtils.filterKeysFromFile(new Path(dataFile.getPath()), candidateRecordKeys,
        hoodieTable.getHadoopConf());
    LOG.info(
        String.format("Total records (%d), bloom filter candidates (%d)/fp(%d), actual matches (%d)", totalKeysChecked,
            candidateRecordKeys.size(), candidateRecordKeys.size() - matchingKeys.size(), matchingKeys.size()));
    return new HoodieKeyLookupResult(partitionPathFileIDPair.getRight(), partitionPathFileIDPair.getLeft(),
        dataFile.getCommitTime(), matchingKeys);
  }
}
