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

import org.apache.hudi.common.bloom.filter.BloomFilter;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Takes a bunch of keys and returns ones that are present in the file group.
 */
public class HoodieKeyLookupHandle<T extends HoodieRecordPayload> extends HoodieReadHandle<T> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieKeyLookupHandle.class);

  private final HoodieTableType tableType;

  private final BloomFilter bloomFilter;

  private final List<String> candidateRecordKeys;

  private long totalKeysChecked;

  public HoodieKeyLookupHandle(HoodieWriteConfig config, HoodieTable<T> hoodieTable,
                               Pair<String, String> partitionPathFilePair) {
    super(config, null, hoodieTable, partitionPathFilePair);
    this.tableType = hoodieTable.getMetaClient().getTableType();
    this.candidateRecordKeys = new ArrayList<>();
    this.totalKeysChecked = 0;
    HoodieTimer timer = new HoodieTimer().startTimer();
    this.bloomFilter = ParquetUtils.readBloomFilterFromParquetMetadata(hoodieTable.getHadoopConf(),
        new Path(getLatestDataFile().getPath()));
    LOG.info("Read bloom filter from {} in {} ms", partitionPathFilePair, timer.endTimer());
  }

  /**
   * Given a list of row keys and one file, return only row keys existing in that file.
   */
  public static List<String> checkCandidatesAgainstFile(Configuration configuration, List<String> candidateRecordKeys,
                                                        Path filePath) throws HoodieIndexException {
    List<String> foundRecordKeys = new ArrayList<>();
    try {
      // Load all rowKeys from the file, to double-confirm
      if (!candidateRecordKeys.isEmpty()) {
        HoodieTimer timer = new HoodieTimer().startTimer();
        Set<String> fileRowKeys =
            ParquetUtils.filterParquetRowKeys(configuration, filePath, new HashSet<>(candidateRecordKeys));
        foundRecordKeys.addAll(fileRowKeys);
        LOG.info(String.format("Checked keys against file %s, in %d ms. #candidates (%d) #found (%d)", filePath,
            timer.endTimer(), candidateRecordKeys.size(), foundRecordKeys.size()));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Keys matching for file {} => {}", filePath, foundRecordKeys);
        }
      }
    } catch (Exception e) {
      throw new HoodieIndexException("Error checking candidate keys against file.", e);
    }
    return foundRecordKeys;
  }

  /**
   * Adds the key for look up.
   */
  public void addKey(String recordKey) {
    // check record key against bloom filter of current file & add to possible keys if needed
    if (bloomFilter.mightContain(recordKey)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Record key {} matches bloom filter in {}", recordKey, partitionPathFilePair);
      }
      candidateRecordKeys.add(recordKey);
    }
    totalKeysChecked++;
  }

  /**
   * Of all the keys, that were added, return a list of keys that were actually found in the file group.
   */
  public KeyLookupResult getLookupResult() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("#The candidate row keys for {} => {}", partitionPathFilePair, candidateRecordKeys);
    }

    HoodieDataFile dataFile = getLatestDataFile();
    List<String> matchingKeys =
        checkCandidatesAgainstFile(hoodieTable.getHadoopConf(), candidateRecordKeys, new Path(dataFile.getPath()));
    LOG.info("Total records ({}), bloom filter candidates ({})/fp({}), actual matches ({})", totalKeysChecked,
                    candidateRecordKeys.size(), candidateRecordKeys.size() - matchingKeys.size(), matchingKeys.size());
    return new KeyLookupResult(partitionPathFilePair.getRight(), partitionPathFilePair.getLeft(),
        dataFile.getCommitTime(), matchingKeys);
  }

  /**
   * Encapsulates the result from a key lookup.
   */
  public static class KeyLookupResult {

    private final String fileId;
    private final String baseInstantTime;
    private final List<String> matchingRecordKeys;
    private final String partitionPath;

    public KeyLookupResult(String fileId, String partitionPath, String baseInstantTime,
                           List<String> matchingRecordKeys) {
      this.fileId = fileId;
      this.partitionPath = partitionPath;
      this.baseInstantTime = baseInstantTime;
      this.matchingRecordKeys = matchingRecordKeys;
    }

    public String getFileId() {
      return fileId;
    }

    public String getBaseInstantTime() {
      return baseInstantTime;
    }

    public String getPartitionPath() {
      return partitionPath;
    }

    public List<String> getMatchingRecordKeys() {
      return matchingRecordKeys;
    }
  }
}
