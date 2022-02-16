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

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.bloom.HoodieDynamicBoundedBloomFilter;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Takes a bunch of keys and returns ones that are present in the file group.
 */
public class HoodieKeyLookupHandle<T extends HoodieRecordPayload, I, K, O> extends HoodieReadHandle<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(HoodieKeyLookupHandle.class);

  private final BloomFilter bloomFilter;
  private final List<String> candidateRecordKeys;
  private final boolean useMetadataTableIndex;
  private Option<String> fileName = Option.empty();
  private long totalKeysChecked;

  public HoodieKeyLookupHandle(HoodieWriteConfig config, HoodieTable<T, I, K, O> hoodieTable,
                               Pair<String, String> partitionPathFileIDPair) {
    this(config, hoodieTable, partitionPathFileIDPair, Option.empty(), false);
  }

  public HoodieKeyLookupHandle(HoodieWriteConfig config, HoodieTable<T, I, K, O> hoodieTable,
                               Pair<String, String> partitionPathFileIDPair, Option<String> fileName,
                               boolean useMetadataTableIndex) {
    super(config, hoodieTable, partitionPathFileIDPair);
    this.candidateRecordKeys = new ArrayList<>();
    this.totalKeysChecked = 0;
    if (fileName.isPresent()) {
      ValidationUtils.checkArgument(FSUtils.getFileId(fileName.get()).equals(getFileId()),
          "File name '" + fileName.get() + "' doesn't match this lookup handle fileid '" + getFileId() + "'");
      this.fileName = fileName;
    }
    this.useMetadataTableIndex = useMetadataTableIndex;
    this.bloomFilter = getBloomFilter();
  }

  private BloomFilter getBloomFilter() {
    BloomFilter bloomFilter = null;
    HoodieTimer timer = new HoodieTimer().startTimer();
    try {
      if (this.useMetadataTableIndex) {
        ValidationUtils.checkArgument(this.fileName.isPresent(),
            "File name not available to fetch bloom filter from the metadata table index.");
        Option<ByteBuffer> bloomFilterByteBuffer =
            hoodieTable.getMetadataTable().getBloomFilter(partitionPathFileIDPair.getLeft(), fileName.get());
        if (!bloomFilterByteBuffer.isPresent()) {
          throw new HoodieIndexException("BloomFilter missing for " + partitionPathFileIDPair.getRight());
        }
        bloomFilter =
            new HoodieDynamicBoundedBloomFilter(StandardCharsets.UTF_8.decode(bloomFilterByteBuffer.get()).toString(),
                BloomFilterTypeCode.DYNAMIC_V0);
      } else {
        try (HoodieFileReader reader = createNewFileReader()) {
          bloomFilter = reader.readBloomFilter();
        }
      }
    } catch (IOException e) {
      throw new HoodieIndexException(String.format("Error reading bloom filter from %s/%s - %s",
          getPartitionPathFileIDPair().getLeft(), this.fileName, e));
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
