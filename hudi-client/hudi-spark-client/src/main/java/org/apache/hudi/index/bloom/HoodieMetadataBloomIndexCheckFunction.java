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

package org.apache.hudi.index.bloom;

import org.apache.hudi.client.utils.LazyIterableIterator;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.hadoop.CachingPath;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.HoodieKeyLookupResult;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.Tuple2;

/**
 * Spark Function2 implementation for checking bloom filters for the
 * requested keys from the metadata table index. The bloom filter
 * checking for keys and the actual file verification for the
 * candidate keys is done in an iterative fashion. In each iteration,
 * bloom filters are requested for a batch of partition files and the
 * keys are checked against them.
 */
public class HoodieMetadataBloomIndexCheckFunction implements
    Function2<Integer, Iterator<Tuple2<Tuple2<String, String>, HoodieKey>>, Iterator<List<HoodieKeyLookupResult>>> {

  private static final Logger LOG = LogManager.getLogger(HoodieMetadataBloomIndexCheckFunction.class);

  private final HoodieTable hoodieTable;
  private final String basePath;
  private final int batchSize;

  public HoodieMetadataBloomIndexCheckFunction(HoodieTable hoodieTable, String basePath, int batchSize) {
    this.hoodieTable = hoodieTable;
    this.basePath = basePath;
    this.batchSize = batchSize;
  }

  @Override
  public Iterator<List<HoodieKeyLookupResult>> call(Integer integer, Iterator<Tuple2<Tuple2<String, String>, HoodieKey>> tuple2Iterator) throws Exception {
    return new BloomIndexLazyKeyCheckIterator(tuple2Iterator);
  }

  private class BloomIndexLazyKeyCheckIterator extends LazyIterableIterator<Tuple2<Tuple2<String, String>, HoodieKey>, List<HoodieKeyLookupResult>> {

    private final Set<String> processedFileIdSet = new HashSet<>();

    public BloomIndexLazyKeyCheckIterator(Iterator<Tuple2<Tuple2<String, String>, HoodieKey>> tuple2Iterator) {
      super(tuple2Iterator);
    }

    @Override
    protected void start() {
    }

    @Override
    protected List<HoodieKeyLookupResult> computeNext() {
      // Partition path and file name pair to list of keys
      final Map<Pair<String, String>, List<HoodieKey>> batchFileToKeysMap = new HashMap<>();
      final List<HoodieKeyLookupResult> resultList = new ArrayList<>();
      String lastFileId = null;

      try {
        // Here we batch process the lookup of bloom filters in metadata table
        // assuming the partition path and file name pairs are already sorted by the corresponding key
        while (inputItr.hasNext()) {
          Tuple2<Tuple2<String, String>, HoodieKey> entry = inputItr.next();
          final String partitionPath = entry._2.getPartitionPath();
          final String fileId = entry._1._1();
          final String filename = entry._1._2();

          if (lastFileId == null || !lastFileId.equals(fileId)) {
            if (processedFileIdSet.contains(fileId)) {
              LOG.warn(String.format("Fetching the bloom filter for file ID %s again.  "
                  + " The input pairs of file ID and record key are not sorted.", fileId));
            }
            lastFileId = fileId;
            processedFileIdSet.add(fileId);
          }

          batchFileToKeysMap.computeIfAbsent(Pair.of(partitionPath, filename), k -> new ArrayList<>()).add(entry._2);

          if (batchFileToKeysMap.size() == batchSize) {
            resultList.addAll(lookupKeysInBloomFilters(batchFileToKeysMap));
            batchFileToKeysMap.clear();
          }
        }

        if (batchFileToKeysMap.size() > 0) {
          resultList.addAll(lookupKeysInBloomFilters(batchFileToKeysMap));
          batchFileToKeysMap.clear();
        }

        return resultList;
      } catch (Throwable e) {
        if (e instanceof HoodieException) {
          throw e;
        }
        throw new HoodieIndexException("Error checking bloom filter using metadata table.", e);
      }
    }

    @Override
    protected void end() {
    }

    private List<HoodieKeyLookupResult> lookupKeysInBloomFilters(
        Map<Pair<String, String>, List<HoodieKey>> fileToKeysMap) {
      List<HoodieKeyLookupResult> resultList = new ArrayList<>();
      List<Pair<String, String>> partitionPathFileNameList = new ArrayList<>(fileToKeysMap.keySet());
      HoodieTimer timer = HoodieTimer.start();
      Map<Pair<String, String>, BloomFilter> fileToBloomFilterMap =
          hoodieTable.getMetadataTable().getBloomFilters(partitionPathFileNameList);
      LOG.error(String.format("Took %d ms to look up %s bloom filters",
          timer.endTimer(), partitionPathFileNameList.size()));

      fileToKeysMap.forEach((partitionPathFileNamePair, hoodieKeyList) -> {
        final String partitionPath = partitionPathFileNamePair.getLeft();
        final String fileName = partitionPathFileNamePair.getRight();
        final String fileId = FSUtils.getFileId(fileName);
        ValidationUtils.checkState(!fileId.isEmpty());

        if (!fileToBloomFilterMap.containsKey(partitionPathFileNamePair)) {
          throw new HoodieIndexException("Failed to get the bloom filter for " + partitionPathFileNamePair);
        }
        final BloomFilter fileBloomFilter = fileToBloomFilterMap.get(partitionPathFileNamePair);

        List<String> candidateRecordKeys = new ArrayList<>();
        hoodieKeyList.forEach(hoodieKey -> {
          if (fileBloomFilter.mightContain(hoodieKey.getRecordKey())) {
            candidateRecordKeys.add(hoodieKey.getRecordKey());
          }
        });

        List<String> matchingKeys =
            HoodieIndexUtils.filterKeysFromFile(
                new CachingPath(FSUtils.getPartitionPath(basePath, partitionPath), fileName),
                candidateRecordKeys,
                hoodieTable.getHadoopConf());
        LOG.debug(
            String.format("Total records (%d), bloom filter candidates (%d)/fp(%d), actual matches (%d)",
                hoodieKeyList.size(), candidateRecordKeys.size(),
                candidateRecordKeys.size() - matchingKeys.size(), matchingKeys.size()));

        resultList.add(new HoodieKeyLookupResult(fileId, partitionPath, FSUtils.getCommitTime(fileName), matchingKeys));
      });
      return resultList;
    }
  }
}
