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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.HoodieKeyLookupResult;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * Implementation of the function probing filtered in candidate keys provided in
 * {@link HoodieBloomFilterProbingResult} w/in corresponding files identified by {@link HoodieFileGroupId}
 * to validate whether the record w/ the provided key is indeed persisted in it
 */
@Slf4j
public class HoodieFileProbingFunction implements
    FlatMapFunction<Iterator<Tuple2<HoodieFileGroupId, HoodieBloomFilterProbingResult>>, List<HoodieKeyLookupResult>> {

  // Assuming each file bloom filter takes up 512K, sizing the max file count
  // per batch so that the total fetched bloom filters would not cross 128 MB.
  private static final long BLOOM_FILTER_CHECK_MAX_FILE_COUNT_PER_BATCH = 256;

  private final Broadcast<HoodieTableFileSystemView> baseFileOnlyViewBroadcast;
  private final StorageConfiguration<?> storageConf;

  public HoodieFileProbingFunction(Broadcast<HoodieTableFileSystemView> baseFileOnlyViewBroadcast,
                                   StorageConfiguration<?> storageConf) {
    this.baseFileOnlyViewBroadcast = baseFileOnlyViewBroadcast;
    this.storageConf = storageConf;
  }

  @Override
  public Iterator<List<HoodieKeyLookupResult>> call(Iterator<Tuple2<HoodieFileGroupId, HoodieBloomFilterProbingResult>> tuple2Iterator) throws Exception {
    return new BloomIndexLazyKeyCheckIterator(tuple2Iterator);
  }

  private class BloomIndexLazyKeyCheckIterator
      extends LazyIterableIterator<Tuple2<HoodieFileGroupId, HoodieBloomFilterProbingResult>, List<HoodieKeyLookupResult>> {

    public BloomIndexLazyKeyCheckIterator(Iterator<Tuple2<HoodieFileGroupId, HoodieBloomFilterProbingResult>> tuple2Iterator) {
      super(tuple2Iterator);
    }

    @Override
    protected List<HoodieKeyLookupResult> computeNext() {
      // Partition path and file name pair to list of keys
      final Map<Pair<String, HoodieBaseFile>, HoodieBloomFilterProbingResult> fileToLookupResults = new HashMap<>();
      final Map<String, HoodieBaseFile> fileIDBaseFileMap = new HashMap<>();

      while (inputItr.hasNext()) {
        Tuple2<HoodieFileGroupId, HoodieBloomFilterProbingResult> entry = inputItr.next();
        final String partitionPath = entry._1.getPartitionPath();
        final String fileId = entry._1.getFileId();

        if (!fileIDBaseFileMap.containsKey(fileId)) {
          Option<HoodieBaseFile> baseFile =
              baseFileOnlyViewBroadcast.getValue().getLatestBaseFile(partitionPath, fileId);
          if (!baseFile.isPresent()) {
            throw new HoodieIndexException("Failed to find the base file for partition: " + partitionPath
                + ", fileId: " + fileId);
          }

          fileIDBaseFileMap.put(fileId, baseFile.get());
        }

        fileToLookupResults.putIfAbsent(Pair.of(partitionPath, fileIDBaseFileMap.get(fileId)), entry._2);

        if (fileToLookupResults.size() > BLOOM_FILTER_CHECK_MAX_FILE_COUNT_PER_BATCH) {
          break;
        }
      }

      if (fileToLookupResults.isEmpty()) {
        return Collections.emptyList();
      }

      return fileToLookupResults.entrySet().stream()
          .map(entry -> {
            Pair<String, HoodieBaseFile> partitionPathFileNamePair = entry.getKey();
            HoodieBloomFilterProbingResult bloomFilterKeyLookupResult = entry.getValue();

            final String partitionPath = partitionPathFileNamePair.getLeft();
            final String fileId = partitionPathFileNamePair.getRight().getFileId();
            ValidationUtils.checkState(!fileId.isEmpty());

            List<String> candidateRecordKeys = bloomFilterKeyLookupResult.getCandidateKeys();

            // TODO add assertion that file is checked only once

            final HoodieBaseFile dataFile = fileIDBaseFileMap.get(fileId);
            List<Pair<String, Long>> matchingKeysAndPositions = HoodieIndexUtils.filterKeysFromFile(
                dataFile.getStoragePath(), candidateRecordKeys, HoodieStorageUtils.getStorage(dataFile.getStoragePath(), storageConf));

            log.debug(
                String.format("Bloom filter candidates (%d) / false positives (%d), actual matches (%d)",
                    candidateRecordKeys.size(), candidateRecordKeys.size() - matchingKeysAndPositions.size(),
                    matchingKeysAndPositions.size()));

            return new HoodieKeyLookupResult(fileId, partitionPath, dataFile.getCommitTime(), matchingKeysAndPositions);
          })
          .collect(Collectors.toList());
    }

  }
}
