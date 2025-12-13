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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.FlatteningIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * Implementation of the function that probing Bloom Filters of individual files verifying
 * whether particular record key could be stored in the latest file-slice of the file-group
 * identified by the {@link HoodieFileGroupId}
 */
@Slf4j
public class HoodieMetadataBloomFilterProbingFunction implements
    PairFlatMapFunction<Iterator<Tuple2<HoodieFileGroupId, String>>, HoodieFileGroupId, HoodieBloomFilterProbingResult> {

  // Assuming each file bloom filter takes up 512K, sizing the max file count
  // per batch so that the total fetched bloom filters would not cross 128 MB.
  private static final long BLOOM_FILTER_CHECK_MAX_FILE_COUNT_PER_BATCH = 256;
  private final HoodieTable hoodieTable;

  private final Broadcast<HoodieTableFileSystemView> baseFileOnlyViewBroadcast;

  /**
   * NOTE: It's critical for this ctor to accept {@link HoodieTable} to make sure that it uses
   *       broadcast-ed instance of {@link HoodieBackedTableMetadata} internally, instead of
   *       one being serialized and deserialized for _every_ task individually
   *
   * NOTE: We pass in broadcasted {@link HoodieTableFileSystemView} to make sure it's materialized
   *       on executor once
   */
  public HoodieMetadataBloomFilterProbingFunction(Broadcast<HoodieTableFileSystemView> baseFileOnlyViewBroadcast,
                                                  HoodieTable hoodieTable) {
    this.baseFileOnlyViewBroadcast = baseFileOnlyViewBroadcast;
    this.hoodieTable = hoodieTable;
  }

  @Override
  public Iterator<Tuple2<HoodieFileGroupId, HoodieBloomFilterProbingResult>> call(Iterator<Tuple2<HoodieFileGroupId, String>> tuple2Iterator) throws Exception {
    return new FlatteningIterator<>(new BloomIndexLazyKeyCheckIterator(tuple2Iterator));
  }

  private class BloomIndexLazyKeyCheckIterator
      extends LazyIterableIterator<Tuple2<HoodieFileGroupId, String>, Iterator<Tuple2<HoodieFileGroupId, HoodieBloomFilterProbingResult>>> {

    public BloomIndexLazyKeyCheckIterator(Iterator<Tuple2<HoodieFileGroupId, String>> tuple2Iterator) {
      super(tuple2Iterator);
    }

    @Override
    protected Iterator<Tuple2<HoodieFileGroupId, HoodieBloomFilterProbingResult>> computeNext() {
      // Partition path and file name pair to list of keys
      final Map<Pair<String, HoodieBaseFile>, List<HoodieKey>> fileToKeysMap = new HashMap<>();
      final Map<String, HoodieBaseFile> fileIDBaseFileMap = new HashMap<>();

      while (inputItr.hasNext()) {
        Tuple2<HoodieFileGroupId, String> entry = inputItr.next();
        String partitionPath = entry._1.getPartitionPath();
        String fileId = entry._1.getFileId();

        if (!fileIDBaseFileMap.containsKey(fileId)) {
          Option<HoodieBaseFile> baseFile = baseFileOnlyViewBroadcast.getValue().getLatestBaseFile(partitionPath, fileId);
          if (!baseFile.isPresent()) {
            throw new HoodieIndexException("Failed to find the base file for partition: " + partitionPath
                + ", fileId: " + fileId);
          }
          fileIDBaseFileMap.put(fileId, baseFile.get());
        }

        fileToKeysMap.computeIfAbsent(Pair.of(partitionPath, fileIDBaseFileMap.get(fileId)),
            k -> new ArrayList<>()).add(new HoodieKey(entry._2, partitionPath));

        if (fileToKeysMap.size() > BLOOM_FILTER_CHECK_MAX_FILE_COUNT_PER_BATCH) {
          break;
        }
      }

      if (fileToKeysMap.isEmpty()) {
        return Collections.emptyIterator();
      }

      List<Pair<String, String>> partitionNameFileNameList = fileToKeysMap.keySet().stream().map(pair -> Pair.of(pair.getLeft(), pair.getRight().getFileName())).collect(Collectors.toList());
      Map<Pair<String, String>, BloomFilter> fileToBloomFilterMap =
          hoodieTable.getMetadataTable().getBloomFilters(partitionNameFileNameList);

      return fileToKeysMap.entrySet().stream()
          .map(entry -> {
            List<HoodieKey> hoodieKeyList = entry.getValue();
            final String partitionPath = entry.getKey().getLeft();
            final HoodieBaseFile baseFile = entry.getKey().getRight();
            final String fileId = baseFile.getFileId();
            ValidationUtils.checkState(!fileId.isEmpty());

            Pair<String, String> partitionPathFileNamePair = Pair.of(partitionPath, baseFile.getFileName());
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

            log.debug("Total records ({}), bloom filter candidates ({})",
                hoodieKeyList.size(), candidateRecordKeys.size());

            return Tuple2.apply(new HoodieFileGroupId(partitionPath, fileId), new HoodieBloomFilterProbingResult(candidateRecordKeys));
          })
          .iterator();
    }
  }
}
