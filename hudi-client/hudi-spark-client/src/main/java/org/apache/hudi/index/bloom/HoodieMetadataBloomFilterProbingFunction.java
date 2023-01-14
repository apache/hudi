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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Spark Function2 implementation for checking bloom filters for the
 * requested keys from the metadata table index. The bloom filter
 * checking for keys and the actual file verification for the
 * candidate keys is done in an iterative fashion. In each iteration,
 * bloom filters are requested for a batch of partition files and the
 * keys are checked against them.
 */
public class HoodieMetadataBloomFilterProbingFunction implements
    PairFlatMapFunction<Iterator<Tuple2<String, HoodieKey>>, String, HoodieBloomFilterKeyLookupResult> {

  private static final Logger LOG = LogManager.getLogger(HoodieMetadataBloomFilterProbingFunction.class);

  // Assuming each file bloom filter takes up 512K, sizing the max file count
  // per batch so that the total fetched bloom filters would not cross 128 MB.
  private static final long BLOOM_FILTER_CHECK_MAX_FILE_COUNT_PER_BATCH = 256;
  private final HoodieTable hoodieTable;


  public HoodieMetadataBloomFilterProbingFunction(HoodieTable hoodieTable) {
    this.hoodieTable = hoodieTable;
  }

  @Override
  public Iterator<Tuple2<String, HoodieBloomFilterKeyLookupResult>> call(Iterator<Tuple2<String, HoodieKey>> tuple2Iterator) throws Exception {
    return new FlattenedIterator<>(new BloomIndexLazyKeyCheckIterator(tuple2Iterator), Function.identity());
  }

  private class BloomIndexLazyKeyCheckIterator
      extends LazyIterableIterator<Tuple2<String, HoodieKey>, Iterator<Tuple2<String, HoodieBloomFilterKeyLookupResult>>> {

    private final TableFileSystemView.BaseFileOnlyView baseFileOnlyView = hoodieTable.getBaseFileOnlyView();

    public BloomIndexLazyKeyCheckIterator(Iterator<Tuple2<String, HoodieKey>> tuple2Iterator) {
      super(tuple2Iterator);
    }

    @Override
    protected Iterator<Tuple2<String, HoodieBloomFilterKeyLookupResult>> computeNext() {
      // Partition path and file name pair to list of keys
      final Map<Pair<String, String>, List<HoodieKey>> fileToKeysMap = new HashMap<>();
      final Map<String, HoodieBaseFile> fileIDBaseFileMap = new HashMap<>();

      while (inputItr.hasNext()) {
        Tuple2<String, HoodieKey> entry = inputItr.next();
        final String partitionPath = entry._2.getPartitionPath();
        final String fileId = entry._1;
        if (!fileIDBaseFileMap.containsKey(fileId)) {
          Option<HoodieBaseFile> baseFile = baseFileOnlyView.getLatestBaseFile(partitionPath, fileId);
          if (!baseFile.isPresent()) {
            throw new HoodieIndexException("Failed to find the base file for partition: " + partitionPath
                + ", fileId: " + fileId);
          }
          fileIDBaseFileMap.put(fileId, baseFile.get());
        }
        fileToKeysMap.computeIfAbsent(Pair.of(partitionPath, fileIDBaseFileMap.get(fileId).getFileName()),
            k -> new ArrayList<>()).add(entry._2);

        if (fileToKeysMap.size() > BLOOM_FILTER_CHECK_MAX_FILE_COUNT_PER_BATCH) {
          break;
        }
      }

      if (fileToKeysMap.isEmpty()) {
        return Collections.emptyIterator();
      }

      List<Pair<String, String>> partitionNameFileNameList = new ArrayList<>(fileToKeysMap.keySet());
      Map<Pair<String, String>, BloomFilter> fileToBloomFilterMap =
          hoodieTable.getMetadataTable().getBloomFilters(partitionNameFileNameList);

      return fileToKeysMap.entrySet().stream()
          .map(entry -> {
            Pair<String, String> partitionPathFileNamePair = entry.getKey();
            List<HoodieKey> hoodieKeyList = entry.getValue();

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

            LOG.debug(String.format("Total records (%d), bloom filter candidates (%d)",
                hoodieKeyList.size(), candidateRecordKeys.size()));

            return Tuple2.apply(fileId, new HoodieBloomFilterKeyLookupResult(fileId, partitionPath, candidateRecordKeys));
          })
          .iterator();
    }
  }
}
