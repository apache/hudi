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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.HoodieBloomRangeInfoHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Simplified re-implementation of {@link HoodieBloomIndex} that does not rely on caching, or
 * incurs the overhead of auto-tuning parallelism.
 */
public class HoodieGlobalBloomIndexV2<T extends HoodieRecordPayload> extends HoodieBloomIndexV2<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieGlobalBloomIndexV2.class);

  public HoodieGlobalBloomIndexV2(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD,
                                              JavaSparkContext jsc,
                                              HoodieTable<T> hoodieTable) {
    return recordRDD
            .sortBy((record) -> String.format("%s-%s", record.getPartitionPath(), record.getRecordKey()),
                    true, config.getBloomIndexV2Parallelism())
            .mapPartitions((itr) -> new LazyRangeAndBloomChecker(itr, hoodieTable)).flatMap(List::iterator)
            .sortBy(Pair::getRight, true, config.getBloomIndexV2Parallelism())
            .mapPartitions((itr) -> new LazyKeyChecker(itr, hoodieTable))
            .filter(Option::isPresent)
            .map(Option::get);
  }

  /**
   * This is not global, since we depend on the partitionPath to do the lookup.
   */
  @Override
  public boolean isGlobal() {
    return true;
  }

  /**
   * Given an iterator of hoodie records, returns a pair of candidate HoodieRecord, FileID pairs,
   * by filtering for ranges and bloom for all records with all fileIds.
   */
  class LazyRangeAndBloomChecker extends
          LazyIterableIterator<HoodieRecord<T>, List<Pair<HoodieRecord<T>, String>>> {

    private HoodieTable<T> table;
    private List<Pair<String, String>> partitionPathFileIDList;
    private IndexFileFilter indexFileFilter;
    private ExternalSpillableMap<String, BloomFilter> fileIDToBloomFilter;
    private HoodieTimer hoodieTimer;
    private long totalTimeMs;
    private long totalCount;
    private long totalMetadataReadTimeMs;
    private long totalRangeCheckTimeMs;
    private long totalBloomCheckTimeMs;
    private long totalMatches;

    public LazyRangeAndBloomChecker(Iterator<HoodieRecord<T>> in, final HoodieTable<T> table) {
      super(in);
      this.table = table;
    }

    @Override
    protected List<Pair<HoodieRecord<T>, String>> computeNext() {

      List<Pair<HoodieRecord<T>, String>> candidates = new ArrayList<>();
      if (!inputItr.hasNext()) {
        return candidates;
      }

      HoodieRecord<T> record = inputItr.next();

      // <Partition path, file name>
      hoodieTimer.startTimer();
      Set<Pair<String, String>> matchingFiles = indexFileFilter
              .getMatchingFilesAndPartition(record.getPartitionPath(), record.getRecordKey());

      totalRangeCheckTimeMs += hoodieTimer.endTimer();
      hoodieTimer.startTimer();

      matchingFiles.forEach(partitionFileIdPair -> {
        BloomFilter filter = fileIDToBloomFilter.get(partitionFileIdPair.getRight());
        if (filter.mightContain(record.getRecordKey())) {
          totalMatches++;
          candidates.add(Pair.of(updatePartition(record, partitionFileIdPair.getLeft()), partitionFileIdPair.getRight()));
        }
      });
      totalBloomCheckTimeMs += hoodieTimer.endTimer();

      if (candidates.isEmpty()) {
        candidates.add(Pair.of(record, ""));
      }

      totalCount++;
      return candidates;
    }

    @Override
    protected void start() {
      totalTimeMs = 0;
      totalMatches = 0;
      totalCount = 0;
      hoodieTimer = new HoodieTimer().startTimer();

      // init global range bloomFilter
      populateFileIDs();
      populateRangeAndBloomFilters();
      totalMetadataReadTimeMs += hoodieTimer.endTimer();
      hoodieTimer.startTimer();
    }

    @Override
    protected void end() {
      totalTimeMs = hoodieTimer.endTimer();
      String rangeCheckInfo = "LazyRangeAndBloomChecker: "
              + "totalCount: " + totalCount + ", "
              + "totalMatches: " + totalMatches + ", "
              + "totalTimeMs: " + totalTimeMs + "ms, "
              + "totalMetadataReadTimeMs: " + totalMetadataReadTimeMs + "ms, "
              + "totalRangeCheckTimeMs: " + totalRangeCheckTimeMs + "ms, "
              + "totalBloomCheckTimeMs: " + totalBloomCheckTimeMs + "ms";
      LOG.info(rangeCheckInfo);
    }

    private HoodieRecord<T> updatePartition(HoodieRecord<T> record, String partitionPath) {
      HoodieKey hoodieKey = new HoodieKey(record.getKey().getRecordKey(), partitionPath);
      return new HoodieRecord<>(hoodieKey, record.getData());
    }

    private void populateFileIDs() {
      try {
        HoodieTableMetaClient metaClient = table.getMetaClient();
        List<String> partitions = FSUtils.getAllPartitionPaths(metaClient.getFs(), metaClient.getBasePath(),
                config.shouldAssumeDatePartitioning());

        // Obtain the latest data files from all the partitions.
        this.partitionPathFileIDList = partitions.stream().flatMap(partition -> {
          Option<HoodieInstant> latestCommitTime =
                  table.getMetaClient().getCommitsTimeline().filterCompletedInstants().lastInstant();
          List<Pair<String, String>> filteredFiles = new ArrayList<>();
          if (latestCommitTime.isPresent()) {
            filteredFiles = table.getBaseFileOnlyView()
                    .getLatestBaseFilesBeforeOrOn(partition, latestCommitTime.get().getTimestamp())
                    .map(f -> Pair.of(partition, f.getFileId())).collect(Collectors.toList());
          }
          return filteredFiles.stream();
        }).collect(Collectors.toList());
      } catch (IOException e) {
        throw new HoodieIOException("Failed to populateFileIDs", e);
      }
    }

    private void populateRangeAndBloomFilters() {
      try {
        // fileId to BloomFilter
        this.fileIDToBloomFilter = new ExternalSpillableMap<>(config.getBloomIndexV2BufferMaxSize(),
                config.getSpillableMapBasePath(), new DefaultSizeEstimator<>(), new DefaultSizeEstimator<>());

        List<Tuple2<String, BloomIndexFileInfo>> fileInfoList = partitionPathFileIDList.stream()
                .map(partitionFileID -> {
                  HoodieBloomRangeInfoHandle<T> indexMetadataHandle = new HoodieBloomRangeInfoHandle<T>(
                          config, table, Pair.of(partitionFileID.getLeft(), partitionFileID.getRight()));
                  this.fileIDToBloomFilter.put(partitionFileID.getRight(), indexMetadataHandle.getBloomFilter());
                  return new Tuple2<>(partitionFileID.getLeft(), indexMetadataHandle.getRangeInfo());
                }).collect(Collectors.toList());

        Map<String, List<BloomIndexFileInfo>> partitionToFileInfo =
                fileInfoList.stream().collect(Collectors.groupingBy(Tuple2::_1, Collectors.mapping(Tuple2::_2, Collectors.toList())));

        this.indexFileFilter = new IntervalTreeBasedGlobalIndexFileFilter(partitionToFileInfo);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to populateRangeAndBloomFilters", e);
      }
    }
  }
}