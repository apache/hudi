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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.LazyIterableIterator;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.HoodieBloomRangeInfoHandle;
import org.apache.hudi.io.HoodieKeyLookupHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Simplified re-implementation of {@link HoodieBloomIndex} that does not rely on caching, or
 * incurs the overhead of auto-tuning parallelism.
 */
public class HoodieBloomIndexV2<T extends HoodieRecordPayload> extends HoodieIndex<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieBloomIndexV2.class);

  public HoodieBloomIndexV2(HoodieWriteConfig config) {
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
   * Checks if the given [Keys] exists in the hoodie table and returns [Key, Option[partitionPath, fileID]] If the
   * optional is empty, then the key is not found.
   */
  @Override
  public JavaPairRDD<HoodieKey, Option<Pair<String, String>>> fetchRecordLocation(JavaRDD<HoodieKey> hoodieKeys,
                                                                                  JavaSparkContext jsc, HoodieTable<T> hoodieTable) {

    // map input records
    JavaRDD<HoodieRecord<T>> inputRecords = hoodieKeys.map(key -> new HoodieRecord(key, null));
    JavaRDD<HoodieRecord<T>> hoodieRecordJavaRDD = tagLocation(inputRecords, jsc, hoodieTable);

    return hoodieRecordJavaRDD.mapToPair(record -> {
      final HoodieKey key = record.getKey();
      final String partitionPath = record.getPartitionPath();
      final HoodieRecordLocation location = record.getCurrentLocation();

      // current location of record on storage, null means the given record not exists in the hoodie table
      Option<Pair<String, String>> partitionPathFileidPair;
      if (location == null) {
        partitionPathFileidPair = Option.empty();
      } else {
        partitionPathFileidPair = Option.of(Pair.of(partitionPath, location.getFileId()));
      }

      return new Tuple2<>(key, partitionPathFileidPair);
    });
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    // Nope, don't need to do anything.
    return true;
  }

  /**
   * This is not global, since we depend on the partitionPath to do the lookup.
   */
  @Override
  public boolean isGlobal() {
    return false;
  }

  /**
   * No indexes into log files yet.
   */
  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  /**
   * Bloom filters are stored, into the same data files.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, JavaSparkContext jsc,
                                             HoodieTable<T> hoodieTable) {
    return writeStatusRDD;
  }

  /**
   * Given an iterator of hoodie records, returns a pair of candidate HoodieRecord, FileID pairs,
   * by filtering for ranges and bloom for all records with all fileIds.
   *
   */
  class LazyRangeAndBloomChecker extends
          LazyIterableIterator<HoodieRecord<T>, List<Pair<HoodieRecord<T>, String>>> {

    private HoodieTable<T> table;
    private String currentPartitionPath;
    private Set<String> fileIDs;
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
      try {
        hoodieTimer.startTimer();
        initIfNeeded(record.getPartitionPath());
        totalMetadataReadTimeMs += hoodieTimer.endTimer();
        hoodieTimer.startTimer();
      } catch (IOException e) {
        throw new HoodieIOException("Error reading index metadata for " + record.getPartitionPath(), e);
      }

      // <Partition path, file name>
      Set<Pair<String, String>> matchingFiles = indexFileFilter
              .getMatchingFilesAndPartition(record.getPartitionPath(), record.getRecordKey());

      totalRangeCheckTimeMs += hoodieTimer.endTimer();
      hoodieTimer.startTimer();

      matchingFiles.forEach(partitionFileIdPair -> {
        BloomFilter filter = fileIDToBloomFilter.get(partitionFileIdPair.getRight());
        if (filter.mightContain(record.getRecordKey())) {
          totalMatches++;
          candidates.add(Pair.of(record, partitionFileIdPair.getRight()));
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

      cleanup();
    }

    private void cleanup() {
      if (this.fileIDs != null) {
        this.fileIDs.clear();
      }
      if (this.fileIDToBloomFilter != null) {
        this.fileIDToBloomFilter.clear();
      }
    }

    private void initIfNeeded(String partitionPath) throws IOException {
      if (!Objects.equals(currentPartitionPath, partitionPath)) {
        this.currentPartitionPath = partitionPath;
        cleanup();
        populateFileIDs();
        populateRangeAndBloomFilters();
      }
    }

    private void populateFileIDs() {
      Option<HoodieInstant> latestCommitTime = table.getMetaClient().getCommitsTimeline().filterCompletedInstants().lastInstant();
      this.fileIDs = latestCommitTime.map(commitTime ->
              table.getBaseFileOnlyView()
                      .getLatestBaseFilesBeforeOrOn(currentPartitionPath, commitTime.getTimestamp())
                      .map(HoodieBaseFile::getFileId)
                      .collect(Collectors.toSet())
      ).orElse(Collections.emptySet());
    }

    private void populateRangeAndBloomFilters() throws IOException {
      this.fileIDToBloomFilter = new ExternalSpillableMap<>(config.getBloomIndexV2BufferMaxSize(),
              config.getSpillableMapBasePath(), new DefaultSizeEstimator<>(), new DefaultSizeEstimator<>());
      List<BloomIndexFileInfo> fileInfos = fileIDs.stream().map(fileID -> {
        HoodieBloomRangeInfoHandle<T> indexMetadataHandle = new HoodieBloomRangeInfoHandle<T>(
                config, table, Pair.of(currentPartitionPath, fileID));
        this.fileIDToBloomFilter.put(fileID, indexMetadataHandle.getBloomFilter());
        return indexMetadataHandle.getRangeInfo();
      }).collect(Collectors.toList());
      this.indexFileFilter = new IntervalTreeBasedIndexFileFilter(Collections.singletonMap(currentPartitionPath, fileInfos));
    }

  }

  /**
   * Double check each HoodieRecord by key.
   * 1. return empty if the record doesn't exist in target file slice.
   * 2. tag the matched record with location.
   *
   */
  class LazyKeyChecker extends LazyIterableIterator<Pair<HoodieRecord<T>, String>, Option<HoodieRecord<T>>> {

    private HoodieKeyLookupHandle<T> currHandle = null;
    private HoodieTable<T> table;
    private HoodieTimer hoodieTimer;
    private long totalTimeMs;
    private long totalCount;
    private long totalReadTimeMs;

    public LazyKeyChecker(Iterator<Pair<HoodieRecord<T>, String>> in, HoodieTable<T> table) {
      super(in);
      this.table = table;
    }

    @Override
    protected Option<HoodieRecord<T>> computeNext() {
      if (!inputItr.hasNext()) {
        return Option.empty();
      }

      final Pair<HoodieRecord<T>, String> recordAndFileId = inputItr.next();
      final String recordFileId = recordAndFileId.getRight();
      final Option<String> fileIdOpt = recordFileId.length() > 0 ? Option.of(recordFileId) : Option.empty();
      final HoodieRecord<T> record = recordAndFileId.getLeft();

      Option<HoodieRecord<T>> recordOpt = fileIdOpt.map((Function<String, Option<HoodieRecord<T>>>) fileId -> {
        hoodieTimer.startTimer();
        if (currHandle == null || !currHandle.getFileId().equals(fileId)) {
          currHandle = new HoodieKeyLookupHandle<>(config, table, Pair.of(record.getPartitionPath(), fileId));
        }
        totalReadTimeMs += hoodieTimer.endTimer();
        hoodieTimer.startTimer();

        if (currHandle.containsKey(record.getRecordKey())) {
          HoodieRecordLocation recordLocation = new HoodieRecordLocation(currHandle.getBaseInstantTime(), currHandle.getFileId());
          return Option.of(getTaggedRecord(record, recordLocation));
        } else {
          return Option.empty();
        }
      }).orElse(Option.of(record));

      totalCount++;
      return recordOpt;
    }

    @Override
    protected void start() {
      totalCount = 0;
      totalTimeMs = 0;
      hoodieTimer = new HoodieTimer().startTimer();
    }

    @Override
    protected void end() {
      this.totalTimeMs = hoodieTimer.endTimer();
      LOG.info("LazyKeyChecker: totalCount: " + totalCount + ", totalTimeMs: " + totalTimeMs + "ms, totalReadTimeMs:" + totalReadTimeMs + "ms");
    }

    private HoodieRecord<T> getTaggedRecord(HoodieRecord<T> inputRecord, HoodieRecordLocation location) {
      HoodieRecord<T> record = new HoodieRecord<>(inputRecord);
      record.unseal();
      record.setCurrentLocation(location);
      record.seal();
      return record;
    }
  }
}
