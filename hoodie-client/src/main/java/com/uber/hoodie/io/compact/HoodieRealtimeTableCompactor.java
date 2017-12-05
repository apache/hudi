/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.io.compact;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.HoodieCompactedLogRecordScanner;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieCompactionException;
import com.uber.hoodie.table.HoodieCopyOnWriteTable;
import com.uber.hoodie.table.HoodieTable;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

/**
 * HoodieRealtimeTableCompactor compacts a hoodie table with merge on read storage. Computes all
 * possible compactions, passes it through a CompactionFilter and executes all the compactions and
 * writes a new version of base files and make a normal commit
 *
 * @see HoodieCompactor
 */
public class HoodieRealtimeTableCompactor implements HoodieCompactor {

  private static Logger log = LogManager.getLogger(HoodieRealtimeTableCompactor.class);

  @Override
  public HoodieCommitMetadata compact(JavaSparkContext jsc, HoodieWriteConfig config,
                                      HoodieTable hoodieTable, String compactionCommitTime) throws IOException {
    Preconditions.checkArgument(
        hoodieTable.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ,
        "HoodieRealtimeTableCompactor can only compact table of type "
            + HoodieTableType.MERGE_ON_READ + " and not " + hoodieTable.getMetaClient()
            .getTableType().name());

    // TODO - rollback any compactions in flight

    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    log.info("Compacting " + metaClient.getBasePath() + " with commit " + compactionCommitTime);
    List<String> partitionPaths =
        FSUtils.getAllPartitionPaths(metaClient.getFs(), metaClient.getBasePath(),
            config.shouldAssumeDatePartitioning());

    log.info("Compaction looking for files to compact in " + partitionPaths + " partitions");
    List<CompactionOperation> operations =
        jsc.parallelize(partitionPaths, partitionPaths.size())
            .flatMap((FlatMapFunction<String, CompactionOperation>) partitionPath -> hoodieTable
                .getRTFileSystemView()
                .getLatestFileSlices(partitionPath)
                .map(s -> new CompactionOperation(s.getDataFile().get(),
                    partitionPath, s.getLogFiles().collect(Collectors.toList()), config))
                .filter(c -> !c.getDeltaFilePaths().isEmpty())
                .collect(toList()).iterator()).collect();
    log.info("Total of " + operations.size() + " compactions are retrieved");

    // Filter the compactions with the passed in filter. This lets us choose most effective compactions only
    operations = config.getCompactionStrategy().orderAndFilter(config, operations);
    if (operations.isEmpty()) {
      log.warn("After filtering, Nothing to compact for " + metaClient.getBasePath());
      return null;
    }

    log.info("After filtering, Compacting " + operations + " files");
    List<HoodieWriteStat> updateStatusMap =
        jsc.parallelize(operations, operations.size())
            .map(s -> executeCompaction(metaClient, config, s, compactionCommitTime))
            .flatMap(new FlatMapFunction<List<HoodieWriteStat>, HoodieWriteStat>() {
              @Override
              public Iterator<HoodieWriteStat> call(
                  List<HoodieWriteStat> hoodieWriteStats)
                  throws Exception {
                return hoodieWriteStats.iterator();
              }
            }).collect();

    HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
    for (HoodieWriteStat stat : updateStatusMap) {
      metadata.addWriteStat(stat.getPartitionPath(), stat);
    }

    log.info("Compaction finished with result " + metadata);

    //noinspection ConstantConditions
    if (isCompactionSucceeded(metadata)) {
      log.info("Compaction succeeded " + compactionCommitTime);
      commitCompaction(compactionCommitTime, metaClient, metadata);
    } else {
      log.info("Compaction failed " + compactionCommitTime);
    }
    return metadata;
  }

  private boolean isCompactionSucceeded(HoodieCommitMetadata result) {
    //TODO figure out a success factor for a compaction
    return true;
  }

  private List<HoodieWriteStat> executeCompaction(HoodieTableMetaClient metaClient,
                                                  HoodieWriteConfig config, CompactionOperation operation, String commitTime)
      throws IOException {
    FileSystem fs = FSUtils.getFs();
    Schema readerSchema =
        HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));

    log.info("Compacting base " + operation.getDataFilePath() + " with delta files " + operation
        .getDeltaFilePaths() + " for commit " + commitTime);
    // TODO - FIX THIS
    // Reads the entire avro file. Always only specific blocks should be read from the avro file (failure recover).
    // Load all the delta commits since the last compaction commit and get all the blocks to be loaded and load it using CompositeAvroLogReader
    // Since a DeltaCommit is not defined yet, reading all the records. revisit this soon.

    String maxInstantTime = metaClient.getActiveTimeline()
        .getTimelineOfActions(
            Sets.newHashSet(HoodieTimeline.COMMIT_ACTION,
                HoodieTimeline.DELTA_COMMIT_ACTION))
        .filterCompletedInstants().lastInstant().get().getTimestamp();

    HoodieCompactedLogRecordScanner scanner = new HoodieCompactedLogRecordScanner(fs,
        metaClient.getBasePath(),
        operation.getDeltaFilePaths(), readerSchema, maxInstantTime);
    if (!scanner.iterator().hasNext()) {
      return Lists.newArrayList();
    }

    // Compacting is very similar to applying updates to existing file
    HoodieCopyOnWriteTable table =
        new HoodieCopyOnWriteTable(config, metaClient);
    Iterator<List<WriteStatus>> result = table
        .handleUpdate(commitTime, operation.getFileId(), scanner.iterator());
    Iterable<List<WriteStatus>> resultIterable = () -> result;
    return StreamSupport.stream(resultIterable.spliterator(), false)
        .flatMap(Collection::stream)
        .map(WriteStatus::getStat)
        .map(s -> {
          s.setTotalRecordsToBeUpdate(scanner.getTotalRecordsToUpdate());
          s.setTotalLogFiles(scanner.getTotalLogFiles());
          s.setTotalLogRecords(scanner.getTotalLogRecords());
          s.setPartitionPath(operation.getPartitionPath());
        return s;})
        .collect(toList());
  }

  public boolean commitCompaction(String commitTime, HoodieTableMetaClient metaClient,
      HoodieCommitMetadata metadata) {
    log.info("Committing Compaction " + commitTime);
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    try {
      activeTimeline.saveAsComplete(
          new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime),
          Optional.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + metaClient.getBasePath() + " at time " + commitTime, e);
    }
    return true;
  }

}
