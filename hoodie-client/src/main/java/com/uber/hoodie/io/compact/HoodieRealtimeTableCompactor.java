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

import static java.util.stream.Collectors.toList;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieWriteStat.RuntimeStats;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.log.HoodieMergedLogRecordScanner;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.io.compact.strategy.CompactionStrategy;
import com.uber.hoodie.table.HoodieCopyOnWriteTable;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;

/**
 * HoodieRealtimeTableCompactor compacts a hoodie table with merge on read storage. Computes all
 * possible compactions, passes it through a CompactionFilter and executes all the compactions and
 * writes a new version of base files and make a normal commit
 *
 * @see HoodieCompactor
 */
public class HoodieRealtimeTableCompactor implements HoodieCompactor {

  private static Logger log = LogManager.getLogger(HoodieRealtimeTableCompactor.class);
  // Accumulator to keep track of total log files for a dataset
  private AccumulatorV2<Long, Long> totalLogFiles;
  // Accumulator to keep track of total log file slices for a dataset
  private AccumulatorV2<Long, Long> totalFileSlices;

  @Override
  public JavaRDD<WriteStatus> compact(JavaSparkContext jsc, HoodieWriteConfig config,
      HoodieTable hoodieTable, String compactionCommitTime) throws IOException {

    totalLogFiles = new LongAccumulator();
    totalFileSlices = new LongAccumulator();
    jsc.sc().register(totalLogFiles);
    jsc.sc().register(totalFileSlices);

    List<CompactionOperation> operations = getCompactionWorkload(jsc, hoodieTable, config,
        compactionCommitTime);
    if (operations == null) {
      return jsc.emptyRDD();
    }
    return executeCompaction(jsc, operations, hoodieTable, config, compactionCommitTime);
  }

  private JavaRDD<WriteStatus> executeCompaction(JavaSparkContext jsc,
      List<CompactionOperation> operations, HoodieTable hoodieTable, HoodieWriteConfig config,
      String compactionCommitTime) throws IOException {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    // Compacting is very similar to applying updates to existing file
    HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, jsc);
    log.info("After filtering, Compacting " + operations + " files");
    return jsc.parallelize(operations, operations.size())
        .map(s -> compact(table, metaClient, config, s, compactionCommitTime))
        .flatMap(writeStatusesItr -> writeStatusesItr.iterator());
  }

  private List<WriteStatus> compact(HoodieCopyOnWriteTable hoodieCopyOnWriteTable, HoodieTableMetaClient metaClient,
      HoodieWriteConfig config,
      CompactionOperation operation, String commitTime) throws IOException {
    FileSystem fs = metaClient.getFs();
    Schema readerSchema = HoodieAvroUtils
        .addMetadataFields(new Schema.Parser().parse(config.getSchema()));

    log.info("Compacting base " + operation.getDataFilePath() + " with delta files " + operation
        .getDeltaFilePaths() + " for commit " + commitTime);
    // TODO - FIX THIS
    // Reads the entire avro file. Always only specific blocks should be read from the avro file
    // (failure recover).
    // Load all the delta commits since the last compaction commit and get all the blocks to be
    // loaded and load it using CompositeAvroLogReader
    // Since a DeltaCommit is not defined yet, reading all the records. revisit this soon.
    String maxInstantTime = metaClient.getActiveTimeline()
        .getTimelineOfActions(
            Sets.newHashSet(HoodieTimeline.COMMIT_ACTION, HoodieTimeline.ROLLBACK_ACTION,
                HoodieTimeline.DELTA_COMMIT_ACTION))

        .filterCompletedInstants().lastInstant().get().getTimestamp();
    log.info("MaxMemoryPerCompaction => " + config.getMaxMemoryPerCompaction());
    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs,
        metaClient.getBasePath(), operation.getDeltaFilePaths(), readerSchema, maxInstantTime,
        config.getMaxMemoryPerCompaction(), config.getCompactionLazyBlockReadEnabled(),
        config.getCompactionReverseLogReadEnabled(), config.getMaxDFSStreamBufferSize(),
        config.getSpillableMapBasePath());
    if (!scanner.iterator().hasNext()) {
      return Lists.<WriteStatus>newArrayList();
    }

    // Compacting is very similar to applying updates to existing file
    Iterator<List<WriteStatus>> result;
    // If the dataFile is present, there is a base parquet file present, perform updates else perform inserts into a
    // new base parquet file.
    if (operation.getDataFilePath().isPresent()) {
      result = hoodieCopyOnWriteTable
          .handleUpdate(commitTime, operation.getFileId(), scanner.getRecords());
    } else {
      result = hoodieCopyOnWriteTable
          .handleInsert(commitTime, operation.getPartitionPath(), operation.getFileId(), scanner.iterator());
    }
    Iterable<List<WriteStatus>> resultIterable = () -> result;
    return StreamSupport.stream(resultIterable.spliterator(), false).flatMap(Collection::stream)
        .map(s -> {
          s.getStat().setTotalUpdatedRecordsCompacted(scanner.getNumMergedRecordsInLog());
          s.getStat().setTotalLogFilesCompacted(scanner.getTotalLogFiles());
          s.getStat().setTotalLogRecords(scanner.getTotalLogRecords());
          s.getStat().setPartitionPath(operation.getPartitionPath());
          s.getStat().setTotalLogSizeCompacted((long) operation.getMetrics().get(
              CompactionStrategy.TOTAL_LOG_FILE_SIZE));
          s.getStat().setTotalLogBlocks(scanner.getTotalLogBlocks());
          s.getStat().setTotalCorruptLogBlock(scanner.getTotalCorruptBlocks());
          s.getStat().setTotalRollbackBlocks(scanner.getTotalRollbacks());
          RuntimeStats runtimeStats = new RuntimeStats();
          runtimeStats.setTotalScanTime(scanner.getTotalTimeTakenToReadAndMergeBlocks());
          s.getStat().setRuntimeStats(runtimeStats);
          return s;
        }).collect(toList());
  }

  private List<CompactionOperation> getCompactionWorkload(JavaSparkContext jsc,
      HoodieTable hoodieTable, HoodieWriteConfig config, String compactionCommitTime)
      throws IOException {

    Preconditions
        .checkArgument(hoodieTable.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ,
            "HoodieRealtimeTableCompactor can only compact table of type "
                + HoodieTableType.MERGE_ON_READ + " and not " + hoodieTable.getMetaClient()
                .getTableType().name());

    //TODO : check if maxMemory is not greater than JVM or spark.executor memory
    // TODO - rollback any compactions in flight
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    log.info("Compacting " + metaClient.getBasePath() + " with commit " + compactionCommitTime);
    List<String> partitionPaths = FSUtils
        .getAllPartitionPaths(metaClient.getFs(), metaClient.getBasePath(),
            config.shouldAssumeDatePartitioning());

    TableFileSystemView.RealtimeView fileSystemView = hoodieTable.getRTFileSystemView();
    log.info("Compaction looking for files to compact in " + partitionPaths + " partitions");
    List<CompactionOperation> operations =
        jsc.parallelize(partitionPaths, partitionPaths.size())
            .flatMap((FlatMapFunction<String, CompactionOperation>) partitionPath -> fileSystemView
                .getLatestFileSlices(partitionPath).map(
                    s -> {
                      List<HoodieLogFile> logFiles = s.getLogFiles().sorted(HoodieLogFile
                          .getBaseInstantAndLogVersionComparator().reversed()).collect(Collectors.toList());
                      totalLogFiles.add((long) logFiles.size());
                      totalFileSlices.add(1L);
                      return new CompactionOperation(s.getDataFile(), partitionPath, logFiles, config);
                    })
                .filter(c -> !c.getDeltaFilePaths().isEmpty())
                .collect(toList()).iterator()).collect();
    log.info("Total of " + operations.size() + " compactions are retrieved");
    log.info("Total number of latest files slices " + totalFileSlices.value());
    log.info("Total number of log files " + totalLogFiles.value());
    log.info("Total number of file slices " + totalFileSlices.value());

    // Filter the compactions with the passed in filter. This lets us choose most effective
    // compactions only
    operations = config.getCompactionStrategy().orderAndFilter(config, operations);
    if (operations.isEmpty()) {
      log.warn("After filtering, Nothing to compact for " + metaClient.getBasePath());
      return null;
    }
    return operations;
  }

}