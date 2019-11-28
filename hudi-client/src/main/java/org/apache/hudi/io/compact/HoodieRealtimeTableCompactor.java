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

package org.apache.hudi.io.compact;

import org.apache.hudi.WriteStatus;
import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.TableFileSystemView.RealtimeView;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.compact.strategy.CompactionStrategy;
import org.apache.hudi.table.HoodieCopyOnWriteTable;
import org.apache.hudi.table.HoodieTable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

/**
 * HoodieRealtimeTableCompactor compacts a hoodie table with merge on read storage. Computes all possible compactions,
 * passes it through a CompactionFilter and executes all the compactions and writes a new version of base files and make
 * a normal commit
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
  public JavaRDD<WriteStatus> compact(JavaSparkContext jsc, HoodieCompactionPlan compactionPlan,
      HoodieTable hoodieTable, HoodieWriteConfig config, String compactionInstantTime) throws IOException {
    if (compactionPlan == null || (compactionPlan.getOperations() == null)
        || (compactionPlan.getOperations().isEmpty())) {
      return jsc.emptyRDD();
    }
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    // Compacting is very similar to applying updates to existing file
    HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, jsc);
    List<CompactionOperation> operations = compactionPlan.getOperations().stream()
        .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
    log.info("Compactor compacting " + operations + " files");

    return jsc.parallelize(operations, operations.size())
        .map(s -> compact(table, metaClient, config, s, compactionInstantTime)).flatMap(List::iterator);
  }

  private List<WriteStatus> compact(HoodieCopyOnWriteTable hoodieCopyOnWriteTable, HoodieTableMetaClient metaClient,
      HoodieWriteConfig config, CompactionOperation operation, String commitTime) throws IOException {
    FileSystem fs = metaClient.getFs();

    Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
    log.info("Compacting base " + operation.getDataFileName() + " with delta files " + operation.getDeltaFileNames()
        + " for commit " + commitTime);
    // TODO - FIX THIS
    // Reads the entire avro file. Always only specific blocks should be read from the avro file
    // (failure recover).
    // Load all the delta commits since the last compaction commit and get all the blocks to be
    // loaded and load it using CompositeAvroLogReader
    // Since a DeltaCommit is not defined yet, reading all the records. revisit this soon.
    String maxInstantTime = metaClient
        .getActiveTimeline().getTimelineOfActions(Sets.newHashSet(HoodieTimeline.COMMIT_ACTION,
            HoodieTimeline.ROLLBACK_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION))
        .filterCompletedInstants().lastInstant().get().getTimestamp();
    log.info("MaxMemoryPerCompaction => " + config.getMaxMemoryPerCompaction());

    List<String> logFiles = operation.getDeltaFileNames().stream().map(
        p -> new Path(FSUtils.getPartitionPath(metaClient.getBasePath(), operation.getPartitionPath()), p).toString())
        .collect(toList());
    HoodieMergedLogRecordScanner scanner = new HoodieMergedLogRecordScanner(fs, metaClient.getBasePath(), logFiles,
        readerSchema, maxInstantTime, config.getMaxMemoryPerCompaction(), config.getCompactionLazyBlockReadEnabled(),
        config.getCompactionReverseLogReadEnabled(), config.getMaxDFSStreamBufferSize(),
        config.getSpillableMapBasePath());
    if (!scanner.iterator().hasNext()) {
      return Lists.<WriteStatus>newArrayList();
    }

    Option<HoodieDataFile> oldDataFileOpt =
        operation.getBaseFile(metaClient.getBasePath(), operation.getPartitionPath());

    // Compacting is very similar to applying updates to existing file
    Iterator<List<WriteStatus>> result;
    // If the dataFile is present, there is a base parquet file present, perform updates else perform inserts into a
    // new base parquet file.
    if (oldDataFileOpt.isPresent()) {
      result = hoodieCopyOnWriteTable.handleUpdate(commitTime, operation.getFileId(), scanner.getRecords(),
          oldDataFileOpt.get());
    } else {
      result = hoodieCopyOnWriteTable.handleInsert(commitTime, operation.getPartitionPath(), operation.getFileId(),
          scanner.iterator());
    }
    Iterable<List<WriteStatus>> resultIterable = () -> result;
    return StreamSupport.stream(resultIterable.spliterator(), false).flatMap(Collection::stream).peek(s -> {
      s.getStat().setTotalUpdatedRecordsCompacted(scanner.getNumMergedRecordsInLog());
      s.getStat().setTotalLogFilesCompacted(scanner.getTotalLogFiles());
      s.getStat().setTotalLogRecords(scanner.getTotalLogRecords());
      s.getStat().setPartitionPath(operation.getPartitionPath());
      s.getStat()
          .setTotalLogSizeCompacted(operation.getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());
      s.getStat().setTotalLogBlocks(scanner.getTotalLogBlocks());
      s.getStat().setTotalCorruptLogBlock(scanner.getTotalCorruptBlocks());
      s.getStat().setTotalRollbackBlocks(scanner.getTotalRollbacks());
      RuntimeStats runtimeStats = new RuntimeStats();
      runtimeStats.setTotalScanTime(scanner.getTotalTimeTakenToReadAndMergeBlocks());
      s.getStat().setRuntimeStats(runtimeStats);
    }).collect(toList());
  }

  @Override
  public HoodieCompactionPlan generateCompactionPlan(JavaSparkContext jsc, HoodieTable hoodieTable,
      HoodieWriteConfig config, String compactionCommitTime, Set<HoodieFileGroupId> fgIdsInPendingCompactions)
      throws IOException {

    totalLogFiles = new LongAccumulator();
    totalFileSlices = new LongAccumulator();
    jsc.sc().register(totalLogFiles);
    jsc.sc().register(totalFileSlices);

    Preconditions.checkArgument(hoodieTable.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ,
        "HoodieRealtimeTableCompactor can only compact table of type " + HoodieTableType.MERGE_ON_READ + " and not "
            + hoodieTable.getMetaClient().getTableType().name());

    // TODO : check if maxMemory is not greater than JVM or spark.executor memory
    // TODO - rollback any compactions in flight
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    log.info("Compacting " + metaClient.getBasePath() + " with commit " + compactionCommitTime);
    List<String> partitionPaths = FSUtils.getAllPartitionPaths(metaClient.getFs(), metaClient.getBasePath(),
        config.shouldAssumeDatePartitioning());

    // filter the partition paths if needed to reduce list status
    partitionPaths = config.getCompactionStrategy().filterPartitionPaths(config, partitionPaths);

    if (partitionPaths.isEmpty()) {
      // In case no partitions could be picked, return no compaction plan
      return null;
    }

    RealtimeView fileSystemView = hoodieTable.getRTFileSystemView();
    log.info("Compaction looking for files to compact in " + partitionPaths + " partitions");
    List<HoodieCompactionOperation> operations = jsc.parallelize(partitionPaths, partitionPaths.size())
        .flatMap((FlatMapFunction<String, CompactionOperation>) partitionPath -> fileSystemView
            .getLatestFileSlices(partitionPath)
            .filter(slice -> !fgIdsInPendingCompactions.contains(slice.getFileGroupId())).map(s -> {
              List<HoodieLogFile> logFiles =
                  s.getLogFiles().sorted(HoodieLogFile.getLogFileComparator()).collect(Collectors.toList());
              totalLogFiles.add((long) logFiles.size());
              totalFileSlices.add(1L);
              // Avro generated classes are not inheriting Serializable. Using CompactionOperation POJO
              // for spark Map operations and collecting them finally in Avro generated classes for storing
              // into meta files.
              Option<HoodieDataFile> dataFile = s.getDataFile();
              return new CompactionOperation(dataFile, partitionPath, logFiles,
                  config.getCompactionStrategy().captureMetrics(config, dataFile, partitionPath, logFiles));
            }).filter(c -> !c.getDeltaFileNames().isEmpty()).collect(toList()).iterator())
        .collect().stream().map(CompactionUtils::buildHoodieCompactionOperation).collect(toList());
    log.info("Total of " + operations.size() + " compactions are retrieved");
    log.info("Total number of latest files slices " + totalFileSlices.value());
    log.info("Total number of log files " + totalLogFiles.value());
    log.info("Total number of file slices " + totalFileSlices.value());
    // Filter the compactions with the passed in filter. This lets us choose most effective
    // compactions only
    HoodieCompactionPlan compactionPlan = config.getCompactionStrategy().generateCompactionPlan(config, operations,
        CompactionUtils.getAllPendingCompactionPlans(metaClient).stream().map(Pair::getValue).collect(toList()));
    Preconditions.checkArgument(
        compactionPlan.getOperations().stream().noneMatch(
            op -> fgIdsInPendingCompactions.contains(new HoodieFileGroupId(op.getPartitionPath(), op.getFileId()))),
        "Bad Compaction Plan. FileId MUST NOT have multiple pending compactions. "
            + "Please fix your strategy implementation." + "FileIdsWithPendingCompactions :" + fgIdsInPendingCompactions
            + ", Selected workload :" + compactionPlan);
    if (compactionPlan.getOperations().isEmpty()) {
      log.warn("After filtering, Nothing to compact for " + metaClient.getBasePath());
    }
    return compactionPlan;
  }
}
