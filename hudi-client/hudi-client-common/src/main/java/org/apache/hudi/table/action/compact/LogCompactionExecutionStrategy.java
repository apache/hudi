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

package org.apache.hudi.table.action.compact;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.table.HoodieCompactionHandler;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

public class LogCompactionExecutionStrategy<T extends HoodieRecordPayload, I, K, O> implements Serializable {

  private static final Logger LOG = LogManager.getLogger(LogCompactionExecutionStrategy.class);

  private final HoodieTable<T, I, K, O> hoodieTable;
  private final transient HoodieEngineContext engineContext;
  private final HoodieWriteConfig writeConfig;
  private final HoodieCompactionHandler compactionHandler;

  public LogCompactionExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig,
                                     HoodieCompactionHandler compactionHandler) {
    this.writeConfig = writeConfig;
    this.hoodieTable = table;
    this.engineContext = engineContext;
    this.compactionHandler = compactionHandler;
  }

  /**
   * Executes compaction based on the stategy class provided within the compaction Plan.
   * Note that commit is not done as part of strategy. commit is callers responsibility.
   */
  public HoodieData<WriteStatus> performCompaction(final HoodieCompactionPlan compactionPlan, final Schema schema,
                                                   final String instantTime) {

    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);

    // Here we use the table schema as the reader schema to read
    // log file.That is because in the case of MergeInto, the config.getSchema may not
    // the same with the table schema.
    HoodieWriteConfig writeConfigCopy = HoodieWriteConfig.newBuilder().withProperties(writeConfig.getProps()).build();
    try {
      Schema readerSchema = schemaResolver.getTableAvroSchema(false);
      writeConfigCopy.setSchema(readerSchema.toString());
    } catch (Exception e) {
      // If there is no commit in the table, just ignore the exception.
    }

    // Compacting is very similar to applying updates to existing file
    List<CompactionOperation> compactionOps = compactionPlan.getOperations().stream()
        .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
    LOG.info("Compactions operations " + compactionOps);

    engineContext.setJobStatus(this.getClass().getSimpleName(), "Log Compacting log files");
    TaskContextSupplier taskContextSupplier = hoodieTable.getTaskContextSupplier();
    return engineContext.parallelize(compactionOps).map(compactionOperation ->
        logCompact(compactionHandler, metaClient, writeConfigCopy, compactionOperation, instantTime, taskContextSupplier))
        .flatMap(List::iterator);
  }

  public List<WriteStatus> logCompact(HoodieCompactionHandler compactionHandler,
                                      HoodieTableMetaClient metaClient,
                                      HoodieWriteConfig config,
                                      CompactionOperation operation,
                                      String instantTime,
                                      TaskContextSupplier taskContextSupplier) {
    FileSystem fs = metaClient.getFs();

    Schema readerSchema = HoodieAvroUtils.addMetadataFields(
        new Schema.Parser().parse(config.getSchema()), config.allowOperationMetadataField());
    LOG.info("Log Compacting delta files " + operation.getDeltaFileNames() + " for commit " + instantTime);

    String maxInstantTime = metaClient
        .getActiveTimeline().getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION,
            HoodieTimeline.ROLLBACK_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION))
        .filterCompletedInstants().lastInstant().get().getTimestamp();
    long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(taskContextSupplier, config);
    LOG.info("MaxMemoryPerCompaction => " + maxMemoryPerCompaction);

    List<String> logFiles = operation.getDeltaFileNames().stream().map(
        p -> new Path(FSUtils.getPartitionPath(metaClient.getBasePath(), operation.getPartitionPath()), p).toString())
        .collect(toList());
    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(fs)
        .withBasePath(metaClient.getBasePath())
        .withLogFilePaths(logFiles)
        .withReaderSchema(readerSchema)
        .withLatestInstantTime(maxInstantTime)
        .withMaxMemorySizeInBytes(maxMemoryPerCompaction)
        .withBufferSize(config.getMaxDFSStreamBufferSize())
        .withSpillableMapBasePath(config.getSpillableMapBasePath())
        .withDiskMapType(config.getCommonConfig().getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(config.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
        .withOperationField(config.allowOperationMetadataField())
        .withPartition(operation.getPartitionPath())
        .withPreserveCommitMetadata(true)
        .withUseScanV2(true)
        .build();
    if (!scanner.iterator().hasNext()) {
      scanner.close();
      return new ArrayList<>();
    }
    LOG.info(String.format("Count of valid block instants found for fileid %s are %s", operation.getFileId(),
        scanner.getValidBlockInstants()));
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.COMPACTED_BLOCK_TIMES,
        StringUtils.join(scanner.getValidBlockInstants(), ","));
    // Compacting is very similar to applying updates to existing file
    Iterator<List<WriteStatus>> result = compactionHandler.handlePreppedInserts(instantTime, operation.getPartitionPath(),
        operation.getFileId(), scanner.getRecords(), header);
    scanner.close();
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
      HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
      runtimeStats.setTotalScanTime(scanner.getTotalTimeTakenToReadAndMergeBlocks());
      s.getStat().setRuntimeStats(runtimeStats);
    }).collect(toList());
  }

}
