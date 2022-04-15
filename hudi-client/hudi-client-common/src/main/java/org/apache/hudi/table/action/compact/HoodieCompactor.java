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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.table.HoodieCompactionHandler;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

/**
 * A HoodieCompactor runs compaction on a hoodie table.
 */
public abstract class HoodieCompactor<T extends HoodieRecordPayload, I, K, O> implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieCompactor.class);

  /**
   * Handles the compaction timeline based on the compaction instant before actual compaction.
   *
   * @param table                     {@link HoodieTable} instance to use.
   * @param pendingCompactionTimeline pending compaction timeline.
   * @param compactionInstantTime     compaction instant
   */
  public abstract void preCompact(
      HoodieTable table, HoodieTimeline pendingCompactionTimeline, String compactionInstantTime);

  /**
   * Maybe persist write status.
   *
   * @param writeStatus {@link HoodieData} of {@link WriteStatus}.
   */
  public abstract void maybePersist(HoodieData<WriteStatus> writeStatus, HoodieWriteConfig config);

  /**
   * Execute compaction operations and report back status.
   */
  public HoodieData<WriteStatus> compact(
      HoodieEngineContext context, HoodieCompactionPlan compactionPlan,
      HoodieTable table, HoodieWriteConfig config, String compactionInstantTime,
      HoodieCompactionHandler compactionHandler) {
    if (compactionPlan == null || (compactionPlan.getOperations() == null)
        || (compactionPlan.getOperations().isEmpty())) {
      return context.emptyHoodieData();
    }
    HoodieActiveTimeline timeline = table.getActiveTimeline();
    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    // Mark instant as compaction inflight
    timeline.transitionCompactionRequestedToInflight(instant);
    table.getMetaClient().reloadActiveTimeline();

    HoodieTableMetaClient metaClient = table.getMetaClient();
    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);

    // Here we firstly use the table schema as the reader schema to read
    // log file.That is because in the case of MergeInto, the config.getSchema may not
    // the same with the table schema.
    try {
      if (StringUtils.isNullOrEmpty(config.getInternalSchema())) {
        Schema readerSchema = schemaResolver.getTableAvroSchema(false);
        config.setSchema(readerSchema.toString());
      }
    } catch (Exception e) {
      // If there is no commit in the table, just ignore the exception.
    }

    // Compacting is very similar to applying updates to existing file
    List<CompactionOperation> operations = compactionPlan.getOperations().stream()
        .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
    LOG.info("Compactor compacting " + operations + " files");

    context.setJobStatus(this.getClass().getSimpleName(), "Compacting file slices: " + config.getTableName());
    TaskContextSupplier taskContextSupplier = table.getTaskContextSupplier();
    return context.parallelize(operations).map(operation -> compact(
        compactionHandler, metaClient, config, operation, compactionInstantTime, taskContextSupplier))
        .flatMap(List::iterator);
  }

  /**
   * Execute a single compaction operation and report back status.
   */
  public List<WriteStatus> compact(HoodieCompactionHandler compactionHandler,
                                   HoodieTableMetaClient metaClient,
                                   HoodieWriteConfig config,
                                   CompactionOperation operation,
                                   String instantTime,
                                   TaskContextSupplier taskContextSupplier) throws IOException {
    FileSystem fs = metaClient.getFs();
    Schema readerSchema;
    Option<InternalSchema> internalSchemaOption = Option.empty();
    if (!StringUtils.isNullOrEmpty(config.getInternalSchema())) {
      readerSchema = new Schema.Parser().parse(config.getSchema());
      internalSchemaOption = SerDeHelper.fromJson(config.getInternalSchema());
      // its safe to modify config here, since we running in task side.
      ((HoodieTable) compactionHandler).getConfig().setDefault(config);
    } else {
      readerSchema = HoodieAvroUtils.addMetadataFields(
          new Schema.Parser().parse(config.getSchema()), config.allowOperationMetadataField());
    }
    LOG.info("Compacting base " + operation.getDataFileName() + " with delta files " + operation.getDeltaFileNames()
        + " for commit " + instantTime);
    // TODO - FIX THIS
    // Reads the entire avro file. Always only specific blocks should be read from the avro file
    // (failure recover).
    // Load all the delta commits since the last compaction commit and get all the blocks to be
    // loaded and load it using CompositeAvroLogReader
    // Since a DeltaCommit is not defined yet, reading all the records. revisit this soon.
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
        .withInternalSchema(internalSchemaOption.orElse(InternalSchema.getEmptyInternalSchema()))
        .withMaxMemorySizeInBytes(maxMemoryPerCompaction)
        .withReadBlocksLazily(config.getCompactionLazyBlockReadEnabled())
        .withReverseReader(config.getCompactionReverseLogReadEnabled())
        .withBufferSize(config.getMaxDFSStreamBufferSize())
        .withSpillableMapBasePath(config.getSpillableMapBasePath())
        .withDiskMapType(config.getCommonConfig().getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(config.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
        .withOperationField(config.allowOperationMetadataField())
        .withPartition(operation.getPartitionPath())
        .build();

    Option<HoodieBaseFile> oldDataFileOpt =
        operation.getBaseFile(metaClient.getBasePath(), operation.getPartitionPath());

    // Considering following scenario: if all log blocks in this fileSlice is rollback, it returns an empty scanner.
    // But in this case, we need to give it a base file. Otherwise, it will lose base file in following fileSlice.
    if (!scanner.iterator().hasNext()) {
      if (!oldDataFileOpt.isPresent()) {
        scanner.close();
        return new ArrayList<>();
      } else {
        // TODO: we may directly rename original parquet file if there is not evolution/devolution of schema
        /*
        TaskContextSupplier taskContextSupplier = hoodieCopyOnWriteTable.getTaskContextSupplier();
        String newFileName = FSUtils.makeDataFileName(instantTime,
            FSUtils.makeWriteToken(taskContextSupplier.getPartitionIdSupplier().get(), taskContextSupplier.getStageIdSupplier().get(), taskContextSupplier.getAttemptIdSupplier().get()),
            operation.getFileId(), hoodieCopyOnWriteTable.getBaseFileExtension());
        Path oldFilePath = new Path(oldDataFileOpt.get().getPath());
        Path newFilePath = new Path(oldFilePath.getParent(), newFileName);
        FileUtil.copy(fs,oldFilePath, fs, newFilePath, false, fs.getConf());
        */
      }
    }

    // Compacting is very similar to applying updates to existing file
    Iterator<List<WriteStatus>> result;
    // If the dataFile is present, perform updates else perform inserts into a new base file.
    if (oldDataFileOpt.isPresent()) {
      result = compactionHandler.handleUpdate(instantTime, operation.getPartitionPath(),
          operation.getFileId(), scanner.getRecords(),
          oldDataFileOpt.get());
    } else {
      result = compactionHandler.handleInsert(instantTime, operation.getPartitionPath(), operation.getFileId(),
          scanner.getRecords());
    }
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
      RuntimeStats runtimeStats = new RuntimeStats();
      runtimeStats.setTotalScanTime(scanner.getTotalTimeTakenToReadAndMergeBlocks());
      s.getStat().setRuntimeStats(runtimeStats);
    }).collect(toList());
  }
}
