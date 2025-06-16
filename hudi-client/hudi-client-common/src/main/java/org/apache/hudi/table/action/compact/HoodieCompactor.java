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
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieCompactionHandler;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class HoodieCompactor<T, I, K, O> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieCompactor.class);

  /**
   * Handles the compaction timeline based on the compaction instant before actual compaction.
   *
   * @param table                     {@link HoodieTable} instance to use.
   * @param pendingCompactionTimeline pending compaction timeline.
   * @param instantTime     compaction instant
   */
  public abstract void preCompact(
      HoodieTable table, HoodieTimeline pendingCompactionTimeline, WriteOperationType operationType, String instantTime);

  /**
   * Maybe persist write status.
   *
   * @param writeStatus {@link HoodieData} of {@link WriteStatus}.
   */
  public abstract void maybePersist(HoodieData<WriteStatus> writeStatus, HoodieEngineContext context, HoodieWriteConfig config, String instantTime);

  /**
   * Execute compaction operations and report back status.
   */
  public HoodieData<WriteStatus> compact(
      HoodieEngineContext context, WriteOperationType operationType,
      HoodieCompactionPlan compactionPlan,
      HoodieTable table, HoodieWriteConfig config, String compactionInstantTime,
      HoodieCompactionHandler compactionHandler) {
    if (compactionPlan == null || (compactionPlan.getOperations() == null)
        || (compactionPlan.getOperations().isEmpty())) {
      return context.emptyHoodieData();
    }
    CompactionExecutionHelper executionHelper = getCompactionExecutionStrategy(compactionPlan);

    // Transition requested to inflight file.
    executionHelper.transitionRequestedToInflight(table, compactionInstantTime);
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

    String maxInstantTime = getMaxInstantTime(metaClient);

    context.setJobStatus(this.getClass().getSimpleName(), "Compacting file slices: " + config.getTableName());
    TaskContextSupplier taskContextSupplier = table.getTaskContextSupplier();
    // if this is a MDT, set up the instant range of log reader just like regular MDT snapshot reader.
    Option<InstantRange> instantRange = CompactHelpers.getInstance().getInstantRange(metaClient);

    boolean useFileGroupReaderBasedCompaction = context.supportsFileGroupReader()   // the engine needs to support fg reader first
        && !metaClient.isMetadataTable()
        && config.getBooleanOrDefault(HoodieReaderConfig.FILE_GROUP_READER_ENABLED)
        && operationType == WriteOperationType.COMPACT
        && !hasBootstrapFile(operations)                                            // bootstrap file read for fg reader is not ready
        && config.populateMetaFields();                                             // Virtual key support by fg reader is not ready

    if (useFileGroupReaderBasedCompaction) {
      ReaderContextFactory<T> readerContextFactory = context.getReaderContextFactory(metaClient);
      return context.parallelize(operations).map(
              operation -> compact(compactionHandler, config, operation, compactionInstantTime, readerContextFactory.getContext()))
          .flatMap(List::iterator);
    } else {
      return context.parallelize(operations).map(
              operation -> compact(compactionHandler, metaClient, config, operation, compactionInstantTime, maxInstantTime,
                  instantRange, taskContextSupplier, executionHelper))
          .flatMap(List::iterator);
    }
  }

  /**
   * Execute a single compaction operation using file group reader and report back status.
   */
  public List<WriteStatus> compact(HoodieCompactionHandler compactionHandler,
                                   HoodieTableMetaClient metaClient,
                                   HoodieWriteConfig config,
                                   CompactionOperation operation,
                                   String instantTime,
                                   String maxInstantTime,
                                   TaskContextSupplier taskContextSupplier) throws IOException {
    return compact(compactionHandler, metaClient, config, operation, instantTime, maxInstantTime, Option.empty(),
        taskContextSupplier, new CompactionExecutionHelper());
  }

  /**
   * Execute a single compaction operation and report back status.
   */
  public List<WriteStatus> compact(HoodieCompactionHandler compactionHandler,
                                   HoodieTableMetaClient metaClient,
                                   HoodieWriteConfig config,
                                   CompactionOperation operation,
                                   String instantTime,
                                   String maxInstantTime,
                                   Option<InstantRange> instantRange,
                                   TaskContextSupplier taskContextSupplier,
                                   CompactionExecutionHelper executionHelper) throws IOException {
    HoodieStorage storage = metaClient.getStorage();
    Schema readerSchema;
    Option<InternalSchema> internalSchemaOption = Option.empty();
    if (!StringUtils.isNullOrEmpty(config.getInternalSchema())) {
      readerSchema = new Schema.Parser().parse(config.getSchema());
      internalSchemaOption = SerDeHelper.fromJson(config.getInternalSchema());
      // its safe to modify config here, since we are running in task side.
      ((HoodieTable) compactionHandler).getConfig().setDefault(config);
    } else {
      readerSchema = HoodieAvroUtils.addMetadataFields(
          new Schema.Parser().parse(config.getSchema()), config.allowOperationMetadataField());
    }
    LOG.info("Compaction operation started for base file: " + operation.getDataFileName() + " and delta files: " + operation.getDeltaFileNames()
        + " for commit " + instantTime);
    // TODO - FIX THIS
    // Reads the entire avro file. Always only specific blocks should be read from the avro file
    // (failure recover).
    // Load all the delta commits since the last compaction commit and get all the blocks to be
    // loaded and load it using CompositeAvroLogReader
    // Since a DeltaCommit is not defined yet, reading all the records. revisit this soon.

    long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(taskContextSupplier, config);
    LOG.info("MaxMemoryPerCompaction => " + maxMemoryPerCompaction);

    List<String> logFiles = operation.getDeltaFileNames().stream().map(p ->
            new StoragePath(FSUtils.constructAbsolutePath(
                metaClient.getBasePath(), operation.getPartitionPath()), p).toString())
        .collect(toList());
    try (HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
          .withStorage(storage)
          .withBasePath(metaClient.getBasePath())
          .withLogFilePaths(logFiles)
          .withReaderSchema(readerSchema)
          .withLatestInstantTime(executionHelper.instantTimeToUseForScanning(instantTime, maxInstantTime))
          .withInstantRange(instantRange)
          .withInternalSchema(internalSchemaOption.orElse(InternalSchema.getEmptyInternalSchema()))
          .withMaxMemorySizeInBytes(maxMemoryPerCompaction)
          .withReverseReader(config.getCompactionReverseLogReadEnabled())
          .withBufferSize(config.getMaxDFSStreamBufferSize())
          .withSpillableMapBasePath(config.getSpillableMapBasePath())
          .withDiskMapType(config.getCommonConfig().getSpillableDiskMapType())
          .withBitCaskDiskMapCompressionEnabled(config.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
          .withOperationField(config.allowOperationMetadataField())
          .withPartition(operation.getPartitionPath())
          .withOptimizedLogBlocksScan(executionHelper.enableOptimizedLogBlockScan(config))
          .withRecordMerger(config.getRecordMerger())
          .withTableMetaClient(metaClient)
          .build()) {

      Option<HoodieBaseFile> oldDataFileOpt =
          operation.getBaseFile(metaClient.getBasePath().toString(), operation.getPartitionPath());

      // Considering following scenario: if all log blocks in this fileSlice is rollback, it returns an empty scanner.
      // But in this case, we need to give it a base file. Otherwise, it will lose base file in following fileSlice.
      if (!scanner.iterator().hasNext()) {
        if (!oldDataFileOpt.isPresent()) {
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
      result = executionHelper.writeFileAndGetWriteStats(compactionHandler, operation, instantTime, scanner, oldDataFileOpt);

      Iterable<List<WriteStatus>> resultIterable = () -> result;
      return StreamSupport.stream(resultIterable.spliterator(), false).flatMap(Collection::stream).peek(s -> {
        final HoodieWriteStat stat = s.getStat();
        stat.setTotalUpdatedRecordsCompacted(scanner.getNumMergedRecordsInLog());
        stat.setTotalLogFilesCompacted(scanner.getTotalLogFiles());
        stat.setTotalLogRecords(scanner.getTotalLogRecords());
        stat.setPartitionPath(operation.getPartitionPath());
        stat
            .setTotalLogSizeCompacted(operation.getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());
        stat.setTotalLogBlocks(scanner.getTotalLogBlocks());
        stat.setTotalCorruptLogBlock(scanner.getTotalCorruptBlocks());
        stat.setTotalRollbackBlocks(scanner.getTotalRollbacks());
        RuntimeStats runtimeStats = new RuntimeStats();
        // scan time has to be obtained from scanner.
        runtimeStats.setTotalScanTime(scanner.getTotalTimeTakenToReadAndMergeBlocks());
        // create and upsert time are obtained from the create or merge handle.
        if (stat.getRuntimeStats() != null) {
          runtimeStats.setTotalCreateTime(stat.getRuntimeStats().getTotalCreateTime());
          runtimeStats.setTotalUpsertTime(stat.getRuntimeStats().getTotalUpsertTime());
        }
        stat.setRuntimeStats(runtimeStats);
      }).collect(toList());
    }
  }

  /**
   * Execute a single compaction operation and report back status.
   */
  public List<WriteStatus> compact(HoodieCompactionHandler compactionHandler,
                                   HoodieWriteConfig writeConfig,
                                   CompactionOperation operation,
                                   String instantTime,
                                   HoodieReaderContext hoodieReaderContext) throws IOException {
    return compactionHandler.compactUsingFileGroupReader(instantTime, operation, writeConfig, hoodieReaderContext);
  }

  public String getMaxInstantTime(HoodieTableMetaClient metaClient) {
    String maxInstantTime = metaClient
        .getActiveTimeline().getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION,
            HoodieTimeline.ROLLBACK_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION))
        .filterCompletedInstants().lastInstant().get().requestedTime();
    return maxInstantTime;
  }

  public CompactionExecutionHelper getCompactionExecutionStrategy(HoodieCompactionPlan compactionPlan) {
    if (compactionPlan.getStrategy() == null || StringUtils.isNullOrEmpty(compactionPlan.getStrategy().getCompactorClassName())) {
      return new CompactionExecutionHelper();
    } else {
      CompactionExecutionHelper executionStrategy = ReflectionUtils.loadClass(compactionPlan.getStrategy().getCompactorClassName());
      return executionStrategy;
    }
  }

  private boolean hasBootstrapFile(List<CompactionOperation> operationList) {
    return operationList.stream().anyMatch(operation -> operation.getBootstrapFilePath().isPresent());
  }
}
