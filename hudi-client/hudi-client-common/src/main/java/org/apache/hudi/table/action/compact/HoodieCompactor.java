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
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormatReader;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.CompactorBroadcastManager;
import org.apache.hudi.table.HoodieCompactionHandler;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.DELETE_BLOCK;

/**
 * A HoodieCompactor runs compaction on a hoodie table.
 */
public abstract class HoodieCompactor<T, I, K, O> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieCompactor.class);

  public Option<CompactorBroadcastManager> getCompactorBroadcastManager(HoodieEngineContext context) {
    return Option.empty();
  }

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
      HoodieEngineContext context, HoodieCompactionPlan compactionPlan,
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

    // Broadcast necessary information.
    Option<CompactorBroadcastManager> broadcastManagerOpt = getCompactorBroadcastManager(context);
    if (broadcastManagerOpt.isPresent()) {
      broadcastManagerOpt.get().prepareAndBroadcast();
    }
    return context.parallelize(operations).map(operation -> compact(
        compactionHandler, metaClient, config, operation, compactionInstantTime, maxInstantTime, instantRange, taskContextSupplier, executionHelper, broadcastManagerOpt))
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
                                   String maxInstantTime,
                                   TaskContextSupplier taskContextSupplier) throws IOException {
    return compact(compactionHandler, metaClient, config, operation, instantTime, maxInstantTime, Option.empty(),
        taskContextSupplier, new CompactionExecutionHelper(), Option.empty());
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
                                   CompactionExecutionHelper executionHelper,
                                   Option<CompactorBroadcastManager> broadcastManagerOpt
  ) throws IOException {
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

    if (broadcastManagerOpt.isPresent() && !isMetadataTable(metaClient)) { // For spark, but not MDT.
      return compactUsingFileGroupReader(logFiles, readerSchema, instantTime, internalSchemaOption, config, operation, metaClient, broadcastManagerOpt, taskContextSupplier);
    } else {
      // if not for partail update, we can go through regular way of compacting.
      return compactUsingLegacyMethod(storage, logFiles, readerSchema, executionHelper, instantTime, maxInstantTime, instantRange, internalSchemaOption, config,
          maxMemoryPerCompaction, operation, metaClient, compactionHandler);
    }
  }

  private List<WriteStatus> compactUsingFileGroupReader(List<String> logFiles, Schema readerSchema, String instantTime,
                                                        Option<InternalSchema> internalSchemaOption, HoodieWriteConfig config,
                                                        CompactionOperation operation, HoodieTableMetaClient metaClient,
                                                        Option<CompactorBroadcastManager> partialUpdateReaderContextOpt,
                                                        TaskContextSupplier taskContextSupplier) throws IOException {
    CompactorBroadcastManager compactorBroadcastManager = partialUpdateReaderContextOpt.get();
    // PATH 1: When the engine decides to return a valid reader context object.
    Option<HoodieReaderContext> readerContextOpt = compactorBroadcastManager.retrieveFileGroupReaderContext(metaClient.getBasePath());
    ValidationUtils.checkArgument(readerContextOpt.isPresent(),"ReaderContext has to be set for compaction using new file group reader ");
    Option<Configuration> configurationOpt = compactorBroadcastManager.retrieveStorageConfig();
    ValidationUtils.checkArgument(configurationOpt.isPresent(), "Custom Configuration has to be set for compaction using new file group reader ");

    HoodieTableMetaClient newMetaClient = HoodieTableMetaClient.builder()
        .setBasePath(metaClient.getBasePath())
        .setConf(new HadoopStorageConfiguration(configurationOpt.get()))
        .build();
    List<HoodieLogFile> hoodieLogFiles = logFiles.stream()
        .map(p -> new HoodieLogFile(new StoragePath(p))).collect(toList());
    return compactWithFileGroupReader(
        readerContextOpt.get(),
        instantTime,
        newMetaClient,
        operation,
        hoodieLogFiles,
        readerSchema,
        internalSchemaOption,
        newMetaClient.getTableConfig().getProps(),
        config,
        taskContextSupplier);
  }

  private List<WriteStatus> compactUsingLegacyMethod(HoodieStorage storage, List<String> logFiles, Schema readerSchema,
                                                     CompactionExecutionHelper executionHelper, String instantTime, String maxInstantTime,
                                                     Option<InstantRange> instantRange,
                                                     Option<InternalSchema> internalSchemaOption, HoodieWriteConfig config, long maxMemoryPerCompaction,
                                                     CompactionOperation operation, HoodieTableMetaClient metaClient,
                                                     HoodieCompactionHandler compactionHandler) throws IOException {
    HoodieMergedLogRecordScanner scanner = getLogRecordScanner(storage, metaClient.getBasePath(), logFiles, readerSchema,
        executionHelper, instantTime, maxInstantTime, instantRange, internalSchemaOption.orElse(InternalSchema.getEmptyInternalSchema()),
        config, maxMemoryPerCompaction, operation, metaClient, false);

    Option<HoodieBaseFile> oldDataFileOpt =
        operation.getBaseFile(metaClient.getBasePath().toString(), operation.getPartitionPath());

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
    result = executionHelper.writeFileAndGetWriteStats(compactionHandler, operation, instantTime, scanner, oldDataFileOpt);
    scanner.close();
    Iterable<List<WriteStatus>> resultIterable = () -> result;
    return StreamSupport.stream(resultIterable.spliterator(), false).flatMap(Collection::stream).peek(s -> {
      final HoodieWriteStat stat = s.getStat();
      stat.setTotalUpdatedRecordsCompacted(scanner.getNumMergedRecordsInLog());
      stat.setTotalLogFilesCompacted(scanner.getTotalLogFiles());
      stat.setTotalLogRecords(scanner.getTotalLogRecords());
      stat.setPartitionPath(operation.getPartitionPath());
      stat.setTotalLogSizeCompacted(
          operation.getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());
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

  private HoodieMergedLogRecordScanner getLogRecordScanner(HoodieStorage storage, StoragePath basePath, List<String> logFiles, Schema readerSchema,
                                                           CompactionExecutionHelper executionHelper, String instantTime, String maxInstantTime,
                                                           Option<InstantRange> instantRange,
                                                           InternalSchema internalSchema, HoodieWriteConfig config, long maxMemoryPerCompaction,
                                                           CompactionOperation operation, HoodieTableMetaClient metaClient,
                                                           boolean onlyDeducePartialUpdates) {

    return HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(logFiles)
        .withReaderSchema(readerSchema)
        .withLatestInstantTime(executionHelper.instantTimeToUseForScanning(instantTime, maxInstantTime))
        .withInstantRange(instantRange)
        .withInternalSchema(internalSchema)
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
        .onlyDetectPartialUpdates(onlyDeducePartialUpdates)
        .build();
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

  List<WriteStatus> compactWithFileGroupReader(HoodieReaderContext readerContext,
                                               String instantTime,
                                               HoodieTableMetaClient metaClient,
                                               CompactionOperation operation,
                                               List<HoodieLogFile> logFiles,
                                               Schema readerSchema,
                                               Option<InternalSchema> internalSchemaOpt,
                                               TypedProperties props,
                                               HoodieWriteConfig config,
                                               TaskContextSupplier taskContextSupplier) throws IOException {
    HoodieTimer timer = HoodieTimer.start();
    timer.startTimer();
    Option<HoodieBaseFile> baseFileOpt =
        operation.getBaseFile(metaClient.getBasePath().toString(), operation.getPartitionPath());
    FileSlice fileSlice = new FileSlice(
        operation.getFileGroupId(),
        operation.getBaseInstantTime(),
        baseFileOpt.isPresent() ? baseFileOpt.get() : null,
        logFiles);

    // 1. Generate the input for fg reader.
    boolean usePosition = config.getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    HoodieFileGroupReader<T> fileGroupReader = new HoodieFileGroupReader<>(
        readerContext,
        metaClient.getStorage(),
        metaClient.getBasePath().toString(),
        instantTime,
        fileSlice,
        readerSchema,
        readerSchema,
        internalSchemaOpt,
        metaClient,
        props,
        0,
        Long.MAX_VALUE,
        usePosition);

    // 2. Get the `HoodieFileGroupReaderIterator` from the fg reader.
    fileGroupReader.initRecordIterators();
    HoodieFileGroupReader.HoodieFileGroupReaderIterator<T> recordIterator
        = fileGroupReader.getClosableIterator();

    // 3. Write the record using parquet writer.
    String writeToken = FSUtils.makeWriteToken(
        taskContextSupplier.getPartitionIdSupplier().get(),
        taskContextSupplier.getStageIdSupplier().get(),
        taskContextSupplier.getAttemptIdSupplier().get());
    String newFileName =
        FSUtils.makeBaseFileName(instantTime, writeToken, operation.getFileId(), ".parquet");
    // Compaction has to use this schema.
    Schema writeSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(readerSchema, config.allowOperationMetadataField());
    StoragePath dirPath = FSUtils.constructAbsolutePath(metaClient.getBasePath().toString(), operation.getPartitionPath());
    StoragePath newBaseFilePath = new StoragePath(dirPath, newFileName);
    WriteStatus writestatus = initNewStatus(config, true, operation);
    long recordsWritten = 0;
    long errorRecords = 0;
    try (HoodieFileWriter fileWriter = HoodieFileWriterFactory.getFileWriter(
        instantTime, newBaseFilePath, metaClient.getStorage(), config,
        writeSchemaWithMetaFields, taskContextSupplier, config.getRecordMerger().getRecordType())) {
      while (recordIterator.hasNext()) {
        T record = recordIterator.next();
        Map<String, String> metadata = readerContext.generateMetadataForRecord(record, readerSchema);
        HoodieRecord hoodieRecord = readerContext.constructHoodieRecord(Option.of(record), metadata, props);
        try {
          fileWriter.write(hoodieRecord.getRecordKey(), hoodieRecord, writeSchemaWithMetaFields);
          recordsWritten++;
          writestatus.markSuccess(hoodieRecord, hoodieRecord.getMetadata());
        } catch (Throwable t) {
          errorRecords++;
          writestatus.markFailure(hoodieRecord, t, hoodieRecord.getMetadata());
          LOG.error("Error write record: {}", record, t);
        }
      }
    }

    // 4. Construct the return values.
    writestatus.setTotalRecords(recordsWritten);
    writestatus.setTotalErrorRecords(errorRecords);
    setupWriteStatus(newBaseFilePath, writestatus, config, metaClient.getStorage(), timer);
    return Collections.singletonList(writestatus);
  }

  private WriteStatus initNewStatus(HoodieWriteConfig config, boolean shouldTrackSuccessRecords, CompactionOperation operation) {
    WriteStatus writeStatus = (WriteStatus) ReflectionUtils.loadClass(
        config.getWriteStatusClassName(), shouldTrackSuccessRecords, config.getWriteStatusFailureFraction());
    HoodieWriteStat stat = new HoodieDeltaWriteStat();
    writeStatus.setFileId(operation.getFileId());
    writeStatus.setPartitionPath(operation.getPartitionPath());
    writeStatus.setStat(stat);
    return writeStatus;
  }

  protected void setupWriteStatus(StoragePath filePath,
                                  WriteStatus writeStatus,
                                  HoodieWriteConfig config,
                                  HoodieStorage storage,
                                  HoodieTimer timer) throws IOException {
    HoodieWriteStat stat = writeStatus.getStat();
    stat.setPartitionPath(writeStatus.getPartitionPath());
    stat.setNumWrites(writeStatus.getTotalRecords());
    stat.setNumDeletes(0);
    stat.setNumInserts(writeStatus.getTotalRecords());
    stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
    stat.setFileId(writeStatus.getFileId());
    stat.setPath(new StoragePath(config.getBasePath()), filePath);
    stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());

    long fileSize = storage.getPathInfo(filePath).getLength();
    stat.setTotalWriteBytes(fileSize);
    stat.setFileSizeInBytes(fileSize);

    RuntimeStats runtimeStats = new RuntimeStats();
    runtimeStats.setTotalCreateTime(timer.endTimer());
    stat.setRuntimeStats(runtimeStats);
  }

  private boolean containsPartialUpdate(Schema readerSchema,
                                        Option<InternalSchema> internalSchema,
                                        List<HoodieLogFile> logFiles,
                                        HoodieTableMetaClient metaClient) throws IOException {
    try (HoodieLogFormatReader logFormatReaderWrapper = new HoodieLogFormatReader(
        metaClient.getStorage(),
        logFiles,
        readerSchema,
        true,
        HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.defaultValue(),
        false,
        metaClient.getTableConfig().getRecordKeyFields().get()[0],
        internalSchema != null ? internalSchema.orElse(null) : null)) {
      while (logFormatReaderWrapper.hasNext()) {
        HoodieLogBlock block = logFormatReaderWrapper.next();
        if (block.isDataOrDeleteBlock()
            && (block.getBlockType() != DELETE_BLOCK
            && ((HoodieDataBlock) block).containsPartialUpdates())) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean isMetadataTable(HoodieTableMetaClient metaClient) {
    return metaClient.getBasePath().getParent().getName().equals(".hoodie");
  }
}
