/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS;
import static org.apache.hudi.common.model.HoodieFileFormat.HFILE;

/**
 * A merge handle implementation based on the {@link HoodieFileGroupReader}.
 * <p>
 * This merge handle is used for compaction, which passes a file slice from the
 * compaction operation of a single file group to a file group reader, get an iterator of
 * the records, and writes the records to a new base file.
 */
@NotThreadSafe
public class FileGroupReaderBasedMergeHandle<T, I, K, O> extends HoodieWriteMergeHandle<T, I, K, O> {
  private static final Logger LOG = LoggerFactory.getLogger(FileGroupReaderBasedMergeHandle.class);

  private final Option<CompactionOperation> compactionOperation;
  private final String maxInstantTime;
  private HoodieReaderContext<T> readerContext;
  private HoodieReadStats readStats;
  private HoodieRecord.HoodieRecordType recordType;
  private Option<HoodieCDCLogger> cdcLogger;
  private final TypedProperties props;
  private final Iterator<HoodieRecord<T>> incomingRecordsItr;

  /**
   * Constructor for Copy-On-Write (COW) merge path.
   * Takes in a base path and an iterator of records to be merged with that file.
   *
   * @param config instance of {@link HoodieWriteConfig} to use.
   * @param instantTime instant time of the current commit.
   * @param hoodieTable instance of {@link HoodieTable} being updated.
   * @param recordItr iterator of records to be merged with the file.
   * @param partitionPath partition path of the base file.
   * @param fileId file ID of the base file.
   * @param taskContextSupplier instance of {@link TaskContextSupplier} to use.
   * @param keyGeneratorOpt optional instance of {@link BaseKeyGenerator} to use for extracting keys from records.
   */
  public FileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                         Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                         TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    this(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, getLatestBaseFile(hoodieTable, partitionPath, fileId), keyGeneratorOpt);
  }

  /**
   * Constructor for Copy-On-Write (COW) merge path.
   * Takes in a base path and an iterator of records to be merged with that file.
   *
   * @param config instance of {@link HoodieWriteConfig} to use.
   * @param instantTime instant time of the current commit.
   * @param hoodieTable instance of {@link HoodieTable} being updated.
   * @param recordItr iterator of records to be merged with the file.
   * @param partitionPath partition path of the base file.
   * @param fileId file ID of the base file.
   * @param taskContextSupplier instance of {@link TaskContextSupplier} to use.
   * @param baseFile current base file that needs to be read for the records in storage.
   * @param keyGeneratorOpt optional instance of {@link BaseKeyGenerator} to use for extracting keys from records.
   */
  public FileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                         Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                         TaskContextSupplier taskContextSupplier, HoodieBaseFile baseFile, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, baseFile, keyGeneratorOpt);
    this.compactionOperation = Option.empty();
    this.readerContext = hoodieTable.getReaderContextFactoryForWrite().getContext();
    TypedProperties properties = config.getProps();
    properties.putAll(hoodieTable.getMetaClient().getTableConfig().getProps());
    this.maxInstantTime = instantTime;
    initRecordTypeAndCdcLogger(hoodieTable.getConfig().getRecordMerger().getRecordType());
    this.props = TypedProperties.copy(config.getProps());
    this.incomingRecordsItr = recordItr;
  }

  /**
   * Constructor used for Compaction flows.
   * Take in a base path and list of log files, to merge them together to produce a new base file.
   *
   * @param config instance of {@link HoodieWriteConfig} to use.
   * @param instantTime instant time of interest.
   * @param hoodieTable instance of {@link HoodieTable} to use.
   * @param compactionOperation compaction operation containing info about base and log files.
   * @param taskContextSupplier instance of {@link TaskContextSupplier} to use.
   * @param maxInstantTime max instant time to use.
   * @param enginRecordType engine record type.
   */
  @SuppressWarnings("unused")
  public FileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                         CompactionOperation compactionOperation, TaskContextSupplier taskContextSupplier,
                                         HoodieReaderContext<T> readerContext, String maxInstantTime,
                                         HoodieRecord.HoodieRecordType enginRecordType) {
    super(config, instantTime, compactionOperation.getPartitionPath(), compactionOperation.getFileId(), hoodieTable, taskContextSupplier);
    this.maxInstantTime = maxInstantTime;
    this.keyToNewRecords = Collections.emptyMap();
    this.readerContext = readerContext;
    this.compactionOperation = Option.of(compactionOperation);
    initRecordTypeAndCdcLogger(enginRecordType);
    init(compactionOperation, this.partitionPath);
    this.props = TypedProperties.copy(config.getProps());
    this.incomingRecordsItr = null;
  }

  private void initRecordTypeAndCdcLogger(HoodieRecord.HoodieRecordType enginRecordType) {
    // If the table is a metadata table or the base file is an HFile, we use AVRO record type, otherwise we use the engine record type.
    this.recordType = hoodieTable.isMetadataTable() || HFILE.getFileExtension().equals(hoodieTable.getBaseFileExtension()) ? HoodieRecord.HoodieRecordType.AVRO : enginRecordType;
    if (hoodieTable.getMetaClient().getTableConfig().isCDCEnabled()) {
      this.cdcLogger = Option.of(new HoodieCDCLogger(
          instantTime,
          config,
          hoodieTable.getMetaClient().getTableConfig(),
          partitionPath,
          storage,
          getWriterSchema(),
          createLogWriter(instantTime, HoodieCDCUtils.CDC_LOGFILE_SUFFIX, Option.empty()),
          IOUtils.getMaxMemoryPerPartitionMerge(taskContextSupplier, config)));
    } else {
      this.cdcLogger = Option.empty();
    }
  }

  private void init(CompactionOperation operation, String partitionPath) {
    LOG.info("partitionPath:{}, fileId to be merged:{}", partitionPath, fileId);
    this.baseFileToMerge = operation.getBaseFile(config.getBasePath(), operation.getPartitionPath()).orElse(null);
    this.writtenRecordKeys = new HashSet<>();
    writeStatus.setStat(new HoodieWriteStat());
    writeStatus.getStat().setTotalLogSizeCompacted(
        operation.getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());
    try {
      Option<String> latestValidFilePath = Option.empty();
      if (baseFileToMerge != null) {
        latestValidFilePath = Option.of(baseFileToMerge.getFileName());
        writeStatus.getStat().setPrevCommit(baseFileToMerge.getCommitTime());
        // At the moment, we only support SI for overwrite with latest payload. So, we don't need to embed entire file slice here.
        // HUDI-8518 will be taken up to fix it for any payload during which we might require entire file slice to be set here.
        // Already AppendHandle adds all logs file from current file slice to HoodieDeltaWriteStat.
        writeStatus.getStat().setPrevBaseFile(latestValidFilePath.get());
      } else {
        writeStatus.getStat().setPrevCommit(HoodieWriteStat.NULL_COMMIT);
      }

      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(storage, instantTime,
          new StoragePath(config.getBasePath()),
          FSUtils.constructAbsolutePath(config.getBasePath(), partitionPath),
          hoodieTable.getPartitionMetafileFormat());
      partitionMetadata.trySave();

      String newFileName = FSUtils.makeBaseFileName(instantTime, writeToken, fileId, hoodieTable.getBaseFileExtension());
      makeOldAndNewFilePaths(partitionPath,
          latestValidFilePath.isPresent() ? latestValidFilePath.get() : null, newFileName);

      LOG.info("Merging data from file group {}, to a new base file {}", fileId, newFilePath);
      // file name is same for all records, in this bunch
      writeStatus.setFileId(fileId);
      writeStatus.setPartitionPath(partitionPath);
      writeStatus.getStat().setPartitionPath(partitionPath);
      writeStatus.getStat().setFileId(fileId);
      setWriteStatusPath();

      // Create Marker file,
      // uses name of `newFilePath` instead of `newFileName`
      // in case the sub-class may roll over the file handle name.
      createMarkerFile(partitionPath, newFilePath.getName());

      // Create the writer for writing the new version file
      fileWriter = HoodieFileWriterFactory.getFileWriter(instantTime, newFilePath, hoodieTable.getStorage(),
          config, writeSchemaWithMetaFields, taskContextSupplier, recordType);
    } catch (IOException io) {
      writeStatus.setGlobalError(io);
      throw new HoodieUpsertException("Failed to initialize HoodieUpdateHandle for FileId: " + fileId + " on commit "
          + instantTime + " on path " + hoodieTable.getMetaClient().getBasePath(), io);
    }
  }

  @Override
  protected void populateIncomingRecordsMap(Iterator<HoodieRecord<T>> newRecordsItr) {
    // no op.
  }

  /**
   * This is only for spark, the engine context fetched from a serialized hoodie table is always local,
   * overrides it to spark specific reader context.
   */
  public void setReaderContext(HoodieReaderContext<T> readerContext) {
    this.readerContext = readerContext;
  }

  /**
   * Reads the file slice of a compaction operation using a file group reader,
   * by getting an iterator of the records; then writes the records to a new base file.
   */
  @Override
  public void doMerge() {
    // For non-compaction operations, the merger needs to be initialized with the writer properties to handle cases like Merge-Into commands
    if (compactionOperation.isEmpty()) {
      this.readerContext.initRecordMergerForIngestion(config.getProps());
    }
    boolean usePosition = config.getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    Option<InternalSchema> internalSchemaOption = SerDeHelper.fromJson(config.getInternalSchema())
        .map(internalSchema -> AvroSchemaEvolutionUtils.reconcileSchema(writeSchemaWithMetaFields, internalSchema,
            config.getBooleanOrDefault(HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS)));
    long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(taskContextSupplier, config);
    props.put(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(maxMemoryPerCompaction));
    Option<Stream<HoodieLogFile>> logFilesStreamOpt = compactionOperation.map(op -> op.getDeltaFileNames().stream().map(logFileName ->
        new HoodieLogFile(new StoragePath(FSUtils.constructAbsolutePath(
            config.getBasePath(), op.getPartitionPath()), logFileName))));
    // Initializes file group reader
    try (HoodieFileGroupReader<T> fileGroupReader = getFileGroupReader(usePosition, internalSchemaOption, props, logFilesStreamOpt, incomingRecordsItr)) {
      // Reads the records from the file slice
      try (ClosableIterator<HoodieRecord<T>> recordIterator = fileGroupReader.getClosableHoodieRecordIterator()) {
        while (recordIterator.hasNext()) {
          HoodieRecord<T> record = recordIterator.next();
          Option<Map<String, String>> recordMetadata = compactionOperation.isEmpty() ? getRecordMetadata(record, writeSchema, props) : Option.empty();
          record.setCurrentLocation(newRecordLocation);
          record.setNewLocation(newRecordLocation);
          if (!partitionPath.equals(record.getPartitionPath())) {
            HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
                + record.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
            writeStatus.markFailure(record, failureEx, recordMetadata);
            continue;
          }
          // Writes the record
          try {
            // For Compaction operations, the preserveMetadata flag is always true as we want to preserve the existing record metadata.
            // For other updates, we only want to preserve the metadata if the record is not being modified by this update. If the record already exists in the base file and is not updated,
            // the operation will be null. Records that are being updated or records being added to the file group for the first time will have an operation set and must generate new metadata.
            boolean shouldPreserveRecordMetadata = preserveMetadata || record.getOperation() == null;
            Schema recordSchema = shouldPreserveRecordMetadata ? writeSchemaWithMetaFields : writeSchema;
            writeToFile(record.getKey(), record, recordSchema, config.getPayloadConfig().getProps(), shouldPreserveRecordMetadata);
            writeStatus.markSuccess(record, recordMetadata);
            recordsWritten++;
          } catch (Exception e) {
            LOG.error("Error writing record {}", record, e);
            writeStatus.markFailure(record, e, recordMetadata);
            fileGroupReader.onWriteFailure(record.getRecordKey());
          }
        }

        // The stats of inserts, updates, and deletes are updated once at the end
        // These will be set in the write stat when closing the merge handle
        this.readStats = fileGroupReader.getStats();
        this.insertRecordsWritten = readStats.getNumInserts();
        this.updatedRecordsWritten = readStats.getNumUpdates();
        this.recordsDeleted = readStats.getNumDeletes();
      }
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to compact file group: " + fileId, e);
    }
  }

  private HoodieFileGroupReader<T> getFileGroupReader(boolean usePosition, Option<InternalSchema> internalSchemaOption, TypedProperties props,
                                                      Option<Stream<HoodieLogFile>> logFileStreamOpt, Iterator<HoodieRecord<T>> incomingRecordsItr) {
    HoodieFileGroupReader.Builder<T> fileGroupBuilder = HoodieFileGroupReader.<T>newBuilder().withReaderContext(readerContext).withHoodieTableMetaClient(hoodieTable.getMetaClient())
        .withLatestCommitTime(maxInstantTime).withPartitionPath(partitionPath).withBaseFileOption(Option.ofNullable(baseFileToMerge))
        .withDataSchema(writeSchemaWithMetaFields).withRequestedSchema(writeSchemaWithMetaFields)
        .withInternalSchema(internalSchemaOption).withProps(props)
        .withShouldUseRecordPosition(usePosition).withSortOutput(hoodieTable.requireSortedRecords())
        .withFileGroupUpdateCallback(createCallback());

    if (logFileStreamOpt.isPresent()) {
      fileGroupBuilder.withLogFiles(logFileStreamOpt.get());
    } else {
      fileGroupBuilder.withRecordIterator(incomingRecordsItr);
    }
    return fileGroupBuilder.build();
  }

  @Override
  protected void writeIncomingRecords() {
    // no operation.
  }

  @Override
  public List<WriteStatus> close() {
    try {
      super.close();
      cdcLogger.ifPresent(logger -> {
        logger.close();
        writeStatus.getStat().setCdcStats(logger.getCDCWriteStats());
      });
      writeStatus.getStat().setTotalLogReadTimeMs(readStats.getTotalLogReadTimeMs());
      writeStatus.getStat().setTotalUpdatedRecordsCompacted(readStats.getTotalUpdatedRecordsCompacted());
      writeStatus.getStat().setTotalLogFilesCompacted(readStats.getTotalLogFilesCompacted());
      writeStatus.getStat().setTotalLogRecords(readStats.getTotalLogRecords());
      writeStatus.getStat().setTotalLogBlocks(readStats.getTotalLogBlocks());
      writeStatus.getStat().setTotalCorruptLogBlock(readStats.getTotalCorruptLogBlock());
      writeStatus.getStat().setTotalRollbackBlocks(readStats.getTotalRollbackBlocks());
      if (compactionOperation.isPresent()) {
        writeStatus.getStat().setTotalLogSizeCompacted(compactionOperation.get().getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());
      }

      if (writeStatus.getStat().getRuntimeStats() != null) {
        writeStatus.getStat().getRuntimeStats().setTotalScanTime(readStats.getTotalLogReadTimeMs());
      }
      return Collections.singletonList(writeStatus);
    } catch (Exception e) {
      throw new HoodieUpsertException("Failed to close " + this.getClass().getSimpleName(), e);
    }
  }

  private Option<BaseFileUpdateCallback<T>> createCallback() {
    List<BaseFileUpdateCallback<T>> callbacks = new ArrayList<>();
    // Handle CDC workflow.
    if (cdcLogger.isPresent()) {
      callbacks.add(new CDCCallback<>(cdcLogger.get(), readerContext));
    }
    // Indexes are not updated during compaction
    if (compactionOperation.isEmpty()) {
      // record index callback
      if (this.writeStatus.isTrackingSuccessfulWrites()) {
        writeStatus.manuallyTrackSuccess();
        RecordLevelIndexCallback<T> recordLevelIndexCallback = new RecordLevelIndexCallback<>(writeStatus, newRecordLocation, partitionPath);
        callbacks.add(recordLevelIndexCallback);
      }
      // Stream secondary index stats.
      if (isSecondaryIndexStatsStreamingWritesEnabled) {
        SecondaryIndexCallback<T> secondaryIndexCallback = new SecondaryIndexCallback<>(
            partitionPath,
            readerContext,
            writeStatus,
            secondaryIndexDefns);
        callbacks.add(secondaryIndexCallback);
      }
    }
    return callbacks.isEmpty() ? Option.empty() : Option.of(CompositeCallback.of(callbacks));
  }

  private static class CDCCallback<T> implements BaseFileUpdateCallback<T> {
    private final HoodieCDCLogger cdcLogger;
    private final RecordContext<T> recordContext;
    // Lazy is used because the schema handler within the reader context is not initialized until the FileGroupReader is fully constructed.
    // This allows the values to be fetched at runtime when iterating through the records.
    private final Lazy<Schema> requestedSchema;
    private final Map<Integer, Option<UnaryOperator<T>>> converterCache;

    CDCCallback(HoodieCDCLogger cdcLogger, HoodieReaderContext<T> readerContext) {
      this.cdcLogger = cdcLogger;
      this.recordContext = readerContext.getRecordContext();
      this.requestedSchema = Lazy.lazily(() -> readerContext.getSchemaHandler().getRequestedSchema());
      this.converterCache = new HashMap<>();
    }

    @Override
    public void onUpdate(String recordKey, BufferedRecord<T> previousRecord, BufferedRecord<T> mergedRecord) {
      cdcLogger.put(recordKey, convertOutput(previousRecord), Option.of(convertOutput(mergedRecord)));
    }

    @Override
    public void onInsert(String recordKey, BufferedRecord<T> newRecord) {
      cdcLogger.put(recordKey, null, Option.of(convertOutput(newRecord)));
    }

    @Override
    public void onDelete(String recordKey, BufferedRecord<T> previousRecord, HoodieOperation hoodieOperation) {
      // delete record from log block and update no base record from base file, skip generating changelog.
      if (previousRecord == null) {
        return;
      }
      cdcLogger.put(recordKey, convertOutput(previousRecord), Option.empty());
    }

    @Override
    public void onFailure(String recordKey) {
      cdcLogger.remove(recordKey);
    }

    private GenericRecord convertOutput(BufferedRecord<T> record) {
      if (record == null || record.getRecord() == null) {
        return null;
      }
      Option<UnaryOperator<T>> converterOpt = converterCache.computeIfAbsent(record.getSchemaId(), schemaId -> {
        Schema recordSchema = recordContext.decodeAvroSchema(schemaId);
        if (AvroSchemaUtils.areSchemasProjectionEquivalent(recordSchema, requestedSchema.get())) {
          return Option.empty();
        } else {
          return Option.of(recordContext.projectRecord(recordSchema, requestedSchema.get()));
        }
      });
      T data = record.getRecord();
      T convertedRecord = converterOpt.map(converter -> converter.apply(data)).orElse(data);
      return recordContext.convertToAvroRecord(convertedRecord, requestedSchema.get());
    }
  }

  private static class RecordLevelIndexCallback<T> implements BaseFileUpdateCallback<T> {
    private final WriteStatus writeStatus;
    private final HoodieRecordLocation fileRecordLocation;
    private final String partitionPath;

    public RecordLevelIndexCallback(WriteStatus writeStatus, HoodieRecordLocation fileRecordLocation, String partitionPath) {
      this.writeStatus = writeStatus;
      this.fileRecordLocation = fileRecordLocation;
      this.partitionPath = partitionPath;
    }

    @Override
    public void onUpdate(String recordKey, BufferedRecord<T> previousRecord, BufferedRecord<T> mergedRecord) {
      writeStatus.addRecordDelegate(HoodieRecordDelegate.create(recordKey, partitionPath, fileRecordLocation, fileRecordLocation));
    }

    @Override
    public void onInsert(String recordKey, BufferedRecord<T> newRecord) {
      writeStatus.addRecordDelegate(HoodieRecordDelegate.create(recordKey, partitionPath, null, fileRecordLocation));
    }

    @Override
    public void onDelete(String recordKey, BufferedRecord<T> previousRecord, HoodieOperation hoodieOperation) {
      // The update before operation is used when a deletion is being sent to the old File Group in a different partition.
      // In this case, we do not want to delete the record metadata from the index.
      if (hoodieOperation != HoodieOperation.UPDATE_BEFORE) {
        writeStatus.addRecordDelegate(HoodieRecordDelegate.create(recordKey, partitionPath, fileRecordLocation, null));
      }
    }

    @Override
    public void onFailure(String recordKey) {
      int lastIndex = writeStatus.getWrittenRecordDelegates().size() - 1;
      if (lastIndex >= 0 && writeStatus.getWrittenRecordDelegates().get(lastIndex).getRecordKey().equals(recordKey)) {
        writeStatus.getWrittenRecordDelegates().remove(lastIndex);
      }
    }
  }

  private static class SecondaryIndexCallback<T> implements BaseFileUpdateCallback<T> {
    private final String partitionPath;
    private final HoodieReaderContext<T> readerContext;
    private final WriteStatus writeStatus;
    private final List<HoodieIndexDefinition> secondaryIndexDefns;

    public SecondaryIndexCallback(String partitionPath,
                                  HoodieReaderContext<T> readerContext,
                                  WriteStatus writeStatus,
                                  List<HoodieIndexDefinition> secondaryIndexDefns) {
      this.partitionPath = partitionPath;
      this.readerContext = readerContext;
      this.secondaryIndexDefns = secondaryIndexDefns;
      this.writeStatus = writeStatus;
    }

    @Override
    public void onUpdate(String recordKey, BufferedRecord<T> previousRecord, BufferedRecord<T> mergedRecord) {
      HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
      SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
          hoodieKey,
          Option.of(mergedRecord),
          previousRecord,
          false,
          writeStatus,
          secondaryIndexDefns,
          readerContext.getRecordContext());
    }

    @Override
    public void onInsert(String recordKey, BufferedRecord<T> newRecord) {
      HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
      SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
          hoodieKey,
          Option.of(newRecord),
          null,
          false,
          writeStatus,
          secondaryIndexDefns,
          readerContext.getRecordContext());
    }

    @Override
    public void onDelete(String recordKey, BufferedRecord<T> previousRecord, HoodieOperation hoodieOperation) {
      HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
      SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
          hoodieKey,
          Option.empty(),
          previousRecord,
          true,
          writeStatus,
          secondaryIndexDefns,
          readerContext.getRecordContext());
    }

    @Override
    public void onFailure(String recordKey) {
      writeStatus.getIndexStats().getSecondaryIndexStats().forEach((partition, indexStats) -> {
        List<Integer> indicesToRemove = new ArrayList<>();
        for (int i = indexStats.size() - 1; i >= 0; i--) {
          if (indexStats.get(i).getRecordKey().equals(recordKey)) {
            indicesToRemove.add(i);
          } else {
            break;
          }
        }
        indicesToRemove.forEach(index -> indexStats.remove((int) index));
      });
    }
  }

  private static class CompositeCallback<T> implements BaseFileUpdateCallback<T> {
    private final List<BaseFileUpdateCallback<T>> callbacks;

    static <T> BaseFileUpdateCallback<T> of(List<BaseFileUpdateCallback<T>> callbacks) {
      if (callbacks.size() == 1) {
        return callbacks.get(0);
      }
      return new CompositeCallback<>(callbacks);
    }

    private CompositeCallback(List<BaseFileUpdateCallback<T>> callbacks) {
      this.callbacks = callbacks;
    }

    @Override
    public void onUpdate(String recordKey, BufferedRecord<T> previousRecord, BufferedRecord<T> mergedRecord) {
      this.callbacks.forEach(callback -> callback.onUpdate(recordKey, previousRecord, mergedRecord));
    }

    @Override
    public void onInsert(String recordKey, BufferedRecord<T> newRecord) {
      this.callbacks.forEach(callback -> callback.onInsert(recordKey, newRecord));
    }

    @Override
    public void onDelete(String recordKey, BufferedRecord<T> previousRecord, HoodieOperation hoodieOperation) {
      this.callbacks.forEach(callback -> callback.onDelete(recordKey, previousRecord, hoodieOperation));
    }

    @Override
    public void onFailure(String recordKey) {
      this.callbacks.forEach(callback -> callback.onFailure(recordKey));
    }
  }
}
