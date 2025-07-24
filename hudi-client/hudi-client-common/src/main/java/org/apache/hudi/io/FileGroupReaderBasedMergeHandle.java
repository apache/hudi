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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS;
import static org.apache.hudi.common.model.HoodieFileFormat.HFILE;

/**
 * A merge handle implementation based on the {@link HoodieFileGroupReader}.
 * This handle uses {@link HoodieFileGroupReader} to merge either a file slice or merge
 * a base file and a set of input records.
 */
@NotThreadSafe
public class FileGroupReaderBasedMergeHandle<T, I, K, O> extends HoodieWriteMergeHandle<T, I, K, O> {
  private static final Logger LOG = LoggerFactory.getLogger(FileGroupReaderBasedMergeHandle.class);
  private final HoodieReaderContext<T> readerContext;
  private final String maxInstantTime;
  private final HoodieRecord.HoodieRecordType recordType;
  private final Option<HoodieCDCLogger> cdcLogger;
  private final Option<CompactionOperation> operation;
  private final Option<Iterator<HoodieRecord<T>>> recordIterator;
  private HoodieReadStats readStats;

  /**
   * For compactor.
   */
  public FileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                         CompactionOperation operation, TaskContextSupplier taskContextSupplier,
                                         Option<BaseKeyGenerator> keyGeneratorOpt,
                                         HoodieReaderContext<T> readerContext, String maxInstantTime,
                                         HoodieRecord.HoodieRecordType enginRecordType) {
    this(config, instantTime, hoodieTable, Option.empty(), operation.getPartitionPath(), operation.getFileId(),
        taskContextSupplier, keyGeneratorOpt, readerContext, maxInstantTime, enginRecordType, Option.of(operation));
  }

  /**
   * For generic FG reader based merge handle.
   */
  public FileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                         Option<Iterator<HoodieRecord<T>>> recordItr, String partitionPath, String fileId,
                                         TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt,
                                         HoodieReaderContext<T> readerContext, String maxInstantTime,
                                         HoodieRecord.HoodieRecordType enginRecordType, Option<CompactionOperation> operation) {
    super(config, instantTime, hoodieTable, Collections.emptyIterator(), partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
    // For regular merge process, recordIterator exists and operation does not.
    this.recordIterator = recordItr;
    // For compaction process, operation exists and recordIterator does not.
    this.operation = operation;
    // Common attributes.
    this.maxInstantTime = maxInstantTime;
    this.keyToNewRecords = Collections.emptyMap();
    this.readerContext = readerContext;
    this.recordType = hoodieTable.isMetadataTable() || HFILE.getFileExtension().equals(hoodieTable.getBaseFileExtension()) 
        ? HoodieRecord.HoodieRecordType.AVRO : enginRecordType;
    this.cdcLogger = hoodieTable.getMetaClient().getTableConfig().isCDCEnabled() 
        ? Option.of(new HoodieCDCLogger(instantTime, config, hoodieTable.getMetaClient().getTableConfig(),
            partitionPath, storage, getWriterSchema(),
            createLogWriter(instantTime, HoodieCDCUtils.CDC_LOGFILE_SUFFIX, Option.empty()),
            IOUtils.getMaxMemoryPerPartitionMerge(taskContextSupplier, config)))
        : Option.empty();
    init(operation, this.partitionPath);
  }

  private void init(Option<CompactionOperation> operation, String partitionPath) {
    LOG.info("partitionPath:{}, fileId to be merged:{}", partitionPath, fileId);

    this.writtenRecordKeys = new HashSet<>();
    writeStatus.setStat(new HoodieWriteStat());
    if (operation.isPresent()) {
      writeStatus.getStat().setTotalLogSizeCompacted(
          operation.get().getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());
    }

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
  }

  /**
   * Reads the file slice of a compaction operation using a file group reader,
   * by getting an iterator of the records; then writes the records to a new base file.
   */
  @Override
  public void doMerge() {
    boolean usePosition = config.getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    Option<InternalSchema> internalSchemaOption = SerDeHelper.fromJson(config.getInternalSchema());
    TypedProperties props = TypedProperties.copy(config.getProps());
    Stream<HoodieLogFile> logFiles = Stream.empty();
    // For compaction.
    if (operation.isPresent()) {
      long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(taskContextSupplier, config);
      props.put(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(maxMemoryPerCompaction));
      logFiles = operation.get().getDeltaFileNames().stream().map(logFileName ->
          new HoodieLogFile(new StoragePath(FSUtils.constructAbsolutePath(
              config.getBasePath(), operation.get().getPartitionPath()), logFileName)));
    }
    // For generic merge.
    if (recordIterator.isPresent()) {
      usePosition = false;
    }
    // Initializes file group reader.
    try (HoodieFileGroupReader<T> fileGroupReader = HoodieFileGroupReader.<T>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(hoodieTable.getMetaClient())
        .withLatestCommitTime(maxInstantTime)
        .withPartitionPath(partitionPath)
        .withBaseFileOption(Option.ofNullable(baseFileToMerge))
        .withLogFiles(logFiles)
        .withDataSchema(writeSchema)
        .withRequestedSchema(writeSchemaWithMetaFields)
        .withInternalSchema(internalSchemaOption)
        .withProps(props)
        .withShouldUseRecordPosition(usePosition)
        .withSortOutput(hoodieTable.requireSortedRecords())
        .withFileGroupUpdateCallback(createCallbacks())
        .withRecordIterator(recordIterator).build()) {
      // Reads the records from the file slice
      try (ClosableIterator<HoodieRecord<T>> recordIterator = fileGroupReader.getClosableHoodieRecordIterator()) {
        while (recordIterator.hasNext()) {
          HoodieRecord<T> record = recordIterator.next();
          record.setCurrentLocation(newRecordLocation);
          record.setNewLocation(newRecordLocation);
          Option<Map<String, String>> recordMetadata = record.getMetadata();
          if (!partitionPath.equals(record.getPartitionPath())) {
            HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
                + record.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
            writeStatus.markFailure(record, failureEx, recordMetadata);
            continue;
          }
          // Writes the record.
          try {
            writeToFile(record.getKey(), record, writeSchemaWithMetaFields,
                config.getPayloadConfig().getProps(), preserveMetadata);
            writeStatus.markSuccess(record, recordMetadata);
            recordsWritten++;
          } catch (Exception e) {
            LOG.error("Error writing record {}", record, e);
            writeStatus.markFailure(record, e, recordMetadata);
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

  private List<BaseFileUpdateCallback<T>> createCallbacks() {
    List<BaseFileUpdateCallback<T>> callbacks = new ArrayList<>();
    // Handle CDC workflow.
    if (cdcLogger.isPresent()) {
      callbacks.add(new CDCCallback<>(cdcLogger.get(), readerContext));
    }
    // Stream secondary index stats.
    if (isSecondaryIndexStatsStreamingWritesEnabled) {
      callbacks.add(new SecondaryIndexCallback<>(
          partitionPath,
          writeSchemaWithMetaFields,
          readerContext,
          this::getNewSchema,
          writeStatus,
          secondaryIndexDefns,
          keyGeneratorOpt,
          config
      ));
    }
    return callbacks;
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
      if (operation.isPresent()) {
        writeStatus.getStat().setTotalLogSizeCompacted(
            operation.get().getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());
      }

      if (writeStatus.getStat().getRuntimeStats() != null) {
        writeStatus.getStat().getRuntimeStats().setTotalScanTime(readStats.getTotalLogReadTimeMs());
      }
      return Collections.singletonList(writeStatus);
    } catch (Exception e) {
      throw new HoodieUpsertException("Failed to close " + this.getClass().getSimpleName(), e);
    }
  }

  private static class CDCCallback<T> implements BaseFileUpdateCallback<T> {
    private final HoodieCDCLogger cdcLogger;
    private final HoodieReaderContext<T> readerContext;
    // Lazy is used because the schema handler within the reader context is not initialized until the FileGroupReader is fully constructed.
    // This allows the values to be fetched at runtime when iterating through the records.
    private final Lazy<Schema> requestedSchema;
    private final Lazy<Option<UnaryOperator<T>>> outputConverter;

    CDCCallback(HoodieCDCLogger cdcLogger, HoodieReaderContext<T> readerContext) {
      this.cdcLogger = cdcLogger;
      this.readerContext = readerContext;
      this.outputConverter = Lazy.lazily(() -> readerContext.getSchemaHandler().getOutputConverter());
      this.requestedSchema = Lazy.lazily(() -> readerContext.getSchemaHandler().getRequestedSchema());
    }

    @Override
    public void onUpdate(String recordKey, T previousRecord, T mergedRecord) {
      cdcLogger.put(recordKey, convertOutput(previousRecord), Option.of(convertOutput(mergedRecord)));

    }

    @Override
    public void onInsert(String recordKey, T newRecord) {
      cdcLogger.put(recordKey, null, Option.of(convertOutput(newRecord)));

    }

    @Override
    public void onDelete(String recordKey, T previousRecord) {
      cdcLogger.put(recordKey, convertOutput(previousRecord), Option.empty());

    }

    @Override
    public String getName() {
      return "CDCCallback";
    }

    private GenericRecord convertOutput(T record) {
      T convertedRecord = outputConverter.get().map(converter -> record == null ? null : converter.apply(record)).orElse(record);
      return convertedRecord == null ? null : readerContext.convertToAvroRecord(convertedRecord, requestedSchema.get());
    }
  }

  private static class SecondaryIndexCallback<T> implements BaseFileUpdateCallback<T> {
    private final String partitionPath;
    private final Schema writeSchemaWithMetaFields;
    private final HoodieReaderContext<T> readerContext;
    private final Supplier<Schema> newSchemaSupplier;
    private final WriteStatus writeStatus;
    private final List<HoodieIndexDefinition> secondaryIndexDefns;
    private final Option<BaseKeyGenerator> keyGeneratorOpt;
    private final HoodieWriteConfig config;

    public SecondaryIndexCallback(String partitionPath,
                                  Schema writeSchemaWithMetaFields,
                                  HoodieReaderContext<T> readerContext,
                                  Supplier<Schema> newSchemaSupplier,
                                  WriteStatus writeStatus,
                                  List<HoodieIndexDefinition> secondaryIndexDefns,
                                  Option<BaseKeyGenerator> keyGeneratorOpt,
                                  HoodieWriteConfig config) {
      this.partitionPath = partitionPath;
      this.writeSchemaWithMetaFields = writeSchemaWithMetaFields;
      this.readerContext = readerContext;
      this.newSchemaSupplier = newSchemaSupplier;
      this.secondaryIndexDefns = secondaryIndexDefns;
      this.keyGeneratorOpt = keyGeneratorOpt;
      this.writeStatus = writeStatus;
      this.config = config;
    }

    @Override
    public void onUpdate(String recordKey, T previousRecord, T mergedRecord) {
      HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
      BufferedRecord<T> bufferedPreviousRecord = BufferedRecord.forRecordWithContext(
          previousRecord, writeSchemaWithMetaFields, readerContext, Option.empty(), false);
      BufferedRecord<T> bufferedMergedRecord = BufferedRecord.forRecordWithContext(
          mergedRecord, writeSchemaWithMetaFields, readerContext, Option.empty(), false);
      SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
          hoodieKey,
          Option.of(readerContext.constructHoodieRecord(bufferedMergedRecord)),
          readerContext.constructHoodieRecord(bufferedPreviousRecord),
          false,
          writeStatus,
          writeSchemaWithMetaFields,
          newSchemaSupplier,
          secondaryIndexDefns,
          keyGeneratorOpt,
          config);
    }

    @Override
    public void onInsert(String recordKey, T newRecord) {
      HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
      BufferedRecord<T> bufferedNewRecord = BufferedRecord.forRecordWithContext(
          newRecord, writeSchemaWithMetaFields, readerContext, Option.empty(), false);
      SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
          hoodieKey,
          Option.of(readerContext.constructHoodieRecord(bufferedNewRecord)),
          null,
          false,
          writeStatus,
          writeSchemaWithMetaFields,
          newSchemaSupplier,
          secondaryIndexDefns,
          keyGeneratorOpt,
          config);
    }

    @Override
    public void onDelete(String recordKey, T previousRecord) {
      HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
      BufferedRecord<T> bufferedPreviousRecord = BufferedRecord.forRecordWithContext(
          previousRecord, writeSchemaWithMetaFields, readerContext, Option.empty(), false);
      SecondaryIndexStreamingTracker.trackSecondaryIndexStats(
          hoodieKey,
          null,
          readerContext.constructHoodieRecord(bufferedPreviousRecord),
          true,
          writeStatus,
          writeSchemaWithMetaFields,
          newSchemaSupplier,
          secondaryIndexDefns,
          keyGeneratorOpt,
          config);
    }

    @Override
    public String getName() {
      return "SecondaryIndexCallback";
    }
  }
}
