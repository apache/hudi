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
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.internal.schema.InternalSchema;
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
import java.io.Serializable;
import java.util.Collections;
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

  private final HoodieReaderContext<T> readerContext;
  private final Option<CompactionOperation> operation;
  private final String maxInstantTime;
  private HoodieReadStats readStats;
  private HoodieRecord.HoodieRecordType recordType;
  private Option<HoodieCDCLogger> cdcLogger;
  private boolean usePosition;
  private boolean isCompaction;
  private final TypedProperties props;
  private Iterator<BufferedRecord<T>> incomingRecordsItr;

  public FileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                         Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                                         TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt,
                                         HoodieReaderContext<T> readerContext, HoodieRecord.HoodieRecordType enginRecordType) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt);
    this.operation = Option.empty();
    this.readerContext = readerContext;
    this.maxInstantTime = instantTime;
    initRecordTypeAndCdcLogger(enginRecordType);
    this.usePosition = false;
    this.props = TypedProperties.copy(config.getProps());
    this.isCompaction = false;
    populateIncomingRecordsMapIterator(recordItr);
  }

  /**
   * Constructor used for Compaction flows.
   * Take in a base path and list of log files, to merge them together to produce a new base file.
   * @param config instance of {@link HoodieWriteConfig} to use.
   * @param instantTime instant time of interest.
   * @param hoodieTable instance of {@link HoodieTable} to use.
   * @param operation compaction operation containing info about base and log files.
   * @param taskContextSupplier instance of {@link TaskContextSupplier} to use.
   * @param readerContext instance of {@link HoodieReaderContext} to use while merging.
   * @param maxInstantTime max instant time to use.
   * @param enginRecordType engine record type.
   */
  public FileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                         CompactionOperation operation, TaskContextSupplier taskContextSupplier,
                                         HoodieReaderContext<T> readerContext, String maxInstantTime,
                                         HoodieRecord.HoodieRecordType enginRecordType) {
    super(config, instantTime, operation.getPartitionPath(), operation.getFileId(), hoodieTable, taskContextSupplier);
    this.maxInstantTime = maxInstantTime;
    this.keyToNewRecords = Collections.emptyMap();
    this.readerContext = readerContext;
    this.operation = Option.of(operation);
    usePosition = config.getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    initRecordTypeAndCdcLogger(enginRecordType);
    init(operation, this.partitionPath);
    this.props = TypedProperties.copy(config.getProps());
    this.isCompaction = true;
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
      LOG.error("Error in update task at commit {}", instantTime, io);
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
   * For COW merge path, lets map the incoming records to another iterator which can be routed to {@link org.apache.hudi.common.table.read.buffer.StreamingFileGroupRecordBufferLoader}.
   * @param newRecordsItr
   */
  private void populateIncomingRecordsMapIterator(Iterator<HoodieRecord<T>> newRecordsItr) {
    String[] orderingFields = readerContext.getMergeMode() == RecordMergeMode.COMMIT_TIME_ORDERING
        ? new String[]{}
        : Option.ofNullable(ConfigUtils.getOrderingFields(props)).orElse(hoodieTable.getConfig().getPreCombineFields().toArray(new String[0]));

    if (!isCompaction) {
      // avoid populating external spillable in base {@link HoodieWriteMergeHandle)
      this.incomingRecordsItr = new MappingIterator(newRecordsItr, record -> {
        try {
          Option<HoodieAvroIndexedRecord> indexedRecordOpt = ((HoodieRecord<T>) record).toIndexedRecord(readerContext.getSchemaHandler().getTableSchema(), props);
          BufferedRecord<T> bufferedRecord;
          if (indexedRecordOpt.isPresent()) {
            bufferedRecord = BufferedRecord.forRecordWithContext((HoodieRecord<T>) indexedRecordOpt.get(), readerContext.getSchemaHandler().getTableSchema(),
                readerContext.getRecordContext(), props, orderingFields);
          } else {
           bufferedRecord = BufferedRecord.forDeleteRecord(((HoodieRecord<?>) record).getRecordKey(),
                ((HoodieRecord<?>) record).getOrderingValue(readerContext.getSchemaHandler().getTableSchema(), props, orderingFields));
          }
          return bufferedRecord;
        } catch (IOException e) {
          throw new HoodieIOException("Failed to convert to Indexed record ", e);
        }
      });
    }
  }

  /**
   * Reads the file slice of a compaction operation using a file group reader,
   * by getting an iterator of the records; then writes the records to a new base file.
   */
  @Override
  public void doMerge() {
    boolean usePosition = config.getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    Option<InternalSchema> internalSchemaOption = SerDeHelper.fromJson(config.getInternalSchema());
    long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(taskContextSupplier, config);
    props.put(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(maxMemoryPerCompaction));
    Option<Stream<HoodieLogFile>> logFilesStreamOpt = operation.map(op -> op.getDeltaFileNames().stream().map(logFileName ->
        new HoodieLogFile(new StoragePath(FSUtils.constructAbsolutePath(
            config.getBasePath(), op.getPartitionPath()), logFileName))));
    // Initializes file group reader
    try (HoodieFileGroupReader<T> fileGroupReader = getFileGroupReader(usePosition, internalSchemaOption, props, isCompaction, logFilesStreamOpt, incomingRecordsItr)) {
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
          // Writes the record
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

  private HoodieFileGroupReader getFileGroupReader(boolean usePosition, Option<InternalSchema> internalSchemaOption, TypedProperties props, boolean isCompaction, Option<Stream<HoodieLogFile>> logFileStreamOpt,
                                                   Iterator<BufferedRecord<T>> incomingRecordsItr) {
    HoodieFileGroupReader.Builder fileGroupBuilder = HoodieFileGroupReader.<T>newBuilder().withReaderContext(readerContext).withHoodieTableMetaClient(hoodieTable.getMetaClient())
        .withLatestCommitTime(maxInstantTime).withPartitionPath(partitionPath).withBaseFileOption(Option.ofNullable(baseFileToMerge))
        .withDataSchema(isCompaction ? writeSchemaWithMetaFields: writeSchema).withRequestedSchema(isCompaction ? writeSchemaWithMetaFields : writeSchema)
        .withInternalSchema(internalSchemaOption).withProps(props)
        .withShouldUseRecordPosition(usePosition).withSortOutput(hoodieTable.requireSortedRecords())
        .withFileGroupUpdateCallback(cdcLogger.map(logger -> new CDCCallback(logger, readerContext)));

    if (isCompaction) {
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
      if (isCompaction) {
        writeStatus.getStat().setTotalLogSizeCompacted(operation.get().getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());
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

    private GenericRecord convertOutput(T record) {
      T convertedRecord = outputConverter.get().map(converter -> record == null ? null : converter.apply(record)).orElse(record);
      return convertedRecord == null ? null : readerContext.getRecordContext().convertToAvroRecord(convertedRecord, requestedSchema.get());
    }
  }
}
