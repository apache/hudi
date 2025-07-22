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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

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
  private final FileSlice fileSlice;
  private final CompactionOperation operation;
  private final String maxInstantTime;
  private HoodieReadStats readStats;
  private final HoodieRecord.HoodieRecordType recordType;
  private final Option<HoodieCDCLogger> cdcLogger;

  public FileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                         FileSlice fileSlice, CompactionOperation operation, TaskContextSupplier taskContextSupplier,
                                         HoodieReaderContext<T> readerContext, String maxInstantTime,
                                         HoodieRecord.HoodieRecordType enginRecordType) {
    super(config, instantTime, operation.getPartitionPath(), operation.getFileId(), hoodieTable, taskContextSupplier);
    this.maxInstantTime = maxInstantTime;
    this.keyToNewRecords = Collections.emptyMap();
    this.readerContext = readerContext;
    this.fileSlice = fileSlice;
    this.operation = operation;
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
    init(operation, this.partitionPath, fileSlice.getBaseFile());
  }

  private void init(CompactionOperation operation, String partitionPath, Option<HoodieBaseFile> baseFileToMerge) {
    LOG.info("partitionPath:{}, fileId to be merged:{}", partitionPath, fileId);
    this.baseFileToMerge = baseFileToMerge.orElse(null);
    this.writtenRecordKeys = new HashSet<>();
    writeStatus.setStat(new HoodieWriteStat());
    writeStatus.getStat().setTotalLogSizeCompacted(
        operation.getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());
    try {
      Option<String> latestValidFilePath = Option.empty();
      if (baseFileToMerge.isPresent()) {
        latestValidFilePath = Option.of(baseFileToMerge.get().getFileName());
        writeStatus.getStat().setPrevCommit(baseFileToMerge.get().getCommitTime());
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

  /**
   * Reads the file slice of a compaction operation using a file group reader,
   * by getting an iterator of the records; then writes the records to a new base file.
   */
  @Override
  public void doMerge() {
    boolean usePosition = config.getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    Option<InternalSchema> internalSchemaOption = SerDeHelper.fromJson(config.getInternalSchema());
    TypedProperties props = TypedProperties.copy(config.getProps());
    long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(taskContextSupplier, config);
    props.put(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(maxMemoryPerCompaction));
    // Initializes file group reader
    try (HoodieFileGroupReader<T> fileGroupReader = HoodieFileGroupReader.<T>newBuilder().withReaderContext(readerContext).withHoodieTableMetaClient(hoodieTable.getMetaClient())
        .withLatestCommitTime(maxInstantTime).withFileSlice(fileSlice).withDataSchema(writeSchemaWithMetaFields).withRequestedSchema(writeSchemaWithMetaFields)
        .withInternalSchema(internalSchemaOption).withProps(props).withShouldUseRecordPosition(usePosition).withSortOutput(hoodieTable.requireSortedRecords())
        .withFileGroupUpdateCallback(cdcLogger.map(logger -> new CDCCallback(logger, readerContext))).build()) {
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
        this.recordsWritten = readStats.getNumInserts() + readStats.getNumUpdates();
      }
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to compact file slice: " + fileSlice, e);
    }
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
      writeStatus.getStat().setTotalLogSizeCompacted(operation.getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());

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
      return convertedRecord == null ? null : readerContext.convertToAvroRecord(convertedRecord, requestedSchema.get());
    }
  }
}
