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

package org.apache.hudi.io.v2;

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieAppendException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.io.MiniBatchHandle;
import org.apache.hudi.io.log.block.HoodieFlinkParquetDataBlock;
import org.apache.hudi.io.storage.ColumnRangeMetadataProvider;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED;
import static org.apache.hudi.common.model.HoodieRecordLocation.INVALID_POSITION;

/**
 * A write handle that supports creating a log file and writing records based on record Iterator.
 * The differences from {@code FlinkAppendHandle} are:
 *
 * <p> 1. {@code RowDataLogHandle} does not convert RowData into Avro record before writing.
 * <p> 2. {@code RowDataLogHandle} writes Parquet data block by default.
 * <p> 3. {@code RowDataLogHandle} does not buffer data internally, instead, it employs
 *        record iterator to write data blocks, thereby enhancing memory efficiency.
 *
 * <p>The back-up writer may roll over to a new log file if there already exists a log file for the
 * given file group and instant.
 */
public class RowDataLogWriteHandle<T, I, K, O> extends HoodieWriteHandle<T, I, K, O> implements MiniBatchHandle {
  private static final Logger LOG = LoggerFactory.getLogger(RowDataLogWriteHandle.class);

  private HoodieLogFormat.Writer writer;
  // Header metadata for a log block
  protected final Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
  protected final Schema logWriteSchema;
  // Total number of records written during appending
  protected long recordsWritten = 0;
  // Total number of records deleted during appending
  protected long recordsDeleted = 0;
  // Total number of records updated during appending
  protected long updatedRecordsWritten = 0;
  // Total number of new records inserted into the delta file
  protected long insertRecordsWritten = 0;
  private boolean isClosed = false;

  private static final AtomicLong RECORD_COUNTER = new AtomicLong(1);

  public RowDataLogWriteHandle(
      HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, I, K, O> hoodieTable,
      String fileId,
      String partitionPath,
      TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier);
    initWriteConf(storage.getConf(), config);
    this.writer = createLogWriter(this.instantTime, null);
    this.logWriteSchema = AvroSchemaCache.intern(
        HoodieAvroUtils.addMetadataFields(writeSchema, config.populateMetaFields(), config.allowOperationMetadataField()));
  }

  /**
   * Append data and delete blocks into log file.
   */
  public WriteStatus appendRowData(Iterator<HoodieRecord> records) {
    initPartitionMeta();
    initWriteStatus();
    try {
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, instantTime);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, logWriteSchema.toString());
      List<HoodieLogBlock> blocks = new ArrayList<>(2);
      List<Pair<DeleteRecord, Long>> recordsToDelete = new ArrayList<>();
      // todo do not need list if there is no delete records, HUDI-9195
      List<HoodieRecord> recordList = new ArrayList<>();

      boolean needPrependMetaFields = config.populateMetaFields() || config.allowOperationMetadataField();
      while (records.hasNext()) {
        HoodieRecord record = records.next();
        if (HoodieOperation.isDelete(record.getOperation()) && !config.allowOperationMetadataField()) {
          DeleteRecord deleteRecord = DeleteRecord.create(record.getKey(), record.getOrderingValue(writeSchema, config.getProps()));
          recordsToDelete.add(Pair.of(deleteRecord, INVALID_POSITION));
        } else {
          record = needPrependMetaFields
              ? record.prependMetaFields(writeSchema, logWriteSchema, populateMetadataFields(record), config.getProps())
              : record;
          recordList.add(record);
        }
      }

      // add data block
      HoodieLogBlock dataBlock = null;
      if (!recordList.isEmpty()) {
        String keyField = config.populateMetaFields()
            ? HoodieRecord.RECORD_KEY_METADATA_FIELD
            : hoodieTable.getMetaClient().getTableConfig().getRecordKeyFieldProp();
        dataBlock = genDataBlock(config, getLogDataBlockFormat(), recordList, keyField, header);
        blocks.add(dataBlock);
      }

      // add delete block
      if (!recordsToDelete.isEmpty()) {
        blocks.add(new HoodieDeleteBlock(recordsToDelete, header));
      }

      if (!blocks.isEmpty()) {
        AppendResult appendResult = writer.appendBlocks(blocks);
        processAppendResult(appendResult, dataBlock);
      }
    } catch (Exception e) {
      throw new HoodieAppendException("Failed while appending records to " + writer.getLogFile().getPath(), e);
    }
    return writeStatus;
  }

  public void initWriteStatus() {
    HoodieDeltaWriteStat deltaWriteStat = new HoodieDeltaWriteStat();
    deltaWriteStat.setPartitionPath(partitionPath);
    deltaWriteStat.setFileId(fileId);
    this.writeStatus.setFileId(fileId);
    this.writeStatus.setPartitionPath(partitionPath);
    this.writeStatus.setStat(deltaWriteStat);
  }

  /**
   * Save hoodie partition meta in the partition path.
   */
  protected void initPartitionMeta() {
    HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(storage, instantTime,
        new StoragePath(config.getBasePath()),
        FSUtils.constructAbsolutePath(config.getBasePath(), partitionPath),
        hoodieTable.getPartitionMetafileFormat());
    partitionMetadata.trySave();
  }

  private void initWriteConf(StorageConfiguration<?> storageConf, HoodieWriteConfig writeConfig) {
    storageConf.set(
        HoodieStorageConfig.PARQUET_WRITE_UTC_TIMEZONE.key(),
        writeConfig.getString(HoodieStorageConfig.PARQUET_WRITE_UTC_TIMEZONE.key()));
  }

  private MetadataValues populateMetadataFields(HoodieRecord<T> hoodieRecord) {
    MetadataValues metadataValues = new MetadataValues();
    if (config.populateMetaFields()) {
      String seqId =
          HoodieRecord.generateSequenceId(instantTime, getPartitionId(), RECORD_COUNTER.getAndIncrement());
      metadataValues.setFileName(fileId);
      metadataValues.setPartitionPath(partitionPath);
      metadataValues.setRecordKey(hoodieRecord.getRecordKey());
      metadataValues.setCommitTime(instantTime);
      metadataValues.setCommitSeqno(seqId);
    }
    if (config.allowOperationMetadataField()) {
      metadataValues.setOperation(hoodieRecord.getOperation().getName());
    }
    return metadataValues;
  }

  private void processAppendResult(AppendResult result, HoodieLogBlock dataBlock) {
    HoodieDeltaWriteStat stat = (HoodieDeltaWriteStat) this.writeStatus.getStat();

    Preconditions.checkArgument(stat.getPath() == null, "Only one append are expected for " + this.getClass().getSimpleName());
    // first time writing to this log block.
    updateWriteStatus(stat, result);

    // for parquet data block, we can get column stats from parquet footer directly.
    if (config.isMetadataColumnStatsIndexEnabled()) {
      Set<String> columnsToIndexSet = new HashSet<>(HoodieTableMetadataUtil
          .getColumnsToIndex(hoodieTable.getMetaClient().getTableConfig(),
              config.getMetadataConfig(), Lazy.eagerly(Option.of(writeSchemaWithMetaFields)),
              Option.of(HoodieRecord.HoodieRecordType.FLINK)).keySet());

      Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMetadata;
      if (dataBlock == null) {
        // only delete block exists
        columnRangeMetadata = new HashMap<>();
        for (String col: columnsToIndexSet) {
          columnRangeMetadata.put(col, HoodieColumnRangeMetadata.create(
              stat.getPath(), col, null, null, 0L, 0L, 0L, 0L));
        }
      } else {
        ValidationUtils.checkArgument(dataBlock instanceof ColumnRangeMetadataProvider,
            "Log block for Flink ingestion should always be an instance of ColumnRangeMetadataProvider for collecting column stats efficiently.");
        columnRangeMetadata =
            ((ColumnRangeMetadataProvider) dataBlock).getColumnRangeMeta().entrySet().stream()
                .filter(e -> columnsToIndexSet.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().copy(stat.getPath())));
      }
      stat.putRecordsStats(columnRangeMetadata);
    }
    resetWriteCounts();
    assert stat.getRuntimeStats() != null;
    LOG.info("WriteHandle for partitionPath {} filePath {}, took {} ms.",
        partitionPath, stat.getPath(), stat.getRuntimeStats().getTotalUpsertTime());
  }

  private void updateWriteStatus(HoodieDeltaWriteStat stat, AppendResult result) {
    // update WriteStat
    stat.setPath(makeFilePath(result.logFile()));
    stat.setLogOffset(result.offset());
    stat.setLogVersion(result.logFile().getLogVersion());
    if (!stat.getLogFiles().contains(result.logFile().getFileName())) {
      stat.addLogFiles(result.logFile().getFileName());
    }
    stat.setFileSizeInBytes(result.size());

    // update write counts
    stat.setNumWrites(recordsWritten);
    stat.setNumUpdateWrites(updatedRecordsWritten);
    stat.setNumInserts(insertRecordsWritten);
    stat.setNumDeletes(recordsDeleted);
    stat.setTotalWriteBytes(result.size());

    // update runtime stats
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalUpsertTime(timer.endTimer());
    stat.setRuntimeStats(runtimeStats);
  }

  private String makeFilePath(HoodieLogFile logFile) {
    return partitionPath.isEmpty()
        ? new StoragePath(logFile.getFileName()).toString()
        : new StoragePath(partitionPath, logFile.getFileName()).toString();
  }

  private void resetWriteCounts() {
    recordsWritten = 0;
    updatedRecordsWritten = 0;
    insertRecordsWritten = 0;
    recordsDeleted = 0;
  }

  private HoodieLogBlockType getLogDataBlockFormat() {
    Option<HoodieLogBlockType> logBlockTypeOpt = config.getLogDataBlockFormat();
    if (logBlockTypeOpt.isPresent()) {
      return logBlockTypeOpt.get();
    }
    return HoodieLogBlock.HoodieLogBlockType.PARQUET_DATA_BLOCK;
  }

  @Override
  public IOType getIOType() {
    return IOType.CREATE;
  }

  /**
   * Build a delete log block based on the delete record iterator, and block header.
   *
   * @param deleteRecordIterator delete record iterator used to build the delete block
   * @param header header for the delete block
   * @return delete block
   */
  private HoodieLogBlock genDeleteBlock(
      Iterator<DeleteRecord> deleteRecordIterator,
      Map<HoodieLogBlock.HeaderMetadataType, String> header) {
    List<Pair<DeleteRecord, Long>> recordsToDelete = new ArrayList<>();
    while (deleteRecordIterator.hasNext()) {
      recordsToDelete.add(Pair.of(deleteRecordIterator.next(), INVALID_POSITION));
    }
    return new HoodieDeleteBlock(recordsToDelete, header);
  }

  /**
   * Build a data block based on the hoodie record iterator, and block header.
   *
   * @param writeConfig hoodie write config
   * @param logDataBlockFormat type of the data block
   * @param records hoodie record list used to build the data block
   * @param keyField name of key field
   * @param header header for the data block
   * @return data block
   */
  private HoodieLogBlock genDataBlock(
      HoodieWriteConfig writeConfig,
      HoodieLogBlockType logDataBlockFormat,
      List<HoodieRecord> records,
      String keyField,
      Map<HoodieLogBlock.HeaderMetadataType, String> header) {
    switch (logDataBlockFormat) {
      case PARQUET_DATA_BLOCK:
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put(PARQUET_COMPRESSION_CODEC_NAME.key(), writeConfig.getParquetCompressionCodec());
        paramsMap.put(PARQUET_COMPRESSION_RATIO_FRACTION.key(), String.valueOf(writeConfig.getParquetCompressionRatio()));
        paramsMap.put(PARQUET_DICTIONARY_ENABLED.key(), String.valueOf(writeConfig.parquetDictionaryEnabled()));
        return new HoodieFlinkParquetDataBlock(
            records,
            header,
            keyField,
            writeConfig.getParquetCompressionCodec(),
            writeConfig.getParquetCompressionRatio(),
            writeConfig.parquetDictionaryEnabled());
      default:
        throw new HoodieException("Data block format " + logDataBlockFormat + " is not implemented for Flink RowData append handle.");
    }
  }

  @Override
  public List<WriteStatus> close() {
    return Collections.singletonList(writeStatus);
  }

  @Override
  public void closeGracefully() {
    if (isClosed) {
      return;
    }
    try {
      if (writer != null) {
        writer.close();
        writer = null;
      }
    } catch (Throwable throwable) {
      LOG.warn("Error while trying to close the append handle", throwable);
    }
    isClosed = true;
  }

  @Override
  public StoragePath getWritePath() {
    return null;
  }
}
