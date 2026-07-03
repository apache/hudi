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

package org.apache.hudi.io;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieHFileDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieParquetDataBlock;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieAppendException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.CommonClientUtils;
import org.apache.hudi.util.Lazy;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_DYNAMIC_MAX_ENTRIES;
import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_FPP_VALUE;
import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_NUM_ENTRIES_VALUE;
import static org.apache.hudi.common.config.HoodieStorageConfig.BLOOM_FILTER_TYPE;
import static org.apache.hudi.common.config.HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM_NAME;
import static org.apache.hudi.common.config.HoodieStorageConfig.HFILE_WITH_BLOOM_FILTER_ENABLED;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.collectColumnRangeMetadata;

/**
 * IO Operation to append data onto an existing file.
 */
@Slf4j
public class HoodieInlineLogAppendHandle<T, I, K, O> extends HoodieAppendHandle<T, I, K, O> {
  private static final int NUMBER_OF_RECORDS_TO_ESTIMATE_RECORD_SIZE = 100;

  // Buffer for holding records in memory before they are flushed to disk
  protected final List<HoodieRecord> recordList = new ArrayList<>();
  // Buffer for holding records (to be deleted), along with their position in log block, in memory before they are flushed to disk
  protected final List<Pair<DeleteRecord, Long>> recordsToDeleteWithPositions = new ArrayList<>();
  // Writer to log into the file group's latest slice.
  protected HoodieLogFormat.Writer writer;

  // Average record size for a HoodieRecord. This size is updated at the end of every log block flushed to disk
  private long averageRecordSize = 0;
  // Total number of bytes written during this append phase (an estimation)
  protected long estimatedNumberOfBytesWritten;
  // Number of records that must be written to meet the max block size for a log block
  private long numberOfRecords = 0;
  // Max block size to limit to for a log block
  private final long maxBlockSize = config.getLogFileDataBlockMaxSize();
  private final SizeEstimator<HoodieRecord> sizeEstimator;

  private final String[] orderingFields;

  /**
   * This is used by log compaction only.
   */
  public HoodieInlineLogAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr,
                            TaskContextSupplier taskContextSupplier, Map<HeaderMetadataType, String> header) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier, true);
    this.useWriterSchema = true;
    this.isLogCompaction = true;
    this.header.putAll(header);
  }

  public HoodieInlineLogAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr, TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier, false);
  }

  private HoodieInlineLogAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                             String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr, TaskContextSupplier taskContextSupplier, boolean preserveMetadata) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier, preserveMetadata);
    this.sizeEstimator = getSizeEstimator();
    this.orderingFields = ConfigUtils.getOrderingFields(recordProperties);
  }

  public HoodieInlineLogAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, TaskContextSupplier sparkTaskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, null, sparkTaskContextSupplier);
  }

  protected SizeEstimator<HoodieRecord> getSizeEstimator() {
    return new DefaultSizeEstimator<>();
  }

  @Override
  protected void createLogWriterForAppend(String instantTime, Option<FileSlice> fileSliceOpt) {
    this.writer = createLogWriter(instantTime, fileSliceOpt);
  }

  @Override
  protected void init(HoodieRecord record) {
    boolean shouldInit = doInit;
    super.init(record);
    if (shouldInit) {
      averageRecordSize = sizeEstimator.sizeEstimate(record);
    }
  }

  @Override
  protected boolean writeRecord(HoodieRecord<T> hoodieRecord) {
    init(hoodieRecord);
    flushToDiskIfRequired(hoodieRecord, false);
    boolean result = super.writeRecord(hoodieRecord);
    if (result) {
      numberOfRecords++;
    }
    return result;
  }

  @Override
  protected void flushAppend() {
    appendDataAndDeleteBlocks(header, true);
    estimatedNumberOfBytesWritten += averageRecordSize * numberOfRecords;
    numberOfRecords = 0;
  }

  @Override
  protected void collectColumnStats(HoodieDeltaWriteStat stat) {
    if (config.isMetadataColumnStatsIndexEnabled()) {
      HoodieIndexVersion indexVersion = HoodieTableMetadataUtil.existingIndexVersionOrDefault(PARTITION_NAME_COLUMN_STATS, hoodieTable.getMetaClient());
      Set<String> columnsToIndexSet = new HashSet<>(HoodieTableMetadataUtil
          .getColumnsToIndex(hoodieTable.getMetaClient().getTableConfig(),
              config.getMetadataConfig(), Lazy.eagerly(Option.of(writeSchemaWithMetaFields)),
              Option.of(this.recordMerger.getRecordType()), indexVersion).keySet());
      final List<Pair<String, HoodieSchemaField>> fieldsToIndex = columnsToIndexSet.stream()
          .map(fieldName -> HoodieSchemaUtils.getNestedField(writeSchemaWithMetaFields, fieldName))
          .filter(Option::isPresent)
          .map(Option::get)
          .collect(Collectors.toList());
      try {
        Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataMap =
            collectColumnRangeMetadata(recordList.iterator(), fieldsToIndex, stat.getPath(), writeSchemaWithMetaFields, storage.getConf(),
                indexVersion);
        stat.putRecordsStats(columnRangeMetadataMap);
      } catch (HoodieException e) {
        throw new HoodieAppendException("Failed to extract append result", e);
      }
    }
  }

  /**
   * Appends data and delete blocks. When appendDeleteBlocks value is false, only data blocks are appended.
   * This is done so that all the data blocks are created first and then a single delete block is added.
   * Otherwise, what can end up happening is creation of multiple small delete blocks get added after each data block.
   */
  protected void appendDataAndDeleteBlocks(Map<HeaderMetadataType, String> header, boolean appendDeleteBlocks) {
    try {
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, instantTime);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, writeSchemaWithMetaFields.toString());
      List<HoodieLogBlock> blocks = new ArrayList<>(2);
      HoodieLogBlock dataBlock = null;
      if (!recordList.isEmpty()) {
        String keyField = config.populateMetaFields()
            ? HoodieRecord.RECORD_KEY_METADATA_FIELD
            : hoodieTable.getMetaClient().getTableConfig().getRecordKeyFieldProp();

        dataBlock = getDataBlock(config, getLogBlockType(), recordList,
            getUpdatedHeader(header), keyField);
        blocks.add(dataBlock);
      }

      if (appendDeleteBlocks && !recordsToDeleteWithPositions.isEmpty()) {
        blocks.add(new HoodieDeleteBlock(
            recordsToDeleteWithPositions,
            getUpdatedHeader(header)));
      }

      if (!blocks.isEmpty()) {
        AppendResult appendResult = writer.appendBlocks(blocks);
        appendDataBlock = dataBlock == null ? Option.empty() : Option.of(dataBlock);
        try {
          processAppendResult(appendResult);
        } finally {
          appendDataBlock = Option.empty();
        }
        recordList.clear();
        if (appendDeleteBlocks) {
          recordsToDeleteWithPositions.clear();
        }
      }
    } catch (Exception e) {
      throw new HoodieAppendException("Failed while appending records to " + writer.getLogFile().getPath(), e);
    }
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return config.getParquetMaxFileSize() >= estimatedNumberOfBytesWritten
        * config.getLogFileToParquetCompressionRatio();
  }

  @Override
  protected void closeLogWriter() throws IOException {
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }

  @Override
  protected void writeInsertAndUpdate(HoodieSchema schema, HoodieRecord<T> hoodieRecord, boolean isUpdateRecord) throws IOException {
    // Check if the record should be ignored (special case for [[ExpressionPayload]])
    if (hoodieRecord.shouldIgnore(schema, recordProperties)) {
      return;
    }

    // Prepend meta-fields into the record
    MetadataValues metadataValues = populateMetadataFields(hoodieRecord);
    HoodieRecord populatedRecord =
        hoodieRecord.prependMetaFields(schema, writeSchemaWithMetaFields, metadataValues, recordProperties);

    // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
    //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
    //       it since these records will be put into the recordList(List).
    recordList.add(populatedRecord.copy());
    if (isUpdateRecord || isLogCompaction) {
      updatedRecordsWritten++;
    } else {
      insertRecordsWritten++;
    }
    recordsWritten++;
  }

  @Override
  protected void writeDelete(HoodieSchema schema, HoodieRecord<T> hoodieRecord) {
    // Clear the new location as the record was deleted
    hoodieRecord.unseal();
    hoodieRecord.clearNewLocation();
    hoodieRecord.seal();
    recordsDeleted++;

    // store ordering value with Java type.
    final Comparable<?> orderingVal = hoodieRecord.getOrderingValueAsJava(writeSchema, recordProperties, orderingFields);
    long position = baseFileInstantTimeOfPositions.isPresent() ? hoodieRecord.getCurrentPosition() : -1L;
    recordsToDeleteWithPositions.add(Pair.of(DeleteRecord.create(hoodieRecord.getKey(), orderingVal), position));
  }

  /**
   * Checks if the number of records have reached the set threshold and then flushes the records to disk.
   */
  protected void flushToDiskIfRequired(HoodieRecord record, boolean appendDeleteBlocks) {
    if (numberOfRecords >= (int) (maxBlockSize / averageRecordSize)
        || numberOfRecords % NUMBER_OF_RECORDS_TO_ESTIMATE_RECORD_SIZE == 0) {
      averageRecordSize = (long) (averageRecordSize * 0.8 + sizeEstimator.sizeEstimate(record) * 0.2);
    }

    // Append if max number of records reached to achieve block size
    if (numberOfRecords >= (maxBlockSize / averageRecordSize)) {
      // Recompute averageRecordSize before writing a new block and update existing value with
      // avg of new and old
      log.info("Flush log block to disk, the current avgRecordSize => {}", averageRecordSize);
      // Delete blocks will be appended after appending all the data blocks.
      appendDataAndDeleteBlocks(header, appendDeleteBlocks);
      estimatedNumberOfBytesWritten += averageRecordSize * numberOfRecords;
      numberOfRecords = 0;
    }
  }

  protected HoodieLogBlock.HoodieLogBlockType getLogBlockType() {
    return CommonClientUtils.getLogBlockType(config, hoodieTable.getMetaClient().getTableConfig());
  }

  protected HoodieLogBlock getDataBlock(HoodieWriteConfig writeConfig,
                                        HoodieLogBlock.HoodieLogBlockType logDataBlockFormat,
                                        List<HoodieRecord> records,
                                        Map<HeaderMetadataType, String> header,
                                        String keyField) {
    switch (logDataBlockFormat) {
      case AVRO_DATA_BLOCK:
        return new HoodieAvroDataBlock(records, header, keyField);
      case HFILE_DATA_BLOCK:
        // Not supporting positions in HFile data blocks
        header.remove(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS);
        records.sort(Comparator.comparing(HoodieRecord::getRecordKey));
        Map<String, String> hfileParams = new HashMap<>();
        hfileParams.put(HFILE_COMPRESSION_ALGORITHM_NAME.key(), writeConfig.getHFileCompressionAlgorithm());
        hfileParams.put(HFILE_WITH_BLOOM_FILTER_ENABLED.key(), Boolean.toString(writeConfig.hfileBloomFilterEnabled()));
        hfileParams.put(BLOOM_FILTER_NUM_ENTRIES_VALUE.key(), Integer.toString(writeConfig.getBloomFilterNumEntries()));
        hfileParams.put(BLOOM_FILTER_FPP_VALUE.key(), Double.toString(writeConfig.getBloomFilterFPP()));
        hfileParams.put(BLOOM_FILTER_DYNAMIC_MAX_ENTRIES.key(), Integer.toString(writeConfig.getDynamicBloomFilterMaxNumEntries()));
        hfileParams.put(BLOOM_FILTER_TYPE.key(), writeConfig.getBloomFilterType());
        return new HoodieHFileDataBlock(
            records, header, writeConfig.getHFileCompressionAlgorithm(), hfileParams, new StoragePath(writeConfig.getBasePath()));
      case PARQUET_DATA_BLOCK:
        return new HoodieParquetDataBlock(
            records,
            header,
            keyField,
            writeConfig.getParquetCompressionCodec(),
            writeConfig.getParquetCompressionRatio(),
            writeConfig.parquetDictionaryEnabled());
      default:
        throw new HoodieException("Data block format " + logDataBlockFormat + " not implemented");
    }
  }

}
