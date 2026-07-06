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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieAppendException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.cdc.HoodieNativeLogFormatWriter;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.CommonClientUtils;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;

/**
 * Append handle for native log files. Unlike {@link HoodieInlineLogAppendHandle}, this handle streams
 * records directly into native format writers and does not buffer records or build inline log blocks.
 */
public class HoodieNativeLogAppendHandle<T, I, K, O> extends HoodieAppendHandle<T, I, K, O> {

  private HoodieNativeLogFormatWriter writer;

  public HoodieNativeLogAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                     String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr,
                                     TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier, false, Collections.emptyMap());
  }

  public HoodieNativeLogAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                     String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr,
                                     TaskContextSupplier taskContextSupplier, Map<HeaderMetadataType, String> header) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier, true, header);
  }

  public HoodieNativeLogAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                     String partitionPath, String fileId, TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, null, taskContextSupplier);
  }

  private HoodieNativeLogAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                      String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr,
                                      TaskContextSupplier taskContextSupplier, boolean preserveMetadata,
                                      Map<HeaderMetadataType, String> header) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier, preserveMetadata);
    CommonClientUtils.validateIndexSupportForNativeLogFormat(config, hoodieTable.getBaseFileFormat());
    this.header.putAll(header);
  }

  @Override
  protected void createLogWriterForAppend(String instantTime, Option<FileSlice> fileSliceOpt) {
    try {
      List<String> orderingFields = HoodieRecordUtils.getOrderingFieldNames(
          hoodieTable.getMetaClient().getTableConfig().getRecordMergeMode(), hoodieTable.getMetaClient());
      this.writer = new HoodieNativeLogFormatWriter(
          storage.getDefaultBufferSize(),
          storage,
          FSUtils.constructAbsolutePath(hoodieTable.getMetaClient().getBasePath(), partitionPath),
          fileId,
          instantTime,
          null,
          writeToken,
          config.getLogFileMaxSize(),
          getLogCreationCallback(),
          config.getWriteVersion(),
          config,
          hoodieTable.getBaseFileFormat(),
          writeSchemaWithMetaFields,
          taskContextSupplier,
          hoodieTable.getReaderContextFactoryForWrite().getContext().getRecordContext(),
          orderingFields,
          baseFileInstantTimeOfPositions);
    } catch (IOException e) {
      throw new HoodieException("Creating native log writer with fileId: " + fileId + ", "
          + "delta commit time: " + instantTime + " error", e);
    }
  }

  @Override
  protected void writeInsertAndUpdate(HoodieSchema schema, HoodieRecord<T> hoodieRecord, boolean isUpdateRecord) throws IOException {
    if (hoodieRecord.shouldIgnore(schema, recordProperties)) {
      return;
    }
    HoodieRecord populatedRecord = hoodieRecord.prependMetaFields(
        schema, writeSchemaWithMetaFields, populateMetadataFields(hoodieRecord), recordProperties);
    String keyField = config.populateMetaFields()
        ? HoodieRecord.RECORD_KEY_METADATA_FIELD
        : hoodieTable.getMetaClient().getTableConfig().getRecordKeyFieldProp();
    if (!writer.canWriteDataFile()) {
      flushAppend();
    }
    writer.appendRecord(populatedRecord, writeSchemaWithMetaFields, keyField);
    if (isUpdateRecord || isLogCompaction) {
      updatedRecordsWritten++;
    } else {
      insertRecordsWritten++;
    }
    recordsWritten++;
  }

  @Override
  protected void writeDelete(HoodieSchema schema, HoodieRecord<T> hoodieRecord) throws IOException {
    hoodieRecord.unseal();
    hoodieRecord.clearNewLocation();
    hoodieRecord.seal();
    String keyField = schema.getField(HoodieRecord.RECORD_KEY_METADATA_FIELD).isPresent()
        ? HoodieRecord.RECORD_KEY_METADATA_FIELD
        : hoodieTable.getMetaClient().getTableConfig().getRecordKeyFieldProp();
    if (!writer.canWriteDeleteFile()) {
      flushAppend();
    }
    writer.appendDeleteRecord(hoodieRecord, schema, keyField);
    recordsDeleted++;
  }

  @Override
  protected void flushAppend() {
    try {
      header.put(HeaderMetadataType.INSTANT_TIME, instantTime);
      header.put(HeaderMetadataType.SCHEMA, writeSchemaWithMetaFields.toString());
      if (writer != null && writer.hasPendingWrites()) {
        writer.flushAppend(getUpdatedHeader(header));
        processAppendResults(writer.getLastAppendResults());
      }
    } catch (IOException e) {
      throw new HoodieAppendException("Failed while flushing records to native log for fileId " + fileId, e);
    }
  }

  @Override
  protected void closeLogWriter() {
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }

  /**
   * Finalizes accounting for all physical native log files produced by one flush.
   *
   * <p>The native writer returns append results in file-write order: data log first and delete log second when both
   * files are produced. Delete-only flushes contain a single delete result at index 0.
   */
  private void processAppendResults(List<AppendResult> results) {
    long elapsedTime = timer.endTimer();
    try {
      for (int i = 0; i < results.size(); i++) {
        processAppendResult(results.get(i), i == 0 ? elapsedTime : 0L);
      }
      if (!results.isEmpty()) {
        resetWriteCounts();
      }
    } finally {
      timer.startTimer();
    }
  }

  protected StoragePath getLogFilePath() {
    return writer.getLogFile().getPath();
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return true;
  }

  @Override
  protected void collectColumnStats(HoodieDeltaWriteStat stat) {
    if (!config.isMetadataColumnStatsIndexEnabled()) {
      return;
    }

    HoodieIndexVersion indexVersion = HoodieTableMetadataUtil.existingIndexVersionOrDefault(PARTITION_NAME_COLUMN_STATS, hoodieTable.getMetaClient());
    Set<String> columnsToIndexSet = new HashSet<>(HoodieTableMetadataUtil
        .getColumnsToIndex(hoodieTable.getMetaClient().getTableConfig(),
            config.getMetadataConfig(), Lazy.eagerly(Option.of(writeSchemaWithMetaFields)),
            Option.of(recordMerger.getRecordType()), indexVersion).keySet());

    if (FSUtils.isNativeDeleteLogFile(new StoragePath(stat.getPath()).getName())) {
      // Native delete logs do not contain data values, so there is no min/max range to collect. Still, publishing
      // empty column stats is meaningful for query-side column-stats pruning: it marks the delete log as indexed but
      // empty. Without these records, the query path treats the log as an un-indexed file and keeps the file slice as a
      // fallback candidate, which is not equivalent to the legacy inline-log behavior for delete-only blocks.
      stat.putRecordsStats(columnsToIndexSet.stream()
          .collect(Collectors.toMap(
              column -> column,
              column -> HoodieColumnRangeMetadata.createEmpty(stat.getPath(), column, indexVersion))));
      return;
    }

    Option<Object> dataFileFormatMetadata = writer.getLastDataFileFormatMetadata();
    if (dataFileFormatMetadata.isPresent()) {
      stat.putRecordsStats(collectNativeLogColumnRangeMetadata(
          stat.getPath(), dataFileFormatMetadata.get(), columnsToIndexSet, indexVersion));
    }
  }

  @Override
  protected void updateWriteCounts(HoodieDeltaWriteStat stat, AppendResult result) {
    if (FSUtils.isNativeDeleteLogFile(result.logFile().getFileName())) {
      stat.setNumDeletes(recordsDeleted);
    } else {
      stat.setNumWrites(recordsWritten);
      stat.setNumUpdateWrites(updatedRecordsWritten);
      stat.setNumInserts(insertRecordsWritten);
    }
    stat.setTotalWriteBytes(result.size());
  }

  @Override
  protected void accumulateWriteCounts(HoodieDeltaWriteStat stat, AppendResult result) {
    throw new HoodieAppendException("Native format log append should not accumulate write counts for "
        + result.logFile().getPath());
  }

  private Map<String, HoodieColumnRangeMetadata<Comparable>> collectNativeLogColumnRangeMetadata(
      String filePath,
      Object metadata,
      Set<String> columnsToIndexSet,
      HoodieIndexVersion indexVersion) {
    if (metadata instanceof ParquetMetadata) {
      return new ParquetUtils()
          .readColumnStatsFromMetadata((ParquetMetadata) metadata, filePath, Option.of(new ArrayList<>(columnsToIndexSet)), indexVersion)
          .stream()
          .filter(columnStats -> columnsToIndexSet.contains(columnStats.getColumnName()))
          .collect(Collectors.toMap(HoodieColumnRangeMetadata::getColumnName, columnStats -> columnStats));
    }
    throw new HoodieAppendException("Unsupported native log file metadata type: " + metadata.getClass().getName());
  }
}
