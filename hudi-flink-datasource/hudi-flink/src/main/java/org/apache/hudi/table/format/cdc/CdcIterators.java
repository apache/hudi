/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format.cdc;

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.HoodieCDCLogRecordIterator;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.format.FlinkReaderContextFactory;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.RowDataProjection;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.table.format.FormatUtils.buildAvroRecordBySchema;

/**
 * Shared iterator implementations for CDC record reading, used by both
 * {@link CdcInputFormat} and the Source V2 CDC split reader.
 */
public final class CdcIterators {

  private CdcIterators() {
  }

  // -------------------------------------------------------------------------
  //  Top-level iterator: fans out over an ordered list of CDC file-splits
  // -------------------------------------------------------------------------

  /**
   * Iterates over an ordered sequence of {@link HoodieCDCFileSplit}s, delegating
   * per-split record reading to a user-supplied factory function.
   */
  public static class CdcFileSplitsIterator implements ClosableIterator<RowData> {
    private CdcImageManager imageManager;
    private final Iterator<HoodieCDCFileSplit> fileSplitIterator;
    private final Function<HoodieCDCFileSplit, ClosableIterator<RowData>> recordIteratorFunc;
    private ClosableIterator<RowData> recordIterator;

    public CdcFileSplitsIterator(
        HoodieCDCFileSplit[] changes,
        CdcImageManager imageManager,
        Function<HoodieCDCFileSplit, ClosableIterator<RowData>> recordIteratorFunc) {
      this.fileSplitIterator = Arrays.asList(changes).iterator();
      this.imageManager = imageManager;
      this.recordIteratorFunc = recordIteratorFunc;
    }

    @Override
    public boolean hasNext() {
      if (recordIterator != null) {
        if (recordIterator.hasNext()) {
          return true;
        } else {
          recordIterator.close();
          recordIterator = null;
        }
      }
      if (fileSplitIterator.hasNext()) {
        recordIterator = recordIteratorFunc.apply(fileSplitIterator.next());
        return recordIterator.hasNext();
      }
      return false;
    }

    @Override
    public RowData next() {
      return recordIterator.next();
    }

    @Override
    public void close() {
      if (recordIterator != null) {
        recordIterator.close();
      }
      if (imageManager != null) {
        imageManager.close();
        imageManager = null;
      }
    }
  }

  // -------------------------------------------------------------------------
  //  BASE_FILE_INSERT / BASE_FILE_DELETE
  // -------------------------------------------------------------------------

  /**
   * Wraps a base-file parquet iterator and marks every record as {@link RowKind#INSERT}.
   * Used for the {@code BASE_FILE_INSERT} CDC inference case.
   */
  public static class AddBaseFileIterator implements ClosableIterator<RowData> {
    private ClosableIterator<RowData> nested;
    private RowData currentRecord;

    public AddBaseFileIterator(ClosableIterator<RowData> nested) {
      this.nested = nested;
    }

    @Override
    public boolean hasNext() {
      if (nested.hasNext()) {
        currentRecord = nested.next();
        currentRecord.setRowKind(RowKind.INSERT);
        return true;
      }
      return false;
    }

    @Override
    public RowData next() {
      return currentRecord;
    }

    @Override
    public void close() {
      if (nested != null) {
        nested.close();
        nested = null;
      }
    }
  }

  /**
   * Wraps a file-slice iterator and marks every record as {@link RowKind#DELETE}, applying
   * required-column projection. Used for the {@code BASE_FILE_DELETE} CDC inference case.
   */
  public static class RemoveBaseFileIterator implements ClosableIterator<RowData> {
    private ClosableIterator<RowData> nested;
    private final RowDataProjection projection;

    public RemoveBaseFileIterator(
        RowType requiredRowType,
        int[] requiredPositions,
        ClosableIterator<RowData> iterator) {
      this.nested = iterator;
      this.projection = RowDataProjection.instance(requiredRowType, requiredPositions);
    }

    @Override
    public boolean hasNext() {
      return nested.hasNext();
    }

    @Override
    public RowData next() {
      RowData row = nested.next();
      row.setRowKind(RowKind.DELETE);
      return projection.project(row);
    }

    @Override
    public void close() {
      if (nested != null) {
        nested.close();
        nested = null;
      }
    }
  }

  // -------------------------------------------------------------------------
  //  LOG_FILE
  // -------------------------------------------------------------------------

  /**
   * Handles the {@code LOG_FILE} CDC inference case: compares records from the log file against
   * before-image snapshots to emit INSERT / UPDATE_BEFORE / UPDATE_AFTER / DELETE events.
   */
  public static class DataLogFileIterator implements ClosableIterator<RowData> {
    private final HoodieSchema tableSchema;
    private final long maxCompactionMemoryInBytes;
    private final CdcImageManager imageManager;
    private final RowDataProjection projection;
    private final BufferedRecordMerger recordMerger;
    private final ClosableIterator<HoodieRecord<RowData>> logRecordIterator;
    private final DeleteContext deleteContext;
    private final HoodieReaderContext<RowData> readerContext;
    private final String[] orderingFields;
    private final TypedProperties props;

    private ExternalSpillableMap<String, byte[]> beforeImages;
    private RowData currentImage;
    private RowData sideImage;

    public DataLogFileIterator(
        long maxCompactionMemoryInBytes,
        CdcImageManager imageManager,
        HoodieCDCFileSplit cdcFileSplit,
        HoodieSchema tableSchema,
        RowType requiredRowType,
        int[] requiredPositions,
        ClosableIterator<HoodieRecord<RowData>> logRecordIterator,
        HoodieTableMetaClient metaClient,
        HoodieWriteConfig writeConfig) throws IOException {
      this.tableSchema = tableSchema;
      this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
      this.imageManager = imageManager;
      this.projection = HoodieSchemaConverter.convertToRowType(tableSchema).equals(requiredRowType)
          ? null : RowDataProjection.instance(requiredRowType, requiredPositions);
      this.props = writeConfig.getProps();
      this.readerContext = new FlinkReaderContextFactory(metaClient).getContext();
      readerContext.initRecordMerger(props);
      this.orderingFields = ConfigUtils.getOrderingFields(props);
      this.recordMerger = BufferedRecordMergerFactory.create(
          readerContext,
          readerContext.getMergeMode(),
          false,
          Option.of(writeConfig.getRecordMerger()),
          tableSchema,
          Option.ofNullable(Pair.of(metaClient.getTableConfig().getPayloadClass(), writeConfig.getPayloadClass())),
          props,
          metaClient.getTableConfig().getPartialUpdateMode());
      this.logRecordIterator = logRecordIterator;
      this.deleteContext = new DeleteContext(props, tableSchema).withReaderSchema(tableSchema);
      initImages(cdcFileSplit, writeConfig);
    }

    private void initImages(HoodieCDCFileSplit fileSplit, HoodieWriteConfig writeConfig) throws IOException {
      if (fileSplit.getBeforeFileSlice().isPresent() && !fileSplit.getBeforeFileSlice().get().isEmpty()) {
        this.beforeImages = imageManager.getOrLoadImages(
            maxCompactionMemoryInBytes, fileSplit.getBeforeFileSlice().get());
      } else {
        this.beforeImages = FormatUtils.spillableMap(writeConfig, maxCompactionMemoryInBytes, getClass().getSimpleName());
      }
    }

    @Override
    public boolean hasNext() {
      if (sideImage != null) {
        currentImage = sideImage;
        sideImage = null;
        return true;
      }
      while (logRecordIterator.hasNext()) {
        HoodieRecord<RowData> record = logRecordIterator.next();
        RowData existed = imageManager.removeImageRecord(record.getRecordKey(), beforeImages);
        if (isDelete(record)) {
          if (existed != null) {
            existed.setRowKind(RowKind.DELETE);
            currentImage = existed;
            return true;
          }
        } else {
          if (existed == null) {
            RowData newRow = record.getData();
            newRow.setRowKind(RowKind.INSERT);
            currentImage = newRow;
            return true;
          } else {
            HoodieOperation operation = HoodieOperation.fromValue(existed.getRowKind().toByteValue());
            HoodieRecord<RowData> historyRecord = new HoodieFlinkRecord(record.getKey(), operation, existed);
            HoodieRecord<RowData> merged = mergeRowWithLog(historyRecord, record).get();
            if (merged.getData() != existed) {
              existed.setRowKind(RowKind.UPDATE_BEFORE);
              currentImage = existed;
              RowData mergedRow = merged.getData();
              mergedRow.setRowKind(RowKind.UPDATE_AFTER);
              imageManager.updateImageRecord(record.getRecordKey(), beforeImages, mergedRow);
              sideImage = mergedRow;
              return true;
            }
          }
        }
      }
      return false;
    }

    @Override
    public RowData next() {
      return projection != null ? projection.project(currentImage) : currentImage;
    }

    @Override
    public void close() {
      logRecordIterator.close();
      imageManager.close();
    }

    @SuppressWarnings("unchecked")
    private Option<HoodieRecord<RowData>> mergeRowWithLog(
        HoodieRecord<RowData> historyRecord, HoodieRecord<RowData> newRecord) {
      try {
        BufferedRecord<RowData> histBuf = BufferedRecords.fromHoodieRecord(
            historyRecord, tableSchema, readerContext.getRecordContext(), props, orderingFields, deleteContext);
        BufferedRecord<RowData> newBuf = BufferedRecords.fromHoodieRecord(
            newRecord, tableSchema, readerContext.getRecordContext(), props, orderingFields, deleteContext);
        BufferedRecord<RowData> merged = recordMerger.finalMerge(histBuf, newBuf);
        return Option.ofNullable(readerContext.getRecordContext()
            .constructHoodieRecord(merged, historyRecord.getPartitionPath()));
      } catch (IOException e) {
        throw new HoodieIOException("Merge base and delta payloads exception", e);
      }
    }

    private boolean isDelete(HoodieRecord<RowData> record) {
      return record.isDelete(deleteContext, CollectionUtils.emptyProps());
    }
  }

  // -------------------------------------------------------------------------
  //  AS_IS — supplemental logging modes
  // -------------------------------------------------------------------------

  /**
   * Base iterator for CDC log files stored with supplemental logging (AS_IS inference case).
   * Reads a {@link HoodieCDCLogRecordIterator} and resolves before/after images using
   * subclass-specific logic.
   */
  public abstract static class BaseImageIterator implements ClosableIterator<RowData> {
    private final HoodieSchema requiredSchema;
    private final int[] requiredPos;
    private final GenericRecordBuilder recordBuilder;
    private final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter;
    private HoodieCDCLogRecordIterator cdcItr;

    private GenericRecord cdcRecord;
    private RowData sideImage;
    private RowData currentImage;

    protected BaseImageIterator(
        org.apache.hadoop.conf.Configuration hadoopConf,
        String tablePath,
        HoodieSchema tableSchema,
        HoodieSchema requiredSchema,
        RowType requiredRowType,
        HoodieSchema cdcSchema,
        HoodieCDCFileSplit fileSplit) {
      this.requiredSchema = requiredSchema;
      this.requiredPos = computeRequiredPos(tableSchema, requiredSchema);
      this.recordBuilder = new GenericRecordBuilder(requiredSchema.getAvroSchema());
      this.avroToRowDataConverter = AvroToRowDataConverters.createRowConverter(requiredRowType);

      StoragePath hadoopTablePath = new StoragePath(tablePath);
      HoodieStorage storage = HoodieStorageUtils.getStorage(
          tablePath, HadoopFSUtils.getStorageConf(hadoopConf));
      HoodieLogFile[] cdcLogFiles = fileSplit.getCdcFiles().stream()
          .map(cdcFile -> {
            try {
              return new HoodieLogFile(storage.getPathInfo(new StoragePath(hadoopTablePath, cdcFile)));
            } catch (IOException e) {
              throw new HoodieIOException("Failed to get file status for CDC log: " + cdcFile, e);
            }
          })
          .toArray(HoodieLogFile[]::new);
      this.cdcItr = new HoodieCDCLogRecordIterator(storage, cdcLogFiles, cdcSchema);
    }

    private static int[] computeRequiredPos(HoodieSchema tableSchema, HoodieSchema requiredSchema) {
      HoodieSchema dataSchema = HoodieSchemaUtils.removeMetadataFields(tableSchema);
      List<String> fields = dataSchema.getFields().stream()
          .map(HoodieSchemaField::name)
          .collect(Collectors.toList());
      return requiredSchema.getFields().stream()
          .map(f -> fields.indexOf(f.name()))
          .mapToInt(i -> i)
          .toArray();
    }

    @Override
    public boolean hasNext() {
      if (sideImage != null) {
        currentImage = sideImage;
        sideImage = null;
        return true;
      } else if (cdcItr.hasNext()) {
        cdcRecord = (GenericRecord) cdcItr.next();
        String op = String.valueOf(cdcRecord.get(0));
        resolveImage(op);
        return true;
      }
      return false;
    }

    protected abstract RowData getAfterImage(RowKind rowKind, GenericRecord cdcRecord);

    protected abstract RowData getBeforeImage(RowKind rowKind, GenericRecord cdcRecord);

    @Override
    public RowData next() {
      return currentImage;
    }

    @Override
    public void close() {
      if (cdcItr != null) {
        cdcItr.close();
        cdcItr = null;
      }
    }

    private void resolveImage(String op) {
      switch (op) {
        case "i":
          currentImage = getAfterImage(RowKind.INSERT, cdcRecord);
          break;
        case "u":
          currentImage = getBeforeImage(RowKind.UPDATE_BEFORE, cdcRecord);
          sideImage = getAfterImage(RowKind.UPDATE_AFTER, cdcRecord);
          break;
        case "d":
          currentImage = getBeforeImage(RowKind.DELETE, cdcRecord);
          break;
        default:
          throw new AssertionError("Unexpected CDC operation: " + op);
      }
    }

    protected RowData resolveAvro(RowKind rowKind, GenericRecord avroRecord) {
      GenericRecord requiredAvroRecord = buildAvroRecordBySchema(
          avroRecord, requiredSchema, requiredPos, recordBuilder);
      RowData resolved = (RowData) avroToRowDataConverter.convert(requiredAvroRecord);
      resolved.setRowKind(rowKind);
      return resolved;
    }
  }

  /**
   * Reads CDC log files that contain both before and after images ({@code DATA_BEFORE_AFTER} mode).
   * CDC record layout: [op, ts, before_image, after_image].
   */
  public static class BeforeAfterImageIterator extends BaseImageIterator {
    public BeforeAfterImageIterator(
        org.apache.hadoop.conf.Configuration hadoopConf,
        String tablePath,
        HoodieSchema tableSchema,
        HoodieSchema requiredSchema,
        RowType requiredRowType,
        HoodieSchema cdcSchema,
        HoodieCDCFileSplit fileSplit) {
      super(hadoopConf, tablePath, tableSchema, requiredSchema, requiredRowType, cdcSchema, fileSplit);
    }

    @Override
    protected RowData getAfterImage(RowKind rowKind, GenericRecord cdcRecord) {
      return resolveAvro(rowKind, (GenericRecord) cdcRecord.get(3));
    }

    @Override
    protected RowData getBeforeImage(RowKind rowKind, GenericRecord cdcRecord) {
      return resolveAvro(rowKind, (GenericRecord) cdcRecord.get(2));
    }
  }

  /**
   * Reads CDC log files containing op + key + before_image ({@code DATA_BEFORE} mode).
   * The after-image is loaded from the after file-slice snapshot via {@link CdcImageManager}.
   * CDC record layout: [op, key, before_image].
   */
  public static class BeforeImageIterator extends BaseImageIterator {
    protected ExternalSpillableMap<String, byte[]> afterImages;
    protected final long maxCompactionMemoryInBytes;
    protected final RowDataProjection projection;
    protected final CdcImageManager imageManager;

    public BeforeImageIterator(
        org.apache.hadoop.conf.Configuration hadoopConf,
        String tablePath,
        HoodieSchema tableSchema,
        HoodieSchema requiredSchema,
        RowType requiredRowType,
        int[] requiredPositions,
        long maxCompactionMemoryInBytes,
        HoodieSchema cdcSchema,
        HoodieCDCFileSplit fileSplit,
        CdcImageManager imageManager) throws IOException {
      super(hadoopConf, tablePath, tableSchema, requiredSchema, requiredRowType, cdcSchema, fileSplit);
      this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
      this.projection = RowDataProjection.instance(requiredRowType, requiredPositions);
      this.imageManager = imageManager;
      initImages(fileSplit);
    }

    protected void initImages(HoodieCDCFileSplit fileSplit) throws IOException {
      ValidationUtils.checkState(fileSplit.getAfterFileSlice().isPresent(),
          "Current file slice does not exist for instant: " + fileSplit.getInstant());
      this.afterImages = imageManager.getOrLoadImages(
          maxCompactionMemoryInBytes, fileSplit.getAfterFileSlice().get());
    }

    @Override
    protected RowData getAfterImage(RowKind rowKind, GenericRecord cdcRecord) {
      String recordKey = cdcRecord.get(1).toString();
      RowData row = imageManager.getImageRecord(recordKey, afterImages, rowKind);
      row.setRowKind(rowKind);
      return projection.project(row);
    }

    @Override
    protected RowData getBeforeImage(RowKind rowKind, GenericRecord cdcRecord) {
      return resolveAvro(rowKind, (GenericRecord) cdcRecord.get(2));
    }
  }

  /**
   * Reads CDC log files containing only op + key ({@code OP_KEY_ONLY} mode).
   * Both before and after images are loaded from file-slice snapshots via {@link CdcImageManager}.
   * CDC record layout: [op, key].
   */
  public static class RecordKeyImageIterator extends BeforeImageIterator {
    protected ExternalSpillableMap<String, byte[]> beforeImages;

    public RecordKeyImageIterator(
        org.apache.hadoop.conf.Configuration hadoopConf,
        String tablePath,
        HoodieSchema tableSchema,
        HoodieSchema requiredSchema,
        RowType requiredRowType,
        int[] requiredPositions,
        long maxCompactionMemoryInBytes,
        HoodieSchema cdcSchema,
        HoodieCDCFileSplit fileSplit,
        CdcImageManager imageManager) throws IOException {
      super(hadoopConf, tablePath, tableSchema, requiredSchema, requiredRowType,
          requiredPositions, maxCompactionMemoryInBytes, cdcSchema, fileSplit, imageManager);
    }

    @Override
    protected void initImages(HoodieCDCFileSplit fileSplit) throws IOException {
      super.initImages(fileSplit);
      ValidationUtils.checkState(fileSplit.getBeforeFileSlice().isPresent(),
          "Before file slice does not exist for instant: " + fileSplit.getInstant());
      this.beforeImages = imageManager.getOrLoadImages(
          maxCompactionMemoryInBytes, fileSplit.getBeforeFileSlice().get());
    }

    @Override
    protected RowData getBeforeImage(RowKind rowKind, GenericRecord cdcRecord) {
      String recordKey = cdcRecord.get(1).toString();
      RowData row = imageManager.getImageRecord(recordKey, beforeImages, rowKind);
      row.setRowKind(rowKind);
      return projection.project(row);
    }
  }

  // -------------------------------------------------------------------------
  //  REPLACE_COMMIT
  // -------------------------------------------------------------------------

  /**
   * Handles the {@code REPLACE_COMMIT} CDC inference case: emits all records from the
   * before-slice as {@link RowKind#DELETE}.
   */
  public static class ReplaceCommitIterator implements ClosableIterator<RowData> {
    private final ClosableIterator<RowData> itr;
    private final RowDataProjection projection;

    public ReplaceCommitIterator(
        String tablePath,
        RowType requiredRowType,
        int[] requiredPositions,
        long maxCompactionMemoryInBytes,
        HoodieCDCFileSplit fileSplit,
        Function<MergeOnReadInputSplit, ClosableIterator<RowData>> splitIteratorFunc) {
      ValidationUtils.checkState(fileSplit.getBeforeFileSlice().isPresent(),
          "Before file slice does not exist for instant: " + fileSplit.getInstant());
      MergeOnReadInputSplit inputSplit = fileSlice2Split(
          tablePath, fileSplit.getBeforeFileSlice().get(), maxCompactionMemoryInBytes);
      this.itr = splitIteratorFunc.apply(inputSplit);
      this.projection = RowDataProjection.instance(requiredRowType, requiredPositions);
    }

    @Override
    public boolean hasNext() {
      return itr.hasNext();
    }

    @Override
    public RowData next() {
      RowData row = itr.next();
      row.setRowKind(RowKind.DELETE);
      return projection.project(row);
    }

    @Override
    public void close() {
      itr.close();
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  public static MergeOnReadInputSplit fileSlice2Split(
          String tablePath,
          FileSlice fileSlice,
          long maxCompactionMemoryInBytes) {
    Option<List<String>> logPaths = Option.ofNullable(fileSlice.getLogFiles()
            .sorted(HoodieLogFile.getLogFileComparator())
            .map(logFile -> logFile.getPath().toString())
            // filter out the cdc logs
            .filter(p -> !p.endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
            .collect(Collectors.toList()));
    String basePath = fileSlice.getBaseFile().map(BaseFile::getPath).orElse(null);
    return new MergeOnReadInputSplit(0, basePath, logPaths, fileSlice.getLatestInstantTime(),
            tablePath, maxCompactionMemoryInBytes, FlinkOptions.REALTIME_PAYLOAD_COMBINE, null,
            fileSlice.getFileId(), fileSlice.getPartitionPath());
  }

  public static MergeOnReadInputSplit singleLogFile2Split(String tablePath, String filePath, long maxCompactionMemoryInBytes) {
    return new MergeOnReadInputSplit(0, null, Option.of(Collections.singletonList(filePath)),
            FSUtils.getDeltaCommitTimeFromLogPath(new StoragePath(filePath)), tablePath, maxCompactionMemoryInBytes,
            FlinkOptions.REALTIME_PAYLOAD_COMBINE, null, FSUtils.getFileIdFromLogPath(new StoragePath(filePath)),
            FSUtils.getRelativePartitionPath(new StoragePath(tablePath), new StoragePath(filePath).getParent()));
  }
}
