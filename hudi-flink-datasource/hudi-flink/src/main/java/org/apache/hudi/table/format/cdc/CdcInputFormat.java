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

import org.apache.avro.Schema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.HoodieCDCLogRecordIterator;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.source.ExpressionPredicates.Predicate;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.table.format.FlinkReaderContextFactory;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.RowDataProjection;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS;
import static org.apache.hudi.table.format.FormatUtils.buildAvroRecordBySchema;

/**
 * The base InputFormat class to read Hoodie data set as change logs.
 */
public class CdcInputFormat extends MergeOnReadInputFormat {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(CdcInputFormat.class);

  private CdcInputFormat(
      Configuration conf,
      MergeOnReadTableState tableState,
      List<DataType> fieldTypes,
      List<Predicate> predicates,
      long limit,
      boolean emitDelete) {
    super(conf, tableState, fieldTypes, predicates, limit, emitDelete, InternalSchemaManager.DISABLED);
  }

  @Override
  protected ClosableIterator<RowData> initIterator(MergeOnReadInputSplit split) throws IOException {
    if (split instanceof CdcInputSplit) {
      HoodieCDCSupplementalLoggingMode mode = OptionsResolver.getCDCSupplementalLoggingMode(conf);
      ImageManager manager = new ImageManager(conf, tableState.getRowType(), this::getFileSliceIterator);
      Function<HoodieCDCFileSplit, ClosableIterator<RowData>> recordIteratorFunc =
          cdcFileSplit -> getRecordIteratorV2(split.getTablePath(), split.getMaxCompactionMemoryInBytes(), cdcFileSplit, mode, manager);
      return new CdcFileSplitsIterator((CdcInputSplit) split, manager, recordIteratorFunc);
    } else {
      return super.initIterator(split);
    }
  }

  /**
   * Returns the builder for {@link MergeOnReadInputFormat}.
   */
  public static Builder builder() {
    return new Builder();
  }

  private ClosableIterator<RowData> getFileSliceIterator(MergeOnReadInputSplit split) {
    try {
      // get full schema iterator.
      final HoodieSchema schema = HoodieSchemaCache.intern(
          new HoodieSchema.Parser().parse(tableState.getAvroSchema()));
      // before/after images have assumption of snapshot scan, so `emitDelete` is set as false
      return getSplitRowIterator(split, schema, schema, FlinkOptions.REALTIME_PAYLOAD_COMBINE, false);
    } catch (IOException e) {
      throw new HoodieException("Failed to create iterator for split: " + split, e);
    }
  }

  private ClosableIterator<RowData> getRecordIteratorV2(
      String tablePath,
      long maxCompactionMemoryInBytes,
      HoodieCDCFileSplit fileSplit,
      HoodieCDCSupplementalLoggingMode mode,
      ImageManager imageManager) {
    try {
      return getRecordIterator(tablePath, maxCompactionMemoryInBytes, fileSplit, mode, imageManager);
    } catch (IOException e) {
      throw new HoodieException("Get record iterator error", e);
    }
  }

  private ClosableIterator<RowData> getRecordIterator(
      String tablePath,
      long maxCompactionMemoryInBytes,
      HoodieCDCFileSplit fileSplit,
      HoodieCDCSupplementalLoggingMode mode,
      ImageManager imageManager) throws IOException {
    switch (fileSplit.getCdcInferCase()) {
      case BASE_FILE_INSERT:
        ValidationUtils.checkState(fileSplit.getCdcFiles() != null && fileSplit.getCdcFiles().size() == 1,
            "CDC file path should exist and be singleton");
        String path = new Path(tablePath, fileSplit.getCdcFiles().get(0)).toString();
        return new AddBaseFileIterator(getBaseFileIterator(path));
      case BASE_FILE_DELETE:
        ValidationUtils.checkState(fileSplit.getBeforeFileSlice().isPresent(),
            "Before file slice should exist");
        FileSlice fileSlice = fileSplit.getBeforeFileSlice().get();
        MergeOnReadInputSplit inputSplit = fileSlice2Split(tablePath, fileSlice, maxCompactionMemoryInBytes);
        return new RemoveBaseFileIterator(tableState, getFileSliceIterator(inputSplit));
      case AS_IS:
        HoodieSchema dataSchema = HoodieSchemaUtils.removeMetadataFields(new HoodieSchema.Parser().parse(tableState.getAvroSchema()));
        Schema cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(mode, dataSchema.getAvroSchema());
        switch (mode) {
          case DATA_BEFORE_AFTER:
            return new BeforeAfterImageIterator(tablePath, tableState, hadoopConf, cdcSchema, fileSplit);
          case DATA_BEFORE:
            return new BeforeImageIterator(conf, hadoopConf, tablePath, tableState, cdcSchema, fileSplit, imageManager);
          case OP_KEY_ONLY:
            return new RecordKeyImageIterator(conf, hadoopConf, tablePath, tableState, cdcSchema, fileSplit, imageManager);
          default:
            throw new AssertionError("Unexpected mode" + mode);
        }
      case LOG_FILE:
        ValidationUtils.checkState(fileSplit.getCdcFiles() != null && fileSplit.getCdcFiles().size() == 1,
            "CDC file path should exist and be singleton");
        String logFilepath = new Path(tablePath, fileSplit.getCdcFiles().get(0)).toString();
        MergeOnReadInputSplit split = singleLogFile2Split(tablePath, logFilepath, maxCompactionMemoryInBytes);
        ClosableIterator<HoodieRecord<RowData>> recordIterator = getSplitRecordIterator(split);
        return new DataLogFileIterator(maxCompactionMemoryInBytes, imageManager, fileSplit, tableState, recordIterator, metaClient);
      case REPLACE_COMMIT:
        return new ReplaceCommitIterator(conf, tablePath, tableState, fileSplit, this::getFileSliceIterator);
      default:
        throw new AssertionError("Unexpected cdc file split infer case: " + fileSplit.getCdcInferCase());
    }
  }

  /**
   * Get a {@link HoodieRecord} iterator using {@link HoodieFileGroupReader}.
   *
   * @param split input split
   *
   * @return {@link RowData} iterator for the given split.
   */
  private ClosableIterator<HoodieRecord<RowData>> getSplitRecordIterator(MergeOnReadInputSplit split) throws IOException {
    final HoodieSchema schema = HoodieSchemaCache.intern(
        new HoodieSchema.Parser().parse(tableState.getAvroSchema()));
    HoodieFileGroupReader<RowData> fileGroupReader =
        createFileGroupReader(split, schema, schema, FlinkOptions.REALTIME_PAYLOAD_COMBINE, true);
    return fileGroupReader.getClosableHoodieRecordIterator();
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------
  static class CdcFileSplitsIterator implements ClosableIterator<RowData> {
    private ImageManager imageManager; //  keep a reference to release resource
    private final Iterator<HoodieCDCFileSplit> fileSplitIterator;
    private final Function<HoodieCDCFileSplit, ClosableIterator<RowData>> recordIteratorFunc;
    private ClosableIterator<RowData> recordIterator;

    CdcFileSplitsIterator(
        CdcInputSplit inputSplit,
        ImageManager imageManager,
        Function<HoodieCDCFileSplit, ClosableIterator<RowData>> recordIteratorFunc) {
      this.fileSplitIterator = Arrays.asList(inputSplit.getChanges()).iterator();
      this.imageManager = imageManager;
      this.recordIteratorFunc = recordIteratorFunc;
    }

    @Override
    public boolean hasNext() {
      if (recordIterator != null) {
        if (recordIterator.hasNext()) {
          return true;
        } else {
          recordIterator.close(); // release resource
          recordIterator = null;
        }
      }
      if (fileSplitIterator.hasNext()) {
        HoodieCDCFileSplit fileSplit = fileSplitIterator.next();
        recordIterator = recordIteratorFunc.apply(fileSplit);
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

  static class AddBaseFileIterator implements ClosableIterator<RowData> {
    // base file record iterator
    private ClosableIterator<RowData> nested;

    private RowData currentRecord;

    AddBaseFileIterator(ClosableIterator<RowData> nested) {
      this.nested = nested;
    }

    @Override
    public boolean hasNext() {
      if (this.nested.hasNext()) {
        currentRecord = this.nested.next();
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
      if (this.nested != null) {
        this.nested.close();
        this.nested = null;
      }
    }
  }

  static class RemoveBaseFileIterator implements ClosableIterator<RowData> {
    private ClosableIterator<RowData> nested;
    private final RowDataProjection projection;

    RemoveBaseFileIterator(MergeOnReadTableState tableState, ClosableIterator<RowData> iterator) {
      this.nested = iterator;
      this.projection = RowDataProjection.instance(tableState.getRequiredRowType(), tableState.getRequiredPositions());
    }

    @Override
    public boolean hasNext() {
      return nested.hasNext();
    }

    @Override
    public RowData next() {
      RowData row = nested.next();
      row.setRowKind(RowKind.DELETE);
      return this.projection.project(row);
    }

    @Override
    public void close() {
      if (this.nested != null) {
        this.nested.close();
        this.nested = null;
      }
    }
  }

  // accounting to HoodieCDCInferenceCase.LOG_FILE
  static class DataLogFileIterator implements ClosableIterator<RowData> {
    private final Schema tableSchema;
    private final long maxCompactionMemoryInBytes;
    private final ImageManager imageManager;
    private final RowDataProjection projection;
    private final BufferedRecordMerger recordMerger;
    private final ClosableIterator<HoodieRecord<RowData>> logRecordIterator;
    private final DeleteContext deleteContext;

    private ExternalSpillableMap<String, byte[]> beforeImages;
    private RowData currentImage;
    private RowData sideImage;
    private HoodieReaderContext<RowData> readerContext;
    private String[] orderingFields;
    private TypedProperties props;

    DataLogFileIterator(
        long maxCompactionMemoryInBytes,
        ImageManager imageManager,
        HoodieCDCFileSplit cdcFileSplit,
        MergeOnReadTableState tableState,
        ClosableIterator<HoodieRecord<RowData>> logRecordIterator,
        HoodieTableMetaClient metaClient) throws IOException {
      this.tableSchema = new HoodieSchema.Parser().parse(tableState.getAvroSchema()).getAvroSchema();
      this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
      this.imageManager = imageManager;
      this.projection = tableState.getRequiredRowType().equals(tableState.getRowType())
          ? null
          : RowDataProjection.instance(tableState.getRequiredRowType(), tableState.getRequiredPositions());
      HoodieWriteConfig writeConfig = this.imageManager.writeConfig;
      this.props = writeConfig.getProps();
      this.readerContext = new FlinkReaderContextFactory(metaClient).getContext();
      readerContext.initRecordMerger(props);
      this.orderingFields = ConfigUtils.getOrderingFields(props);
      this.recordMerger = BufferedRecordMergerFactory.create(
          readerContext,
          readerContext.getMergeMode(),
          false,
          Option.of(imageManager.writeConfig.getRecordMerger()),
          tableSchema,
          Option.ofNullable(Pair.of(metaClient.getTableConfig().getPayloadClass(), writeConfig.getPayloadClass())),
          props,
          metaClient.getTableConfig().getPartialUpdateMode());
      this.logRecordIterator = logRecordIterator;
      initImages(cdcFileSplit);
      this.deleteContext = new DeleteContext(props, tableSchema).withReaderSchema(tableSchema);
    }

    private void initImages(HoodieCDCFileSplit fileSplit) throws IOException {
      // init before images
      if (fileSplit.getBeforeFileSlice().isPresent() && !fileSplit.getBeforeFileSlice().get().isEmpty()) {
        this.beforeImages = this.imageManager.getOrLoadImages(
            maxCompactionMemoryInBytes, fileSplit.getBeforeFileSlice().get());
      } else {
        // still initializes an empty map
        this.beforeImages = FormatUtils.spillableMap(this.imageManager.writeConfig, maxCompactionMemoryInBytes, getClass().getSimpleName());
      }
    }

    @Override
    public boolean hasNext() {
      if (this.sideImage != null) {
        this.currentImage = this.sideImage;
        this.sideImage = null;
        return true;
      }
      while (logRecordIterator.hasNext()) {
        HoodieRecord<RowData> record = logRecordIterator.next();
        RowData existed = imageManager.removeImageRecord(record.getRecordKey(), beforeImages);
        if (isDelete(record)) {
          // it's a deleted record.
          if (existed != null) {
            // there is a real record deleted.
            existed.setRowKind(RowKind.DELETE);
            this.currentImage = existed;
            return true;
          }
        } else {
          if (existed == null) {
            // a new record is inserted.
            RowData newRow = record.getData();
            newRow.setRowKind(RowKind.INSERT);
            this.currentImage = newRow;
            return true;
          } else {
            // an existed record is updated, assuming new record and existing record share the same hoodie key
            HoodieOperation operation = HoodieOperation.fromValue(existed.getRowKind().toByteValue());
            HoodieRecord<RowData> historyRecord = new HoodieFlinkRecord(record.getKey(), operation, existed);
            HoodieRecord<RowData> merged = mergeRowWithLog(historyRecord, record).get();
            if (merged.getData() != existed) {
              // update happens
              existed.setRowKind(RowKind.UPDATE_BEFORE);
              this.currentImage = existed;
              RowData mergedRow = merged.getData();
              mergedRow.setRowKind(RowKind.UPDATE_AFTER);
              this.imageManager.updateImageRecord(record.getRecordKey(), beforeImages, mergedRow);
              this.sideImage = mergedRow;

              return true;
            }
          }
        }
      }
      return false;
    }

    @Override
    public RowData next() {
      return this.projection != null ? this.projection.project(this.currentImage) : this.currentImage;
    }

    @Override
    public void close() {
      this.logRecordIterator.close();
      this.imageManager.close();
    }

    @SuppressWarnings("unchecked")
    private Option<HoodieRecord<RowData>> mergeRowWithLog(HoodieRecord<RowData> historyRecord, HoodieRecord<RowData> newRecord) {
      try {
        BufferedRecord<RowData> historyBufferedRecord = BufferedRecords.fromHoodieRecord(historyRecord, tableSchema, readerContext.getRecordContext(), props, orderingFields, deleteContext);
        BufferedRecord<RowData> newBufferedRecord = BufferedRecords.fromHoodieRecord(newRecord, tableSchema, readerContext.getRecordContext(), props, orderingFields, deleteContext);
        BufferedRecord<RowData> mergedRecord = recordMerger.finalMerge(historyBufferedRecord, newBufferedRecord);
        return Option.ofNullable(readerContext.getRecordContext().constructHoodieRecord(mergedRecord, historyRecord.getPartitionPath()));
      } catch (IOException e) {
        throw new HoodieIOException("Merge base and delta payloads exception", e);
      }
    }

    private boolean isDelete(HoodieRecord<RowData> record) {
      return record.isDelete(deleteContext, CollectionUtils.emptyProps());
    }
  }

  abstract static class BaseImageIterator implements ClosableIterator<RowData> {
    private final Schema requiredSchema;
    private final int[] requiredPos;
    private final GenericRecordBuilder recordBuilder;
    private final AvroToRowDataConverters.AvroToRowDataConverter avroToRowDataConverter;

    // the changelog records iterator
    private HoodieCDCLogRecordIterator cdcItr;

    private GenericRecord cdcRecord;

    private RowData sideImage;

    private RowData currentImage;

    BaseImageIterator(
        org.apache.hadoop.conf.Configuration hadoopConf,
        String tablePath,
        MergeOnReadTableState tableState,
        Schema cdcSchema,
        HoodieCDCFileSplit fileSplit) {
      this.requiredSchema = new HoodieSchema.Parser().parse(tableState.getRequiredAvroSchema()).getAvroSchema();
      this.requiredPos = getRequiredPos(tableState.getAvroSchema(), this.requiredSchema);
      this.recordBuilder = new GenericRecordBuilder(requiredSchema);
      this.avroToRowDataConverter = AvroToRowDataConverters.createRowConverter(tableState.getRequiredRowType());
      StoragePath hadoopTablePath = new StoragePath(tablePath);
      HoodieStorage storage = new HoodieHadoopStorage(tablePath, hadoopConf);
      HoodieLogFile[] cdcLogFiles = fileSplit.getCdcFiles().stream().map(cdcFile -> {
        try {
          return new HoodieLogFile(
              storage.getPathInfo(new StoragePath(hadoopTablePath, cdcFile)));
        } catch (IOException e) {
          throw new HoodieIOException("Fail to call getFileStatus", e);
        }
      }).toArray(HoodieLogFile[]::new);
      this.cdcItr = new HoodieCDCLogRecordIterator(storage, cdcLogFiles, cdcSchema);
    }

    private int[] getRequiredPos(String tableSchema, Schema required) {
      Schema dataSchema = HoodieAvroUtils.removeMetadataFields(new HoodieSchema.Parser().parse(tableSchema).getAvroSchema());
      List<String> fields = dataSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
      return required.getFields().stream()
          .map(f -> fields.indexOf(f.name()))
          .mapToInt(i -> i)
          .toArray();
    }

    @Override
    public boolean hasNext() {
      if (this.sideImage != null) {
        currentImage = this.sideImage;
        this.sideImage = null;
        return true;
      } else if (this.cdcItr.hasNext()) {
        cdcRecord = (GenericRecord) this.cdcItr.next();
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
      if (this.cdcItr != null) {
        this.cdcItr.close();
        this.cdcItr = null;
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
          throw new AssertionError("Unexpected");
      }
    }

    protected RowData resolveAvro(RowKind rowKind, GenericRecord avroRecord) {
      GenericRecord requiredAvroRecord = buildAvroRecordBySchema(
          avroRecord,
          HoodieSchema.fromAvroSchema(requiredSchema),
          requiredPos,
          recordBuilder);
      RowData resolved = (RowData) avroToRowDataConverter.convert(requiredAvroRecord);
      resolved.setRowKind(rowKind);
      return resolved;
    }
  }

  // op, ts, before_image, after_image
  static class BeforeAfterImageIterator extends BaseImageIterator {
    BeforeAfterImageIterator(
        String tablePath,
        MergeOnReadTableState tableState,
        org.apache.hadoop.conf.Configuration hadoopConf,
        Schema cdcSchema,
        HoodieCDCFileSplit fileSplit) {
      super(hadoopConf, tablePath, tableState, cdcSchema, fileSplit);
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

  // op, key, before_image
  static class BeforeImageIterator extends BaseImageIterator {
    protected ExternalSpillableMap<String, byte[]> afterImages;

    protected final long maxCompactionMemoryInBytes;

    protected final RowDataProjection projection;
    protected final ImageManager imageManager;

    BeforeImageIterator(
        Configuration flinkConf,
        org.apache.hadoop.conf.Configuration hadoopConf,
        String tablePath,
        MergeOnReadTableState tableState,
        Schema cdcSchema,
        HoodieCDCFileSplit fileSplit,
        ImageManager imageManager) throws IOException {
      super(hadoopConf, tablePath, tableState, cdcSchema, fileSplit);
      this.maxCompactionMemoryInBytes = StreamerUtil.getMaxCompactionMemoryInBytes(flinkConf);
      this.projection = RowDataProjection.instance(tableState.getRequiredRowType(), tableState.getRequiredPositions());
      this.imageManager = imageManager;
      initImages(fileSplit);
    }

    protected void initImages(
        HoodieCDCFileSplit fileSplit) throws IOException {
      ValidationUtils.checkState(fileSplit.getAfterFileSlice().isPresent(),
          "Current file slice does not exist for instant: " + fileSplit.getInstant());
      this.afterImages = this.imageManager.getOrLoadImages(
          maxCompactionMemoryInBytes, fileSplit.getAfterFileSlice().get());
    }

    @Override
    protected RowData getAfterImage(RowKind rowKind, GenericRecord cdcRecord) {
      String recordKey = cdcRecord.get(1).toString();
      RowData row = imageManager.getImageRecord(recordKey, this.afterImages, rowKind);
      row.setRowKind(rowKind);
      return this.projection.project(row);
    }

    @Override
    protected RowData getBeforeImage(RowKind rowKind, GenericRecord cdcRecord) {
      return resolveAvro(rowKind, (GenericRecord) cdcRecord.get(2));
    }
  }

  // op, key
  static class RecordKeyImageIterator extends BeforeImageIterator {
    protected ExternalSpillableMap<String, byte[]> beforeImages;

    RecordKeyImageIterator(
        Configuration flinkConf,
        org.apache.hadoop.conf.Configuration hadoopConf,
        String tablePath,
        MergeOnReadTableState tableState,
        Schema cdcSchema,
        HoodieCDCFileSplit fileSplit,
        ImageManager imageManager) throws IOException {
      super(flinkConf, hadoopConf, tablePath, tableState, cdcSchema, fileSplit, imageManager);
    }

    protected void initImages(HoodieCDCFileSplit fileSplit) throws IOException {
      // init after images
      super.initImages(fileSplit);
      // init before images
      ValidationUtils.checkState(fileSplit.getBeforeFileSlice().isPresent(),
          "Before file slice does not exist for instant: " + fileSplit.getInstant());
      this.beforeImages = this.imageManager.getOrLoadImages(
          maxCompactionMemoryInBytes, fileSplit.getBeforeFileSlice().get());
    }

    @Override
    protected RowData getBeforeImage(RowKind rowKind, GenericRecord cdcRecord) {
      String recordKey = cdcRecord.get(1).toString();
      RowData row = this.imageManager.getImageRecord(recordKey, this.beforeImages, rowKind);
      row.setRowKind(rowKind);
      return this.projection.project(row);
    }
  }

  static class ReplaceCommitIterator implements ClosableIterator<RowData> {
    private final ClosableIterator<RowData> itr;
    private final RowDataProjection projection;

    ReplaceCommitIterator(
        Configuration flinkConf,
        String tablePath,
        MergeOnReadTableState tableState,
        HoodieCDCFileSplit fileSplit,
        Function<MergeOnReadInputSplit, ClosableIterator<RowData>> splitIteratorFunc) {
      this.itr = initIterator(tablePath, StreamerUtil.getMaxCompactionMemoryInBytes(flinkConf), fileSplit, splitIteratorFunc);
      this.projection = RowDataProjection.instance(tableState.getRequiredRowType(), tableState.getRequiredPositions());
    }

    private ClosableIterator<RowData> initIterator(
        String tablePath,
        long maxCompactionMemoryInBytes,
        HoodieCDCFileSplit fileSplit,
        Function<MergeOnReadInputSplit, ClosableIterator<RowData>> splitIteratorFunc) {
      // init before images

      // the before file slice must exist,
      // see HoodieCDCExtractor#extractCDCFileSplits for details
      ValidationUtils.checkState(fileSplit.getBeforeFileSlice().isPresent(),
          "Before file slice does not exist for instant: " + fileSplit.getInstant());
      MergeOnReadInputSplit inputSplit = CdcInputFormat.fileSlice2Split(
          tablePath, fileSplit.getBeforeFileSlice().get(), maxCompactionMemoryInBytes);
      return splitIteratorFunc.apply(inputSplit);
    }

    @Override
    public boolean hasNext() {
      return this.itr.hasNext();
    }

    @Override
    public RowData next() {
      RowData row = this.itr.next();
      row.setRowKind(RowKind.DELETE);
      return this.projection.project(row);
    }

    @Override
    public void close() {
      this.itr.close();
    }
  }

  public static final class BytesArrayInputView extends DataInputStream implements DataInputView {
    public BytesArrayInputView(byte[] data) {
      super(new ByteArrayInputStream(data));
    }

    public void skipBytesToRead(int numBytes) throws IOException {
      while (numBytes > 0) {
        int skipped = this.skipBytes(numBytes);
        numBytes -= skipped;
      }
    }
  }

  public static final class BytesArrayOutputView extends DataOutputStream implements DataOutputView {
    public BytesArrayOutputView(ByteArrayOutputStream baos) {
      super(baos);
    }

    public void skipBytesToWrite(int numBytes) throws IOException {
      for (int i = 0; i < numBytes; ++i) {
        this.write(0);
      }
    }

    public void write(DataInputView source, int numBytes) throws IOException {
      byte[] buffer = new byte[numBytes];
      source.readFully(buffer);
      this.write(buffer);
    }
  }

  /**
   * A before/after image manager
   * that caches the image records by versions(file slices).
   */
  private static class ImageManager implements AutoCloseable {
    private final HoodieWriteConfig writeConfig;

    private final RowDataSerializer serializer;
    private final Function<MergeOnReadInputSplit, ClosableIterator<RowData>> splitIteratorFunc;

    private final Map<String, ExternalSpillableMap<String, byte[]>> cache;

    public ImageManager(
        Configuration flinkConf,
        RowType rowType,
        Function<MergeOnReadInputSplit, ClosableIterator<RowData>> splitIteratorFunc) {
      this.serializer = new RowDataSerializer(rowType);
      this.splitIteratorFunc = splitIteratorFunc;
      this.cache = new TreeMap<>();
      this.writeConfig = FlinkWriteClients.getHoodieClientConfig(flinkConf);
    }

    public ExternalSpillableMap<String, byte[]> getOrLoadImages(
        long maxCompactionMemoryInBytes,
        FileSlice fileSlice) throws IOException {
      final String instant = fileSlice.getBaseInstantTime();
      if (this.cache.containsKey(instant)) {
        return cache.get(instant);
      }
      // clean the earliest file slice first
      if (this.cache.size() > 1) {
        // keep at most 2 versions: before & after
        String instantToClean = this.cache.keySet().iterator().next();
        this.cache.remove(instantToClean).close();
      }
      ExternalSpillableMap<String, byte[]> images = loadImageRecords(maxCompactionMemoryInBytes, fileSlice);
      this.cache.put(instant, images);
      return images;
    }

    private ExternalSpillableMap<String, byte[]> loadImageRecords(
        long maxCompactionMemoryInBytes,
        FileSlice fileSlice) throws IOException {
      MergeOnReadInputSplit inputSplit = CdcInputFormat.fileSlice2Split(writeConfig.getBasePath(), fileSlice, maxCompactionMemoryInBytes);
      // initialize the image records map
      ExternalSpillableMap<String, byte[]> imageRecordsMap =
          FormatUtils.spillableMap(writeConfig, maxCompactionMemoryInBytes, getClass().getSimpleName());
      try (ClosableIterator<RowData> itr = splitIteratorFunc.apply(inputSplit)) {
        while (itr.hasNext()) {
          RowData row = itr.next();
          String recordKey = row.getString(HOODIE_RECORD_KEY_COL_POS).toString();
          ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
          serializer.serialize(row, new BytesArrayOutputView(baos));
          imageRecordsMap.put(recordKey, baos.toByteArray());
        }
      }
      return imageRecordsMap;
    }

    public RowData getImageRecord(
        String recordKey,
        ExternalSpillableMap<String, byte[]> cache,
        RowKind rowKind) {
      byte[] bytes = cache.get(recordKey);
      ValidationUtils.checkState(bytes != null,
          "Key " + recordKey + " does not exist in current file group");
      try {
        RowData row = serializer.deserialize(new BytesArrayInputView(bytes));
        row.setRowKind(rowKind);
        return row;
      } catch (IOException e) {
        throw new HoodieException("Deserialize bytes into row data exception", e);
      }
    }

    public void updateImageRecord(
        String recordKey,
        ExternalSpillableMap<String, byte[]> cache,
        RowData row) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
      try {
        serializer.serialize(row, new BytesArrayOutputView(baos));
      } catch (IOException e) {
        throw new HoodieException("Serialize row data into bytes exception", e);
      }
      cache.put(recordKey, baos.toByteArray());
    }

    public RowData removeImageRecord(
        String recordKey,
        ExternalSpillableMap<String, byte[]> cache) {
      byte[] bytes = cache.remove(recordKey);
      if (bytes == null) {
        return null;
      }
      try {
        return serializer.deserialize(new BytesArrayInputView(bytes));
      } catch (IOException e) {
        throw new HoodieException("Deserialize bytes into row data exception", e);
      }
    }

    @Override
    public void close() {
      this.cache.values().forEach(ExternalSpillableMap::close);
      this.cache.clear();
    }
  }

  /**
   * Builder for {@link CdcInputFormat}.
   */
  public static class Builder extends MergeOnReadInputFormat.Builder {

    public Builder config(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder tableState(MergeOnReadTableState tableState) {
      this.tableState = tableState;
      return this;
    }

    public Builder fieldTypes(List<DataType> fieldTypes) {
      this.fieldTypes = fieldTypes;
      return this;
    }

    public Builder predicates(List<Predicate> predicates) {
      this.predicates = predicates;
      return this;
    }

    public Builder limit(long limit) {
      this.limit = limit;
      return this;
    }

    public Builder emitDelete(boolean emitDelete) {
      this.emitDelete = emitDelete;
      return this;
    }

    public CdcInputFormat build() {
      return new CdcInputFormat(conf, tableState, fieldTypes, predicates, limit, emitDelete);
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
        .filter(path -> !path.endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
        .collect(Collectors.toList()));
    String basePath = fileSlice.getBaseFile().map(BaseFile::getPath).orElse(null);
    return new MergeOnReadInputSplit(0, basePath, logPaths,
        fileSlice.getLatestInstantTime(), tablePath, maxCompactionMemoryInBytes,
        FlinkOptions.REALTIME_PAYLOAD_COMBINE, null, fileSlice.getFileId());
  }

  public static MergeOnReadInputSplit singleLogFile2Split(String tablePath, String filePath, long maxCompactionMemoryInBytes) {
    return new MergeOnReadInputSplit(0, null, Option.of(Collections.singletonList(filePath)),
        FSUtils.getDeltaCommitTimeFromLogPath(new StoragePath(filePath)), tablePath, maxCompactionMemoryInBytes,
        FlinkOptions.REALTIME_PAYLOAD_COMBINE, null, FSUtils.getFileIdFromLogPath(new StoragePath(filePath)));
  }
}
