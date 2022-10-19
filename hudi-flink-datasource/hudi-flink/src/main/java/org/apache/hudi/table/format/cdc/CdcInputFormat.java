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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.table.log.HoodieCDCLogRecordIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.table.format.cow.vector.reader.ParquetColumnarRowSplitReader;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.RowDataProjection;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
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

  private CdcInputFormat(
      Configuration conf,
      MergeOnReadTableState tableState,
      List<DataType> fieldTypes,
      String defaultPartName,
      long limit,
      boolean emitDelete) {
    super(conf, tableState, fieldTypes, defaultPartName, limit, emitDelete);
  }

  @Override
  protected RecordIterator initIterator(MergeOnReadInputSplit split) throws IOException {
    if (split instanceof CdcInputSplit) {
      HoodieCDCSupplementalLoggingMode mode = OptionsResolver.getCDCSupplementalLoggingMode(conf);
      ImageManager manager = new ImageManager(conf, tableState.getRowType(), this::getFileSliceIterator);
      Function<HoodieCDCFileSplit, RecordIterator> recordIteratorFunc =
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

  private RecordIterator getFileSliceIterator(MergeOnReadInputSplit split) {
    if (!(split.getLogPaths().isPresent() && split.getLogPaths().get().size() > 0)) {
      // base file only
      return new BaseFileOnlyIterator(getFullSchemaReader(split.getBasePath().get()));
    } else if (!split.getBasePath().isPresent()) {
      // log files only
      return new LogFileOnlyIterator(getFullLogFileIterator(split));
    } else {
      Schema tableSchema = new Schema.Parser().parse(this.tableState.getAvroSchema());
      return new MergeIterator(
          conf,
          hadoopConf,
          split,
          this.tableState.getRowType(),
          this.tableState.getRowType(),
          tableSchema,
          Option.empty(),
          Option.empty(),
          false,
          this.tableState.getOperationPos(),
          getFullSchemaReader(split.getBasePath().get()));
    }
  }

  private RecordIterator getRecordIteratorV2(
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

  private RecordIterator getRecordIterator(
      String tablePath,
      long maxCompactionMemoryInBytes,
      HoodieCDCFileSplit fileSplit,
      HoodieCDCSupplementalLoggingMode mode,
      ImageManager imageManager) throws IOException {
    switch (fileSplit.getCdcInferCase()) {
      case BASE_FILE_INSERT:
        ValidationUtils.checkState(fileSplit.getCdcFiles() != null && fileSplit.getCdcFiles().size() == 1,
            "CDC file path should exist and be only one");
        String path = new Path(tablePath, fileSplit.getCdcFiles().get(0)).toString();
        return new AddBaseFileIterator(getRequiredSchemaReader(path));
      case BASE_FILE_DELETE:
        ValidationUtils.checkState(fileSplit.getBeforeFileSlice().isPresent(),
            "Before file slice should exist");
        FileSlice fileSlice = fileSplit.getBeforeFileSlice().get();
        MergeOnReadInputSplit inputSplit = fileSlice2Split(tablePath, fileSlice, maxCompactionMemoryInBytes);
        return new RemoveBaseFileIterator(tableState, getFileSliceIterator(inputSplit));
      case AS_IS:
        Schema dataSchema = HoodieAvroUtils.removeMetadataFields(new Schema.Parser().parse(tableState.getAvroSchema()));
        Schema cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(mode, dataSchema);
        switch (mode) {
          case WITH_BEFORE_AFTER:
            return new BeforeAfterImageIterator(tablePath, tableState, hadoopConf, cdcSchema, fileSplit);
          case WITH_BEFORE:
            return new BeforeImageIterator(conf, hadoopConf, tablePath, tableState, cdcSchema, fileSplit, imageManager);
          case OP_KEY:
            return new RecordKeyImageIterator(conf, hadoopConf, tablePath, tableState, cdcSchema, fileSplit, imageManager);
          default:
            throw new AssertionError("Unexpected mode" + mode);
        }
      case REPLACE_COMMIT:
        return new ReplaceCommitIterator(conf, tablePath, tableState, fileSplit, this::getFileSliceIterator);
      default:
        throw new AssertionError("Unexpected cdc file split infer case: " + fileSplit.getCdcInferCase());
    }
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------
  static class CdcFileSplitsIterator implements RecordIterator {
    private ImageManager imageManager; //  keep a reference to release resource
    private final Iterator<HoodieCDCFileSplit> fileSplitIterator;
    private final Function<HoodieCDCFileSplit, RecordIterator> recordIteratorFunc;
    private RecordIterator recordIterator;

    CdcFileSplitsIterator(
        CdcInputSplit inputSplit,
        ImageManager imageManager,
        Function<HoodieCDCFileSplit, RecordIterator> recordIteratorFunc) {
      this.fileSplitIterator = Arrays.asList(inputSplit.getChanges()).iterator();
      this.imageManager = imageManager;
      this.recordIteratorFunc = recordIteratorFunc;
    }

    @Override
    public boolean reachedEnd() throws IOException {
      if (recordIterator != null) {
        if (!recordIterator.reachedEnd()) {
          return false;
        } else {
          recordIterator.close(); // release resource
          recordIterator = null;
        }
      }
      if (fileSplitIterator.hasNext()) {
        HoodieCDCFileSplit fileSplit = fileSplitIterator.next();
        recordIterator = recordIteratorFunc.apply(fileSplit);
        return recordIterator.reachedEnd();
      }
      return true;
    }

    @Override
    public RowData nextRecord() {
      return recordIterator.nextRecord();
    }

    @Override
    public void close() throws IOException {
      if (recordIterator != null) {
        recordIterator.close();
      }
      if (imageManager != null) {
        imageManager.close();
        imageManager = null;
      }
    }
  }

  static class AddBaseFileIterator implements RecordIterator {
    // base file reader
    private ParquetColumnarRowSplitReader reader;

    private RowData currentRecord;

    AddBaseFileIterator(ParquetColumnarRowSplitReader reader) {
      this.reader = reader;
    }

    @Override
    public boolean reachedEnd() throws IOException {
      if (!this.reader.reachedEnd()) {
        currentRecord = this.reader.nextRecord();
        currentRecord.setRowKind(RowKind.INSERT);
        return false;
      }
      return true;
    }

    @Override
    public RowData nextRecord() {
      return currentRecord;
    }

    @Override
    public void close() throws IOException {
      if (this.reader != null) {
        this.reader.close();
        this.reader = null;
      }
    }
  }

  static class RemoveBaseFileIterator implements RecordIterator {
    private RecordIterator nested;
    private final RowDataProjection projection;

    RemoveBaseFileIterator(MergeOnReadTableState tableState, RecordIterator iterator) {
      this.nested = iterator;
      this.projection = RowDataProjection.instance(tableState.getRequiredRowType(), tableState.getRequiredPositions());
    }

    @Override
    public boolean reachedEnd() throws IOException {
      return nested.reachedEnd();
    }

    @Override
    public RowData nextRecord() {
      RowData row = nested.nextRecord();
      row.setRowKind(RowKind.DELETE);
      return this.projection.project(row);
    }

    @Override
    public void close() throws IOException {
      if (this.nested != null) {
        this.nested.close();
        this.nested = null;
      }
    }
  }

  abstract static class BaseImageIterator implements RecordIterator {
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
      this.requiredSchema = new Schema.Parser().parse(tableState.getRequiredAvroSchema());
      this.requiredPos = getRequiredPos(tableState.getAvroSchema(), this.requiredSchema);
      this.recordBuilder = new GenericRecordBuilder(requiredSchema);
      this.avroToRowDataConverter = AvroToRowDataConverters.createRowConverter(tableState.getRequiredRowType());
      Path hadoopTablePath = new Path(tablePath);
      FileSystem fs = FSUtils.getFs(hadoopTablePath, hadoopConf);
      HoodieLogFile[] cdcLogFiles = fileSplit.getCdcFiles().stream().map(cdcFile -> {
        try {
          return new HoodieLogFile(fs.getFileStatus(new Path(hadoopTablePath, cdcFile)));
        } catch (IOException e) {
          throw new HoodieIOException("Fail to call getFileStatus", e);
        }
      }).toArray(HoodieLogFile[]::new);
      this.cdcItr = new HoodieCDCLogRecordIterator(fs, cdcLogFiles, cdcSchema);
    }

    private int[] getRequiredPos(String tableSchema, Schema required) {
      Schema dataSchema = HoodieAvroUtils.removeMetadataFields(new Schema.Parser().parse(tableSchema));
      List<String> fields = dataSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
      return required.getFields().stream()
          .map(f -> fields.indexOf(f.name()))
          .mapToInt(i -> i)
          .toArray();
    }

    @Override
    public boolean reachedEnd() {
      if (this.sideImage != null) {
        currentImage = this.sideImage;
        this.sideImage = null;
        return false;
      } else if (this.cdcItr.hasNext()) {
        cdcRecord = (GenericRecord) this.cdcItr.next();
        String op = String.valueOf(cdcRecord.get(0));
        resolveImage(op);
        return false;
      }
      return true;
    }

    protected abstract RowData getAfterImage(RowKind rowKind, GenericRecord cdcRecord);

    protected abstract RowData getBeforeImage(RowKind rowKind, GenericRecord cdcRecord);

    @Override
    public RowData nextRecord() {
      return currentImage;
    }

    @Override
    public void close() throws IOException {
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
          requiredSchema,
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
        HoodieCDCFileSplit fileSplit) throws IOException {
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

  static class ReplaceCommitIterator implements RecordIterator {
    private final RecordIterator itr;
    private final RowDataProjection projection;

    ReplaceCommitIterator(
        Configuration flinkConf,
        String tablePath,
        MergeOnReadTableState tableState,
        HoodieCDCFileSplit fileSplit,
        Function<MergeOnReadInputSplit, RecordIterator> splitIteratorFunc) {
      this.itr = initIterator(tablePath, StreamerUtil.getMaxCompactionMemoryInBytes(flinkConf), fileSplit, splitIteratorFunc);
      this.projection = RowDataProjection.instance(tableState.getRequiredRowType(), tableState.getRequiredPositions());
    }

    private RecordIterator initIterator(
        String tablePath,
        long maxCompactionMemoryInBytes,
        HoodieCDCFileSplit fileSplit,
        Function<MergeOnReadInputSplit, RecordIterator> splitIteratorFunc) {
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
    public boolean reachedEnd() throws IOException {
      return this.itr.reachedEnd();
    }

    @Override
    public RowData nextRecord() {
      RowData row = this.itr.nextRecord();
      row.setRowKind(RowKind.DELETE);
      return this.projection.project(row);
    }

    @Override
    public void close() throws IOException {
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
    private final Function<MergeOnReadInputSplit, RecordIterator> splitIteratorFunc;

    private final Map<String, ExternalSpillableMap<String, byte[]>> cache;

    public ImageManager(
        Configuration flinkConf,
        RowType rowType,
        Function<MergeOnReadInputSplit, RecordIterator> splitIteratorFunc) {
      this.serializer = new RowDataSerializer(rowType);
      this.splitIteratorFunc = splitIteratorFunc;
      this.cache = new TreeMap<>();
      this.writeConfig = StreamerUtil.getHoodieClientConfig(flinkConf);
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
      RecordIterator itr = splitIteratorFunc.apply(inputSplit);
      // initialize the image records map
      ExternalSpillableMap<String, byte[]> imageRecordsMap =
          FormatUtils.spillableMap(writeConfig, maxCompactionMemoryInBytes);
      while (!itr.reachedEnd()) {
        RowData row = itr.nextRecord();
        String recordKey = row.getString(HOODIE_RECORD_KEY_COL_POS).toString();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
        serializer.serialize(row, new BytesArrayOutputView(baos));
        imageRecordsMap.put(recordKey, baos.toByteArray());
      }
      itr.close(); // release resource
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

    public Builder defaultPartName(String defaultPartName) {
      this.defaultPartName = defaultPartName;
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
      return new CdcInputFormat(conf, tableState, fieldTypes,
          defaultPartName, limit, emitDelete);
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
        fileSlice.getBaseInstantTime(), tablePath, maxCompactionMemoryInBytes,
        FlinkOptions.REALTIME_PAYLOAD_COMBINE, null, fileSlice.getFileId());
  }
}
